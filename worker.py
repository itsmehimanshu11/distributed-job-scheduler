import logging
import os
import re
try:
    import resource
except ImportError:
    resource = None  # not available on Windows; job resource limits are skipped there
import signal
import socket
import subprocess
import sys
import threading
import time

from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

from pythonjsonlogger import jsonlogger
from sqlalchemy import create_engine, text


load_dotenv()


DATABASE_URL = os.environ["DATABASE_URL"]


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)


# ============================================================
# Structured (JSON) logging
# ============================================================
#
# Replaces the previous print()-based logging. JSON lines are
# straightforward to ship into a log aggregator (CloudWatch, Loki,
# ELK, etc.) and to grep/filter by field (worker_id, job_id, event)
# instead of parsing free-text strings.

logger = logging.getLogger("scheduler.worker")
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

_log_handler = logging.StreamHandler(sys.stdout)
_log_handler.setFormatter(
    jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s"
    )
)
logger.handlers = [_log_handler]
logger.propagate = False


def log(event, level="info", **fields):
    """
    Small convenience wrapper so every log line is a structured
    event (`event="job_completed"`) plus whatever fields are
    relevant, always tagged with this worker's id.
    """

    getattr(logger, level)(event, extra={"worker_id": WORKER_ID, **fields})


# ============================================================
# Scheduler configuration
# ============================================================

AGING_INTERVAL_SECONDS = 10
MAX_AGING_BONUS = 100

WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"

HEARTBEAT_INTERVAL_SECONDS = 5
STALE_WORKER_TIMEOUT_SECONDS = 15

# ============================================================
# Job execution safety limits
# ============================================================
#
# These are defense-in-depth, NOT a full sandbox. A job command
# still runs as this worker process's user with its network
# access. For untrusted job submitters, run workers themselves in
# a locked-down container (no host mounts, restricted network,
# non-root user, seccomp profile) or move to a container-per-job
# execution model. What's below only bounds a single runaway job's
# CPU time and memory so it can't take the whole worker down.

JOB_MAX_MEMORY_MB = int(os.getenv("JOB_MAX_MEMORY_MB", "512"))
JOB_MAX_CPU_SECONDS = int(os.getenv("JOB_MAX_CPU_SECONDS", "280"))
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", "300"))

# Best-effort denylist for obviously destructive commands. This is
# NOT a security boundary (shell quoting/obfuscation defeats it
# trivially) -- it exists to catch honest mistakes, not a
# determined attacker. Real isolation belongs at the container/OS
# level, not in a regex.
BLOCKED_COMMAND_PATTERNS = [
    re.compile(pattern)
    for pattern in [
        r"rm\s+-rf\s+/(?:\s|$)",
        r":\(\)\s*\{\s*:\|\s*:\s*&\s*\}\s*;",  # classic fork bomb
        r"mkfs\.",
        r"dd\s+if=.*of=/dev/(sd|nvme|hd)",
    ]
]

def _apply_job_resource_limits():
    """
    Runs inside the child process (via `preexec_fn`) right before
    exec, so the limits apply to the job's command, not the worker
    itself.
    """
    if resource is None:
        return

    memory_bytes = JOB_MAX_MEMORY_MB * 1024 * 1024
    resource.setrlimit(
        resource.RLIMIT_AS, (memory_bytes, memory_bytes)
    )
    resource.setrlimit(
        resource.RLIMIT_CPU,
        (JOB_MAX_CPU_SECONDS, JOB_MAX_CPU_SECONDS),
    )

def command_is_blocked(command: str) -> str | None:
    """
    Returns a rejection reason if the command matches a known
    dangerous pattern, otherwise None.
    """

    for pattern in BLOCKED_COMMAND_PATTERNS:
        if pattern.search(command):
            return f"Command matched blocked pattern: {pattern.pattern}"

    return None


# ============================================================
# Graceful shutdown
# ============================================================
#
# On SIGTERM/SIGINT (e.g. `docker stop`, or this worker being
# scaled down), stop claiming NEW jobs but let whatever job is
# already running finish naturally instead of hard-killing it
# mid-execution -- a half-run shell command can leave things in a
# worse state than a slightly-late shutdown does. If the container
# runtime kills the process anyway after its stop timeout, the
# stale-job recovery path (recover_stale_jobs) still requeues the
# job normally.

shutdown_event = threading.Event()


def _handle_shutdown_signal(signum, _frame):
    log(
        "shutdown_signal_received",
        signal=signal.Signals(signum).name,
    )
    shutdown_event.set()


signal.signal(signal.SIGTERM, _handle_shutdown_signal)
signal.signal(signal.SIGINT, _handle_shutdown_signal)


# ============================================================
# Worker registration
# ============================================================

def register_worker():
    """
    Register this worker in the database, retrying briefly if the
    `workers` table doesn't exist yet.

    On a fresh `docker compose up`, the `worker` container can start
    before the `api` container has finished running its Alembic
    migrations (Compose only waits for the database to be healthy,
    not for migrations to finish). Rather than crashing and relying
    on the container's restart policy to paper over that race,
    retry in-process for a few seconds first.
    """

    max_attempts = 10
    delay_seconds = 1

    for attempt in range(1, max_attempts + 1):
        try:
            with engine.begin() as db:
                db.execute(
                    text(
                        """
                        INSERT INTO workers (
                            worker_id,
                            status,
                            started_at,
                            last_heartbeat
                        )
                        VALUES (
                            :worker_id,
                            'active',
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (worker_id)
                        DO UPDATE SET
                            status = 'active',
                            last_heartbeat = CURRENT_TIMESTAMP
                        """
                    ),
                    {
                        "worker_id": WORKER_ID,
                    },
                )

            log("worker_registered")
            return

        except Exception as exc:
            if attempt == max_attempts:
                log(
                    "worker_registration_failed",
                    level="error",
                    attempt=attempt,
                    error=str(exc),
                )
                raise

            log(
                "worker_registration_retry",
                level="warning",
                attempt=attempt,
                error=str(exc),
            )
            time.sleep(delay_seconds)


# ============================================================
# Worker heartbeat
# ============================================================

def heartbeat_loop():

    while True:

        try:

            with engine.begin() as db:

                db.execute(
                    text(
                        """
                        UPDATE workers
                        SET
                            status = 'active',
                            last_heartbeat = CURRENT_TIMESTAMP
                        WHERE worker_id = :worker_id
                        """
                    ),
                    {
                        "worker_id": WORKER_ID,
                    },
                )

        except Exception as exc:

            log("heartbeat_error", level="warning", error=str(exc))

        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


# ============================================================
# Recover stale jobs
# ============================================================

def recover_stale_jobs():
    """
    Recover jobs whose assigned worker has stopped
    sending heartbeats.
    """

    with engine.begin() as db:

        result = db.execute(
            text(
                """
                WITH stale_jobs AS (
                    SELECT
                        j.id,
                        j.name,
                        j.worker_id AS old_worker_id
                    FROM jobs AS j
                    JOIN workers AS w
                        ON w.worker_id = j.worker_id
                    WHERE
                        j.status = 'running'
                        AND (
                            w.last_heartbeat IS NULL
                            OR w.last_heartbeat <
                                CURRENT_TIMESTAMP
                                - (
                                    :stale_timeout
                                    * INTERVAL '1 second'
                                )
                        )
                    FOR UPDATE OF j SKIP LOCKED
                )
                UPDATE jobs AS j
                SET
                    status = 'pending',
                    worker_id = NULL,
                    claimed_at = NULL,
                    next_run_at = CURRENT_TIMESTAMP,
                    last_error =
                        'Worker became stale; job returned to queue'
                FROM stale_jobs AS s
                WHERE j.id = s.id
                RETURNING
                    j.id,
                    j.name,
                    s.old_worker_id
                """
            ),
            {
                "stale_timeout": STALE_WORKER_TIMEOUT_SECONDS,
            },
        )

        recovered_jobs = result.mappings().all()

        for job in recovered_jobs:

            log(
                "stale_job_recovered",
                job_id=job["id"],
                job_name=job["name"],
                previous_worker_id=job["old_worker_id"],
            )

        return len(recovered_jobs)


# ============================================================
# Claim job
# ============================================================

def claim_job():
    """
    Atomically claim one eligible pending job.

    Scheduling policy:

    1. Aging bonus based on waiting time.
    2. effective_priority = priority + aging bonus.
    3. Highest effective priority wins.
    4. Older jobs win when priorities are equal.
    5. next_run_at must be due.
    6. FOR UPDATE SKIP LOCKED allows multiple workers
       to safely claim different jobs.
    """

    recover_stale_jobs()

    with engine.begin() as db:

        result = db.execute(
            text(
                """
                SELECT
                    id,
                    name,
                    command,
                    attempts,
                    max_retries,
                    priority,
                    next_run_at,

                    LEAST(
                        FLOOR(
                            EXTRACT(
                                EPOCH FROM (
                                    CURRENT_TIMESTAMP - created_at
                                )
                            ) / :aging_interval
                        )::INTEGER,
                        :max_aging_bonus
                    ) AS aging_bonus

                FROM jobs

                WHERE status = 'pending'

                  AND (
                      next_run_at IS NULL
                      OR next_run_at <= CURRENT_TIMESTAMP
                  )

                ORDER BY
                    (
                        priority
                        +
                        LEAST(
                            FLOOR(
                                EXTRACT(
                                    EPOCH FROM (
                                        CURRENT_TIMESTAMP - created_at
                                    )
                                ) / :aging_interval
                            )::INTEGER,
                            :max_aging_bonus
                        )
                    ) DESC,

                    created_at ASC,
                    id ASC

                FOR UPDATE SKIP LOCKED

                LIMIT 1
                """
            ),
            {
                "aging_interval": AGING_INTERVAL_SECONDS,
                "max_aging_bonus": MAX_AGING_BONUS,
            },
        )

        job = result.mappings().first()

        if job is None:
            return None

        # Mark job as running and increment attempt count

        db.execute(
            text(
                """
                UPDATE jobs
                SET
                    status = 'running',
                    attempts = attempts + 1,
                    next_run_at = NULL,
                    last_error = NULL,
                    worker_id = :worker_id,
                    claimed_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """
            ),
            {
                "id": job["id"],
                "worker_id": WORKER_ID,
            },
        )

        job = dict(job)

        # Keep local copy consistent with database

        job["attempts"] += 1

        return job


# ============================================================
# Execute job
# ============================================================

def execute_job(job):

    log(
        "job_execution_started",
        job_id=job["id"],
        job_name=job["name"],
        command=job["command"],
    )

    effective_priority = (
        job["priority"]
        + min(job["aging_bonus"], MAX_AGING_BONUS)
    )

    log(
        "job_priority_computed",
        job_id=job["id"],
        base_priority=job["priority"],
        aging_bonus=job["aging_bonus"],
        effective_priority=effective_priority,
    )

    status = "failed"
    error_message = None

    # ========================================================
    # Best-effort safety check before running anything
    # ========================================================

    block_reason = command_is_blocked(job["command"])

    if block_reason is not None:

        error_message = block_reason

        log(
            "job_rejected_blocked_command",
            level="warning",
            job_id=job["id"],
            reason=block_reason,
        )

    else:

        # ====================================================
        # Execute command
        # ====================================================

        try:

            result = subprocess.run(
                job["command"],
                shell=True,
                capture_output=True,
                text=True,
                timeout=JOB_TIMEOUT_SECONDS,
                preexec_fn=(
                    _apply_job_resource_limits
                    if os.name == "posix"
                    else None
                ),
            )

            if result.returncode == 0:

                status = "completed"

                log(
                    "job_completed",
                    job_id=job["id"],
                    stdout=(
                        result.stdout.strip()[:2000]
                        if result.stdout
                        else None
                    ),
                )

            else:

                error_message = (
                    result.stderr.strip()
                    if result.stderr
                    else f"Exit code {result.returncode}"
                )

                log(
                    "job_failed",
                    level="warning",
                    job_id=job["id"],
                    exit_code=result.returncode,
                    error=error_message[:2000],
                )

        except subprocess.TimeoutExpired:

            error_message = (
                f"Job timed out after {JOB_TIMEOUT_SECONDS} seconds"
            )

            log(
                "job_failed",
                level="warning",
                job_id=job["id"],
                reason="timeout",
            )

        except MemoryError:

            error_message = (
                f"Job exceeded memory limit of "
                f"{JOB_MAX_MEMORY_MB}MB"
            )

            log(
                "job_failed",
                level="warning",
                job_id=job["id"],
                reason="memory_limit_exceeded",
            )

        except Exception as exc:

            error_message = str(exc)

            log(
                "job_failed",
                level="error",
                job_id=job["id"],
                reason="unexpected_error",
                error=str(exc),
            )

    # ========================================================
    # Update database
    # ========================================================

    with engine.begin() as db:

        # ====================================================
        # SUCCESS
        # ====================================================

        if status == "completed":

            db.execute(
                text(
                    """
                    UPDATE jobs
                    SET
                        status = 'completed',
                        next_run_at = NULL,
                        last_error = NULL
                    WHERE id = :id
                    """
                ),
                {
                    "id": job["id"],
                },
            )

            log("job_marked_completed", job_id=job["id"])

        # ====================================================
        # FAILURE
        # ====================================================

        else:

            # max_retries means retries AFTER the first attempt.
            #
            # max_retries = 3:
            #
            # Attempt 1 -> retry
            # Attempt 2 -> retry
            # Attempt 3 -> retry
            # Attempt 4 -> permanently failed

            if job["attempts"] <= job["max_retries"]:

                # ============================================
                # RETRY WITH EXPONENTIAL BACKOFF
                # ============================================

                retry_delay = min(
                    5 * (2 ** (job["attempts"] - 1)),
                    60,
                )

                next_run_at = (
                    datetime.now(timezone.utc).replace(tzinfo=None)
                    + timedelta(seconds=retry_delay)
                )

                db.execute(
                    text(
                        """
                        UPDATE jobs
                        SET
                            status = 'pending',
                            worker_id = NULL,
                            claimed_at = NULL,
                            next_run_at = :next_run_at,
                            last_error = :error
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": job["id"],
                        "error": error_message,
                        "next_run_at": next_run_at,
                    },
                )

                log(
                    "job_scheduled_for_retry",
                    job_id=job["id"],
                    attempt=job["attempts"],
                    max_attempts=job["max_retries"] + 1,
                    retry_delay_seconds=retry_delay,
                )

            # ================================================
            # PERMANENT FAILURE
            # ================================================

            else:

                db.execute(
                    text(
                        """
                        UPDATE jobs
                        SET
                            status = 'failed',
                            next_run_at = NULL,
                            last_error = :error
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": job["id"],
                        "error": error_message,
                    },
                )

                log(
                    "job_permanently_failed",
                    level="error",
                    job_id=job["id"],
                    attempts=job["attempts"],
                    error=error_message,
                )


# ============================================================
# Deregister worker
# ============================================================

def deregister_worker():
    """
    Best-effort removal of this worker's row on graceful shutdown,
    so the dashboard doesn't show a worker that's already gone
    while waiting out the stale-heartbeat timeout.
    """

    try:
        with engine.begin() as db:
            db.execute(
                text("DELETE FROM workers WHERE worker_id = :worker_id"),
                {"worker_id": WORKER_ID},
            )

        log("worker_deregistered")

    except Exception as exc:
        log("worker_deregister_failed", level="warning", error=str(exc))


# ============================================================
# Worker loop
# ============================================================

def worker_loop():

    register_worker()

    heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        daemon=True,
    )

    heartbeat_thread.start()

    log("worker_started")
    log("worker_waiting_for_jobs")

    while not shutdown_event.is_set():

        try:

            job = claim_job()

            if job is None:

                # Sleep in short increments so a shutdown signal
                # received while idle is noticed within ~0.2s
                # instead of waiting out a full 2s sleep.
                shutdown_event.wait(timeout=2)

                continue

            execute_job(job)

        except Exception as exc:

            log("worker_loop_error", level="error", error=str(exc))

            shutdown_event.wait(timeout=2)

    log("worker_shutting_down_gracefully")
    deregister_worker()


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    worker_loop()