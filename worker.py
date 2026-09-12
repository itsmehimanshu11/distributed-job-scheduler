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
# These RLIMIT-based caps apply in SANDBOX_MODE=subprocess (job
# commands run directly on the worker's own OS). They are
# defense-in-depth, NOT a full sandbox in that mode -- a job still
# runs as this worker process's user with its network access. For
# untrusted job submitters, use SANDBOX_MODE=docker instead (see
# below), which runs each job in an isolated, disposable container.

JOB_MAX_MEMORY_MB = int(os.getenv("JOB_MAX_MEMORY_MB", "512"))
JOB_MAX_CPU_SECONDS = int(os.getenv("JOB_MAX_CPU_SECONDS", "280"))
JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", "300"))

# ============================================================
# Sandboxed (container-per-job) execution
# ============================================================
#
# SANDBOX_MODE=subprocess (default): runs the job command directly
# on the worker's own OS via subprocess, bounded only by the
# resource limits above. Fine for trusted/internal use.
#
# SANDBOX_MODE=docker: runs the job command inside a fresh,
# disposable container instead -- no access to the worker's
# filesystem, no network by default, a non-root user, and a
# read-only root filesystem. This is real isolation, not just a
# resource cap, and is what makes it reasonable to accept job
# submissions from less-trusted callers.
#
# Docker mode requires the worker to have access to a Docker
# socket (see docker-compose.yml: mounting /var/run/docker.sock
# into the worker container). If the socket isn't reachable,
# docker mode logs a warning once and falls back to subprocess
# mode rather than silently failing every job.

SANDBOX_MODE = os.getenv("SANDBOX_MODE", "subprocess").strip().lower()
JOB_RUNNER_IMAGE = os.getenv("JOB_RUNNER_IMAGE", "python:3.12-slim")
JOB_NETWORK_DISABLED = os.getenv("JOB_NETWORK_DISABLED", "true").lower() == "true"

try:
    import docker as docker_sdk
except ImportError:
    docker_sdk = None

_docker_client = None
_docker_client_init_failed = False


def _get_docker_client():
    """
    Lazily creates (and caches) a Docker client for sandboxed
    execution. Returns None if Docker isn't available, logging the
    reason exactly once so the worker doesn't spam logs on every
    job while running in subprocess fallback.
    """

    global _docker_client, _docker_client_init_failed

    if _docker_client is not None:
        return _docker_client

    if _docker_client_init_failed:
        return None

    if docker_sdk is None:
        log(
            "docker_sandbox_unavailable",
            level="warning",
            reason="docker SDK not installed",
        )
        _docker_client_init_failed = True
        return None

    try:
        client = docker_sdk.from_env()
        client.ping()
        _docker_client = client
        return _docker_client

    except Exception as exc:
        log(
            "docker_sandbox_unavailable",
            level="warning",
            reason=str(exc),
        )
        _docker_client_init_failed = True
        return None


def run_job_in_container(command: str):
    """
    Executes `command` inside a fresh, disposable container.

    Returns a dict shaped like a subprocess.CompletedProcess for a
    uniform interface with run_job_subprocess: {returncode, stdout,
    stderr}. Raises the same exceptions execute_job() already
    handles (TimeoutError, generic Exception) so the calling logic
    doesn't need to know which backend ran the job.
    """

    client = _get_docker_client()

    if client is None:
        raise RuntimeError(
            "Docker sandbox requested but unavailable "
            "(falling back to subprocess mode)"
        )

    container = client.containers.run(
        image=JOB_RUNNER_IMAGE,
        command=["sh", "-c", command],
        detach=True,
        remove=False,
        network_disabled=JOB_NETWORK_DISABLED,
        mem_limit=f"{JOB_MAX_MEMORY_MB}m",
        # cpu_period/cpu_quota together cap CPU as a fraction of a
        # core over each 100ms period, e.g. quota=50000 with the
        # default 100000 period caps usage at 0.5 CPU cores.
        cpu_period=100000,
        cpu_quota=100000,
        user="nobody",
        read_only=True,
        # The job may still need to write temp files even though
        # the root filesystem is read-only.
        tmpfs={"/tmp": "size=64m"},
        security_opt=["no-new-privileges"],
        labels={"scheduler.managed": "job-sandbox"},
    )

    try:
        result = container.wait(timeout=JOB_TIMEOUT_SECONDS)
        exit_code = result.get("StatusCode", 1)

        logs = container.logs(stdout=True, stderr=True).decode(
            "utf-8", errors="replace"
        )

        return {
            "returncode": exit_code,
            "stdout": logs,
            "stderr": "" if exit_code == 0 else logs,
        }

    except Exception as exc:
        # docker-py raises on a wait() timeout among other things;
        # normalize to the same TimeoutError execute_job() already
        # catches for the subprocess path.
        if "timeout" in str(exc).lower():
            raise TimeoutError(
                f"Job timed out after {JOB_TIMEOUT_SECONDS} seconds"
            ) from exc
        raise

    finally:
        try:
            container.remove(force=True)
        except Exception:
            pass


def run_job_subprocess(command: str):
    """
    Executes `command` directly on the worker's own OS via
    subprocess, bounded by the RLIMIT-based resource caps. Returns
    the same {returncode, stdout, stderr} shape as
    run_job_in_container for a uniform call site.
    """

    result = subprocess.run(
        command,
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

    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }

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
        # Execute command (sandboxed container or subprocess)
        # ====================================================

        use_docker = SANDBOX_MODE == "docker"

        try:

            if use_docker:
                try:
                    result = run_job_in_container(job["command"])
                except RuntimeError:
                    # Docker unavailable -- fall back rather than
                    # fail every job outright.
                    log(
                        "job_sandbox_fallback",
                        level="warning",
                        job_id=job["id"],
                    )
                    result = run_job_subprocess(job["command"])
            else:
                result = run_job_subprocess(job["command"])

            if result["returncode"] == 0:

                status = "completed"

                log(
                    "job_completed",
                    job_id=job["id"],
                    sandbox="docker" if use_docker else "subprocess",
                    stdout=(
                        result["stdout"].strip()[:2000]
                        if result["stdout"]
                        else None
                    ),
                )

            else:

                error_message = (
                    result["stderr"].strip()
                    if result["stderr"]
                    else f"Exit code {result['returncode']}"
                )

                log(
                    "job_failed",
                    level="warning",
                    job_id=job["id"],
                    exit_code=result["returncode"],
                    error=error_message[:2000],
                )

        except (subprocess.TimeoutExpired, TimeoutError):

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