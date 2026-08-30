import os
import socket
import subprocess
import threading
import time

from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text


load_dotenv()


DATABASE_URL = os.environ["DATABASE_URL"]


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)


# ============================================================
# Scheduler configuration
# ============================================================

AGING_INTERVAL_SECONDS = 10
MAX_AGING_BONUS = 100

WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"

HEARTBEAT_INTERVAL_SECONDS = 5
STALE_WORKER_TIMEOUT_SECONDS = 15


# ============================================================
# Worker registration
# ============================================================

def register_worker():

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

    print(
        f"[WORKER] Registered worker: {WORKER_ID}",
        flush=True,
    )


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

            print(
                f"[WORKER] Heartbeat error: {exc}",
                flush=True,
            )

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

            print(
                f"[WORKER] Recovered stale job "
                f"id={job['id']} "
                f"name={job['name']} "
                f"from worker={job['old_worker_id']}",
                flush=True,
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

    print(
        f"[WORKER] Worker={WORKER_ID} "
        f"Executing job id={job['id']} "
        f"name={job['name']}",
        flush=True,
    )

    print(
        f"[WORKER] Command: {job['command']}",
        flush=True,
    )

    effective_priority = (
        job["priority"]
        + min(job["aging_bonus"], MAX_AGING_BONUS)
    )

    print(
        f"[WORKER] Priority: {job['priority']} | "
        f"Aging bonus: {job['aging_bonus']} | "
        f"Effective priority: {effective_priority}",
        flush=True,
    )

    status = "failed"
    error_message = None

    # ========================================================
    # Execute command
    # ========================================================

    try:

        result = subprocess.run(
            job["command"],
            shell=True,
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode == 0:

            status = "completed"

            print(
                f"[WORKER] Worker={WORKER_ID} "
                f"Job {job['id']} completed",
                flush=True,
            )

            if result.stdout:

                print(
                    f"[WORKER] Output: "
                    f"{result.stdout.strip()}",
                    flush=True,
                )

        else:

            error_message = (
                result.stderr.strip()
                if result.stderr
                else f"Exit code {result.returncode}"
            )

            print(
                f"[WORKER] Worker={WORKER_ID} "
                f"Job {job['id']} FAILED "
                f"with exit code {result.returncode}",
                flush=True,
            )

            print(
                f"[WORKER] Failure reason: {error_message}",
                flush=True,
            )

    except subprocess.TimeoutExpired:

        error_message = "Job timed out after 300 seconds"

        print(
            f"[WORKER] Worker={WORKER_ID} "
            f"Job {job['id']} FAILED: timeout",
            flush=True,
        )

    except Exception as exc:

        error_message = str(exc)

        print(
            f"[WORKER] Worker={WORKER_ID} "
            f"Job {job['id']} FAILED with error: {exc}",
            flush=True,
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

            print(
                f"[WORKER] Job {job['id']} "
                f"marked as COMPLETED in database",
                flush=True,
            )

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

                print(
                    f"[WORKER] Job {job['id']} "
                    f"will be RETRIED",
                    flush=True,
                )

                print(
                    f"[WORKER] Attempt "
                    f"{job['attempts']} "
                    f"of {job['max_retries'] + 1}",
                    flush=True,
                )

                print(
                    f"[WORKER] Retry scheduled in "
                    f"{retry_delay} seconds",
                    flush=True,
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

                print(
                    f"[WORKER] Worker={WORKER_ID} "
                    f"Job {job['id']} "
                    f"PERMANENTLY FAILED",
                    flush=True,
                )

                print(
                    f"[WORKER] Job {job['id']} "
                    f"failed after {job['attempts']} attempts",
                    flush=True,
                )

                print(
                    f"[WORKER] Final error: "
                    f"{error_message}",
                    flush=True,
                )


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

    print(
        f"[WORKER] Worker started: {WORKER_ID}",
        flush=True,
    )

    print(
        "[WORKER] Waiting for pending jobs...",
        flush=True,
    )

    while True:

        try:

            job = claim_job()

            if job is None:

                time.sleep(2)

                continue

            execute_job(job)

        except Exception as exc:

            print(
                f"[WORKER] Worker loop error: {exc}",
                flush=True,
            )

            time.sleep(2)


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    worker_loop()