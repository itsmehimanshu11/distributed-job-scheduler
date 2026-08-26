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

    print(f"[WORKER] Registered worker: {WORKER_ID}")


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

            print(f"[WORKER] Heartbeat error: {exc}")

        time.sleep(HEARTBEAT_INTERVAL_SECONDS)

# ============================================================
# recover stale jobs
# ============================================================

def recover_stale_jobs():
    """
    Recover jobs whose assigned worker has stopped sending heartbeats.

    A worker is considered stale when its last heartbeat is older than
    STALE_WORKER_TIMEOUT_SECONDS.
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
                                - (:stale_timeout * INTERVAL '1 second')
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
                f"id={job['id']} name={job['name']} "
                f"from worker={job['old_worker_id']}"
            )

        return len(recovered_jobs)

# ============================================================
# Claim job
# ============================================================

def claim_job():
    """
    Atomically claim one eligible pending job.

    Scheduling policy:

    1. Calculate aging bonus from how long the job has waited.
    2. effective_priority = priority + aging bonus.
    3. Highest effective priority wins.
    4. Older jobs win when effective priorities are equal.
    5. next_run_at must be due.
    6. FOR UPDATE SKIP LOCKED allows multiple workers
       to safely claim different jobs concurrently.
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
        f"[WORKER] Executing job "
        f"id={job['id']} name={job['name']}"
    )

    print(
        f"[WORKER] Command: {job['command']}"
    )

    print(
        f"[WORKER] Priority: {job['priority']} | "
        f"Aging bonus: {job['aging_bonus']} | "
        f"Effective priority: "
        f"{job['priority'] + min(job['aging_bonus'], MAX_AGING_BONUS)}"
    )

    status = "failed"
    error_message = None

    # --------------------------------------------------------
    # Execute command
    # --------------------------------------------------------

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
                f"[WORKER] Job {job['id']} completed"
            )

            if result.stdout:

                print(
                    f"[WORKER] Output: "
                    f"{result.stdout.strip()}"
                )

        else:

            error_message = (
                result.stderr.strip()
                if result.stderr
                else f"Exit code {result.returncode}"
            )

            print(
                f"[WORKER] Job {job['id']} failed "
                f"with exit code {result.returncode}"
            )

            if result.stderr:

                print(
                    f"[WORKER] Error: "
                    f"{result.stderr.strip()}"
                )

    except subprocess.TimeoutExpired:

        error_message = "Job timed out after 300 seconds"

        print(
            f"[WORKER] Job {job['id']} timed out"
        )

    except Exception as exc:

        error_message = str(exc)

        print(
            f"[WORKER] Job {job['id']} error: {exc}"
        )

    # --------------------------------------------------------
    # Update database
    # --------------------------------------------------------

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
                f"marked as completed in database"
            )

        # ====================================================
        # FAILURE
        # ====================================================

        else:

            # max_retries means retries AFTER the first attempt.
            #
            # Example:
            #
            # max_retries = 3
            #
            # Attempt 1 -> retry
            # Attempt 2 -> retry
            # Attempt 3 -> retry
            # Attempt 4 -> permanently failed

            if job["attempts"] <= job["max_retries"]:

                # ------------------------------------------------
                # Exponential backoff
                #
                # Attempt 1 -> 5 seconds
                # Attempt 2 -> 10 seconds
                # Attempt 3 -> 20 seconds
                # Attempt 4 -> 40 seconds
                #
                # Maximum delay = 60 seconds
                # ------------------------------------------------

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
                    f"[WORKER] Job {job['id']} will be retried "
                    f"(attempt {job['attempts']} "
                    f"of {job['max_retries'] + 1})"
                )

                print(
                    f"[WORKER] Retry scheduled in "
                    f"{retry_delay} seconds"
                )

            # ====================================================
            # PERMANENT FAILURE
            # ====================================================

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
                    f"[WORKER] Job {job['id']} permanently failed "
                    f"after {job['attempts']} attempts"
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

    print("[WORKER] Worker started")
    print("[WORKER] Waiting for pending jobs...")

    while True:

        job = claim_job()

        if job is None:

            time.sleep(2)

            continue

        execute_job(job)


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    worker_loop()