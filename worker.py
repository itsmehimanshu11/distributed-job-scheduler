import subprocess
import time

from sqlalchemy import create_engine, text


DATABASE_URL = (
    "postgresql+psycopg://"
    "scheduler:scheduler_password@localhost:5432/scheduler_db"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)


def claim_job():
    """
    Atomically claim one pending job.

    FOR UPDATE SKIP LOCKED allows multiple workers
    to safely work on different jobs concurrently.
    """

    with engine.begin() as db:
        result = db.execute(
            text(
                """
                SELECT
                    id,
                    name,
                    command,
                    attempts,
                    max_retries
                FROM jobs
                WHERE status = 'pending'
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            )
        )

        job = result.mappings().first()

        if job is None:
            return None

        db.execute(
            text(
                """
                UPDATE jobs
                SET
                    status = 'running',
                    attempts = attempts + 1,
                    last_error = NULL
                WHERE id = :id
                """
            ),
            {
                "id": job["id"],
            },
        )

        job = dict(job)
        job["attempts"] += 1

        return job


def execute_job(job):
    print(
        f"[WORKER] Executing job "
        f"id={job['id']} name={job['name']}"
    )

    print(f"[WORKER] Command: {job['command']}")

    status = "failed"
    error_message = None

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

    with engine.begin() as db:

        if status == "completed":

            db.execute(
                text(
                    """
                    UPDATE jobs
                    SET
                        status = 'completed',
                        last_error = NULL
                    WHERE id = :id
                    """
                ),
                {
                    "id": job["id"],
                },
            )

        else:

            # max_retries means retries AFTER the first attempt.
            # Therefore total allowed attempts =
            # max_retries + 1.
            if job["attempts"] <= job["max_retries"]:

                db.execute(
                    text(
                        """
                        UPDATE jobs
                        SET
                            status = 'pending',
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
                    f"[WORKER] Job {job['id']} will be retried "
                    f"(attempt {job['attempts']} "
                    f"of {job['max_retries'] + 1})"
                )

            else:

                db.execute(
                    text(
                        """
                        UPDATE jobs
                        SET
                            status = 'failed',
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


def worker_loop():
    print("[WORKER] Worker started")
    print("[WORKER] Waiting for pending jobs...")

    while True:

        job = claim_job()

        if job is None:
            time.sleep(2)
            continue

        execute_job(job)


if __name__ == "__main__":
    worker_loop()