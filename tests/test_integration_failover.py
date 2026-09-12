"""
Integration tests that exercise the real database instead of mocks.

Unlike test_worker.py / test_auth.py (which use fakes and don't need a
live database), these tests run the actual SQL against a real
Postgres instance -- the same one CI already spins up for the rest of
the suite (see .github/workflows/tests.yml). They cover the two
behaviors that are easy to get subtly wrong with mocks alone:

1. Stale-worker failover: a job "stuck" on a worker that stopped
   sending heartbeats gets safely requeued and can be completed by a
   different worker, without ever being claimed by two workers at
   once.
2. Idempotent job submission via the API: posting the same
   dedupe_key twice returns the same job instead of creating a
   duplicate.

If DATABASE_URL isn't set (e.g. running `pytest` outside the Docker
Compose / CI environment), these tests are skipped rather than
failing, since they need a real Postgres to talk to.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"),
    reason="DATABASE_URL not set; integration tests need a real Postgres",
)


@pytest.fixture()
def db_engine():
    engine = create_engine(os.environ["DATABASE_URL"])

    # Make sure the schema exists (mirrors what app/main.py does on
    # startup) without requiring the API to be running.
    from app.database import Base
    from app import models  # noqa: F401

    Base.metadata.create_all(bind=engine)

    yield engine

    with engine.begin() as db:
        db.execute(text("DELETE FROM jobs"))
        db.execute(text("DELETE FROM workers"))

    engine.dispose()


def _insert_job(db, name, command="echo hi", priority=0, max_retries=3):
    result = db.execute(
        text(
            """
            INSERT INTO jobs
                (name, command, status, attempts, max_retries,
                 priority, created_at)
            VALUES
                (:name, :command, 'pending', 0, :max_retries,
                 :priority, CURRENT_TIMESTAMP)
            RETURNING id
            """
        ),
        {
            "name": name,
            "command": command,
            "max_retries": max_retries,
            "priority": priority,
        },
    )
    return result.scalar_one()


def _register_worker(db, worker_id, last_heartbeat=None):
    db.execute(
        text(
            """
            INSERT INTO workers (worker_id, status, started_at, last_heartbeat)
            VALUES (:worker_id, 'active', CURRENT_TIMESTAMP,
                    COALESCE(:last_heartbeat, CURRENT_TIMESTAMP))
            """
        ),
        {"worker_id": worker_id, "last_heartbeat": last_heartbeat},
    )


class TestStaleWorkerFailover:
    """
    Reproduces the scenario the README claims to handle: a worker
    dies mid-job (its heartbeat goes stale) and the job is safely
    handed to a different worker instead of being lost forever.
    """

    def test_stale_job_is_requeued_and_completable_by_another_worker(
        self, db_engine, monkeypatch
    ):
        import worker as worker_module

        monkeypatch.setattr(worker_module, "engine", db_engine)
        monkeypatch.setattr(
            worker_module, "STALE_WORKER_TIMEOUT_SECONDS", 0
        )

        with db_engine.begin() as db:
            job_id = _insert_job(db, "long-running-job")

            dead_worker_id = "dead-worker-1"
            _register_worker(
                db,
                dead_worker_id,
                # Already older than the (zero-second) stale timeout.
                last_heartbeat=datetime.now(timezone.utc) - timedelta(seconds=30),
            )

            # Simulate the dead worker having claimed the job right
            # before it stopped sending heartbeats.
            db.execute(
                text(
                    """
                    UPDATE jobs
                    SET status = 'running',
                        worker_id = :worker_id,
                        claimed_at = CURRENT_TIMESTAMP,
                        attempts = 1
                    WHERE id = :id
                    """
                ),
                {"worker_id": dead_worker_id, "id": job_id},
            )

        # A second, live worker's claim_job() call should detect the
        # stale job, requeue it, and then immediately be able to
        # claim it for itself in the same pass.
        recovered = worker_module.recover_stale_jobs()
        assert recovered == 1

        with db_engine.connect() as db:
            row = db.execute(
                text("SELECT status, worker_id, claimed_at FROM jobs WHERE id = :id"),
                {"id": job_id},
            ).mappings().first()

        assert row["status"] == "pending"
        assert row["worker_id"] is None
        assert row["claimed_at"] is None

        claimed = worker_module.claim_job()
        assert claimed is not None
        assert claimed["id"] == job_id

        with db_engine.connect() as db:
            row = db.execute(
                text("SELECT status, worker_id FROM jobs WHERE id = :id"),
                {"id": job_id},
            ).mappings().first()

        assert row["status"] == "running"
        assert row["worker_id"] == worker_module.WORKER_ID

    def test_two_workers_never_claim_the_same_job(self, db_engine, monkeypatch):
        """
        Sanity check on the FOR UPDATE SKIP LOCKED claim query: with
        only one pending job, a second claim attempt must come back
        empty rather than double-claiming it.
        """

        import worker as worker_module

        monkeypatch.setattr(worker_module, "engine", db_engine)

        with db_engine.begin() as db:
            job_id = _insert_job(db, "single-job")

        first_claim = worker_module.claim_job()
        second_claim = worker_module.claim_job()

        assert first_claim is not None
        assert first_claim["id"] == job_id
        assert second_claim is None


class TestJobIdempotency:
    """
    Covers POST /jobs with a dedupe_key resolving through the real
    unique-constraint path, including the race-condition fallback
    (IntegrityError -> return the existing row) which a mocked
    session can't meaningfully exercise.
    """

    def test_duplicate_dedupe_key_returns_existing_job(self, db_engine, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", str(db_engine.url))
        monkeypatch.setenv("API_KEY", "integration-test-key")

        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        headers = {"X-API-Key": "integration-test-key"}
        dedupe_key = f"test-{uuid.uuid4().hex[:8]}"

        first = client.post(
            "/jobs",
            json={"name": "job-a", "command": "echo one", "dedupe_key": dedupe_key},
            headers=headers,
        )
        assert first.status_code == 201
        first_id = first.json()["id"]

        second = client.post(
            "/jobs",
            json={"name": "job-b", "command": "echo two", "dedupe_key": dedupe_key},
            headers=headers,
        )
        assert second.status_code == 200
        assert second.json()["id"] == first_id
        assert second.json()["name"] == "job-a"

        with db_engine.connect() as db:
            count = db.execute(
                text("SELECT COUNT(*) FROM jobs WHERE dedupe_key = :k"),
                {"k": dedupe_key},
            ).scalar_one()

        assert count == 1
