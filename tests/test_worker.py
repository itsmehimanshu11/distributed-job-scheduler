import subprocess
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import worker


class FakeDB:
    def __init__(self, first_result=None):
        self.first_result = first_result
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append((statement, params))

        result = MagicMock()

        if len(self.calls) == 1 and self.first_result is not None:
            result.mappings.return_value.first.return_value = self.first_result
        else:
            result.mappings.return_value.first.return_value = None

        return result


class FakeEngine:
    def __init__(self, db):
        self.db = db

    def begin(self):
        return self

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


# ============================================================
# claim_job tests
# ============================================================


def test_claim_job_returns_none_when_no_job(monkeypatch):
    """claim_job() should return None when no pending job exists."""

    db = FakeDB(first_result=None)

    monkeypatch.setattr(worker, "engine", FakeEngine(db))
    monkeypatch.setattr(worker, "recover_stale_jobs", lambda: 0)

    result = worker.claim_job()

    assert result is None


def test_claim_job_claims_pending_job(monkeypatch):
    """claim_job() should mark a pending job as running."""

    job = {
        "id": 101,
        "name": "TEST_JOB",
        "command": "echo hello",
        "attempts": 0,
        "max_retries": 3,
        "priority": 100,
        "next_run_at": None,
        "aging_bonus": 0,
    }

    db = FakeDB(first_result=job)

    monkeypatch.setattr(worker, "engine", FakeEngine(db))
    monkeypatch.setattr(worker, "recover_stale_jobs", lambda: 0)
    monkeypatch.setattr(worker, "WORKER_ID", "TEST-WORKER")

    result = worker.claim_job()

    assert result is not None
    assert result["id"] == 101
    assert result["attempts"] == 1

    # SELECT + UPDATE
    assert len(db.calls) == 2

    update_params = db.calls[1][1]

    assert update_params["id"] == 101
    assert update_params["worker_id"] == "TEST-WORKER"


def test_claim_job_uses_priority_and_locking(monkeypatch):
    """
    Verify the claim query contains the important distributed
    scheduling mechanisms.
    """

    job = {
        "id": 102,
        "name": "HIGH_PRIORITY",
        "command": "echo priority",
        "attempts": 0,
        "max_retries": 3,
        "priority": 100,
        "next_run_at": None,
        "aging_bonus": 5,
    }

    db = FakeDB(first_result=job)

    monkeypatch.setattr(worker, "engine", FakeEngine(db))
    monkeypatch.setattr(worker, "recover_stale_jobs", lambda: 0)

    worker.claim_job()

    query = str(db.calls[0][0]).upper()

    assert "ORDER BY" in query
    assert "PRIORITY" in query
    assert "CREATED_AT" in query
    assert "FOR UPDATE SKIP LOCKED" in query


# ============================================================
# execute_job success tests
# ============================================================


def test_execute_job_success(monkeypatch):
    """A successful command should mark the job as completed."""

    job = {
        "id": 200,
        "name": "SUCCESS_TEST",
        "command": "echo SUCCESS",
        "attempts": 1,
        "max_retries": 3,
        "priority": 100,
        "aging_bonus": 0,
    }

    db = FakeDB()

    monkeypatch.setattr(worker, "engine", FakeEngine(db))

    completed = subprocess.CompletedProcess(
        args=job["command"],
        returncode=0,
        stdout="SUCCESS\n",
        stderr="",
    )

    monkeypatch.setattr(
        worker.subprocess,
        "run",
        lambda *args, **kwargs: completed,
    )

    worker.execute_job(job)

    assert len(db.calls) == 1

    params = db.calls[0][1]

    assert params["id"] == 200

    query = str(db.calls[0][0]).lower()

    assert "status = 'completed'" in query


# ============================================================
# execute_job retry tests
# ============================================================


def test_execute_job_failure_schedules_retry(monkeypatch):
    """A failed command should be returned to pending when retries remain."""

    job = {
        "id": 201,
        "name": "RETRY_TEST",
        "command": "cmd /c exit 1",
        "attempts": 1,
        "max_retries": 3,
        "priority": 100,
        "aging_bonus": 0,
    }

    db = FakeDB()

    monkeypatch.setattr(worker, "engine", FakeEngine(db))

    failed = subprocess.CompletedProcess(
        args=job["command"],
        returncode=1,
        stdout="",
        stderr="Test failure",
    )

    monkeypatch.setattr(
        worker.subprocess,
        "run",
        lambda *args, **kwargs: failed,
    )

    worker.execute_job(job)

    assert len(db.calls) == 1

    query = str(db.calls[0][0]).lower()
    params = db.calls[0][1]

    assert "status = 'pending'" in query
    assert params["error"] == "Test failure"
    assert params["next_run_at"] is not None


def test_execute_job_retry_uses_exponential_backoff(monkeypatch):
    """Retry delays should follow 5, 10, 20, 40 seconds."""

    expected_delays = {
        1: 5,
        2: 10,
        3: 20,
    }

    for attempt, expected_delay in expected_delays.items():

        job = {
            "id": 300 + attempt,
            "name": "BACKOFF_TEST",
            "command": "cmd /c exit 1",
            "attempts": attempt,
            "max_retries": 3,
            "priority": 100,
            "aging_bonus": 0,
        }

        db = FakeDB()

        monkeypatch.setattr(worker, "engine", FakeEngine(db))

        failed = subprocess.CompletedProcess(
            args=job["command"],
            returncode=1,
            stdout="",
            stderr="failure",
        )

        monkeypatch.setattr(
            worker.subprocess,
            "run",
            lambda *args, **kwargs: failed,
        )

        before = datetime.now(timezone.utc).replace(tzinfo=None)

        worker.execute_job(job)

        after = datetime.now(timezone.utc).replace(tzinfo=None)

        next_run_at = db.calls[0][1]["next_run_at"]

        expected_min = before + timedelta(seconds=expected_delay)
        expected_max = after + timedelta(seconds=expected_delay)

        assert expected_min <= next_run_at <= expected_max


# ============================================================
# Permanent failure tests
# ============================================================


def test_execute_job_permanently_fails_after_max_retries(monkeypatch):
    """
    When attempts exceed max_retries, the job should become failed
    instead of being retried again.
    """

    job = {
        "id": 400,
        "name": "PERMANENT_FAILURE_TEST",
        "command": "cmd /c exit 1",
        "attempts": 4,
        "max_retries": 3,
        "priority": 100,
        "aging_bonus": 0,
    }

    db = FakeDB()

    monkeypatch.setattr(worker, "engine", FakeEngine(db))

    failed = subprocess.CompletedProcess(
        args=job["command"],
        returncode=1,
        stdout="",
        stderr="Final failure",
    )

    monkeypatch.setattr(
        worker.subprocess,
        "run",
        lambda *args, **kwargs: failed,
    )

    worker.execute_job(job)

    assert len(db.calls) == 1

    query = str(db.calls[0][0]).lower()
    params = db.calls[0][1]

    assert "status = 'failed'" in query
    assert "next_run_at = null" in query
    assert params["error"] == "Final failure"