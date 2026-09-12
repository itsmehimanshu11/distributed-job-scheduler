"""
Tests for the SQLAlchemy model column defaults in app/models.py.

The rest of the test suite never actually exercises Worker.started_at,
Worker.last_heartbeat, or Job.created_at's ORM-level defaults -- every
other test either inserts via raw SQL with an explicit timestamp, or
the API sets created_at explicitly. That's exactly why a previous
regression here (a deprecated datetime.utcnow callable left in place)
went unnoticed: no test actually created a row through the ORM
constructor without supplying a timestamp. These tests close that gap.
"""

import os
import warnings

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"),
    reason="DATABASE_URL not set; these tests need a real Postgres",
)


@pytest.fixture()
def db_session():
    engine = create_engine(os.environ["DATABASE_URL"])

    from app.database import Base
    from app import models  # noqa: F401

    Base.metadata.create_all(bind=engine)

    session = Session(engine)
    yield session
    session.close()

    with engine.begin() as db:
        from sqlalchemy import text
        db.execute(text("DELETE FROM jobs"))
        db.execute(text("DELETE FROM workers"))

    engine.dispose()


class TestModelDefaultsAreNotDeprecated:
    def test_worker_defaults_do_not_emit_deprecation_warning(self, db_session):
        from app.models import Worker

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            worker = Worker(worker_id="model-default-test-worker")
            db_session.add(worker)
            db_session.commit()

        assert worker.started_at is not None
        assert worker.last_heartbeat is not None

    def test_job_defaults_do_not_emit_deprecation_warning(self, db_session):
        from app.models import Job

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            job = Job(name="model-default-test-job", command="echo hi")
            db_session.add(job)
            db_session.commit()

        assert job.created_at is not None

    def test_worker_and_job_timestamps_are_timezone_naive_utc_in_db(self, db_session):
        from datetime import datetime, timezone
        from app.models import Worker

        worker = Worker(worker_id="model-default-timestamp-check")
        db_session.add(worker)
        db_session.commit()
        db_session.refresh(worker)

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        assert abs((now - worker.started_at).total_seconds()) < 10
