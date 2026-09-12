from datetime import datetime, timezone

from sqlalchemy import String, Text, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


def _utcnow() -> datetime:
    """
    Timezone-aware replacement for the deprecated `datetime.utcnow`.
    Used as an ORM column default (a callable), not called directly
    here -- SQLAlchemy calls this itself at insert time for any row
    created without an explicit value for the column.
    """
    return datetime.now(timezone.utc)


class Worker(Base):
    __tablename__ = "workers"

    worker_id: Mapped[str] = mapped_column(
        String(100),
        primary_key=True,
    )

    status: Mapped[str] = mapped_column(
        String(50),
        default="active",
        nullable=False,
    )

    started_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=_utcnow,
        nullable=False,
    )

    last_heartbeat: Mapped[datetime] = mapped_column(
        DateTime,
        default=_utcnow,
        nullable=False,
    )


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True,
    )

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    command: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        String(50),
        default="pending",
        nullable=False,
    )

    attempts: Mapped[int] = mapped_column(
        default=0,
        nullable=False,
    )

    max_retries: Mapped[int] = mapped_column(
        default=3,
        nullable=False,
    )

    # Higher number = higher priority
    priority: Mapped[int] = mapped_column(
        default=0,
        nullable=False,
    )

    # Job cannot be executed before this time.
    # Used for retry backoff.
    next_run_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )

    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    # Worker currently executing this job.
    worker_id: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
    )

    # When the current worker claimed the job.
    claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=_utcnow,
        nullable=False,
    )

    # Optional client-supplied key for idempotent job submission.
    # If a job with the same dedupe_key already exists, the API
    # returns the existing job instead of creating a duplicate.
    dedupe_key: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
    )
