from datetime import datetime, timezone

from sqlalchemy import String, Text, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Worker(Base):
    __tablename__ = "workers"

    worker_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    status: Mapped[str] = mapped_column(String(50), default="active", nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    command: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(default=3, nullable=False)
    priority: Mapped[int] = mapped_column(default=0, nullable=False)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)
    dedupe_key: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True)
