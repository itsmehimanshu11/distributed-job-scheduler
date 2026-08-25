from datetime import datetime

from sqlalchemy import String, Text, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


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

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        nullable=False,
    )