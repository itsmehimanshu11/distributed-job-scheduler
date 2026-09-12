"""baseline schema (workers + jobs tables)

Revision ID: 0001_baseline
Revises:
Create Date: 2026-09-12

This migration represents the schema as it existed before Alembic was
introduced into this project. It is safe to run against a brand-new
database (it creates both tables from scratch).

For an EXISTING deployment where the tables already exist (created via
Base.metadata.create_all on app startup), do not run this migration
directly -- instead run:

    alembic stamp 0001_baseline

which tells Alembic "the database is already at this revision" without
attempting to re-create the tables. Then run `alembic upgrade head` to
apply any migrations that come after it (e.g. 0002_add_dedupe_key).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001_baseline"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "workers",
        sa.Column("worker_id", sa.String(length=100), primary_key=True),
        sa.Column(
            "status",
            sa.String(length=50),
            nullable=False,
            server_default="active",
        ),
        sa.Column(
            "started_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "last_heartbeat",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    op.create_table(
        "jobs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("command", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=50),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "max_retries", sa.Integer(), nullable=False, server_default="3"
        ),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_run_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("worker_id", sa.String(length=100), nullable=True),
        sa.Column("claimed_at", sa.DateTime(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def downgrade() -> None:
    op.drop_table("jobs")
    op.drop_table("workers")
