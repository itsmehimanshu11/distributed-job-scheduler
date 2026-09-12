"""add dedupe_key to jobs

Revision ID: 0002_add_dedupe_key
Revises: 0001_baseline
Create Date: 2026-09-12

Adds an optional, unique dedupe_key column to the jobs table so clients
can submit jobs idempotently (POST /jobs with the same dedupe_key twice
returns the original job instead of creating a duplicate).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0002_add_dedupe_key"
down_revision: Union[str, None] = "0001_baseline"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("dedupe_key", sa.String(length=255), nullable=True),
    )
    op.create_unique_constraint(
        "uq_jobs_dedupe_key", "jobs", ["dedupe_key"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_jobs_dedupe_key", "jobs", type_="unique")
    op.drop_column("jobs", "dedupe_key")
