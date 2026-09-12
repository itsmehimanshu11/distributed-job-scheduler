"""
DEPRECATED: this one-off script is kept only so old deployment
instructions don't hard-fail. Schema changes are now managed with
Alembic (see the `alembic/` directory).

To bring an existing database up to date:

    # If the database already has the `workers` table and the
    # worker_id/claimed_at columns on `jobs` (i.e. you previously
    # ran this script), tell Alembic it's already at the baseline:
    alembic stamp 0001_baseline

    # Then apply anything after the baseline (e.g. the dedupe_key
    # column):
    alembic upgrade head

For a brand-new database, just run `alembic upgrade head` -- no
need to stamp anything first.
"""

import sys


def migrate():
    print(
        "[MIGRATION] This script is deprecated. "
        "Use Alembic instead:\n\n"
        "  alembic stamp 0001_baseline   # existing DB only\n"
        "  alembic upgrade head\n"
    )
    sys.exit(1)


if __name__ == "__main__":
    migrate()
