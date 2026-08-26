from sqlalchemy import text

from app.database import engine


def migrate():
    with engine.begin() as db:

        # Create workers table
        db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id VARCHAR(100) PRIMARY KEY,
                    status VARCHAR(50) NOT NULL DEFAULT 'active',
                    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        # Add worker_id to existing jobs table
        db.execute(
            text(
                """
                ALTER TABLE jobs
                ADD COLUMN IF NOT EXISTS worker_id VARCHAR(100)
                """
            )
        )

        # Add claimed_at to existing jobs table
        db.execute(
            text(
                """
                ALTER TABLE jobs
                ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP
                """
            )
        )

    print("[MIGRATION] Worker schema updated successfully.")


if __name__ == "__main__":
    migrate()