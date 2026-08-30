import os
import uuid
import docker

from docker.errors import DockerException
from threading import Lock
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware

from .database import engine, get_db, Base
from .models import Job, Worker


# ============================================================
# LOAD ENVIRONMENT
# ============================================================

load_dotenv()


# ============================================================
# DOCKER WORKER SCALING CONFIGURATION
# ============================================================

WORKER_IMAGE = os.getenv(
    "WORKER_IMAGE",
    "distributedjobscheduler-worker:latest",
)

WORKER_NETWORK = os.getenv(
    "WORKER_NETWORK",
    "distributedjobscheduler_default",
)

MIN_WORKERS = 1
MAX_WORKERS = 32

worker_scale_lock = Lock()


# ============================================================
# APPLICATION
# ============================================================

app = FastAPI(
    title="Distributed Job Scheduler API",
    description="A distributed background job scheduling service.",
    version="1.0.0",
)


# ============================================================
# CORS
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# FRONTEND
# ============================================================

app.mount(
    "/frontend",
    StaticFiles(directory="frontend"),
    name="frontend",
)


# ============================================================
# DATABASE
# ============================================================

Base.metadata.create_all(bind=engine)


# ============================================================
# API KEY AUTHENTICATION
# ============================================================

def verify_api_key(
    x_api_key: str | None = Header(default=None),
):
    """
    Verify the API key supplied through the X-API-Key header.
    """

    expected_api_key = os.getenv("API_KEY")

    if not expected_api_key:
        raise HTTPException(
            status_code=500,
            detail="API_KEY is not configured",
        )

    if x_api_key != expected_api_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key",
        )

    return True


# ============================================================
# REQUEST MODELS
# ============================================================

class JobCreate(BaseModel):
    name: str
    command: str

    priority: int = Field(
        default=0,
        ge=0,
        le=100,
    )

    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
    )


class WorkerScaleRequest(BaseModel):
    workers: int = Field(
        ...,
        ge=MIN_WORKERS,
        le=MAX_WORKERS,
    )


# ============================================================
# DOCKER HELPERS
# ============================================================

def get_docker_client():
    """
    Connect to the Docker Engine through the Docker socket.
    """

    try:
        client = docker.from_env()
        client.ping()
        return client

    except DockerException as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Docker Engine unavailable: {exc}",
        )


def get_worker_containers(client):
    """
    Return all workers belonging to this scheduler.

    Supports:

    1. The original Docker Compose worker.
    2. Workers dynamically created from the dashboard.
    """

    containers_by_id = {}

    # --------------------------------------------------------
    # Dynamically created workers
    # --------------------------------------------------------

    try:
        scheduler_workers = client.containers.list(
            filters={
                "label": [
                    "scheduler.managed=true",
                    "scheduler.component=worker",
                ]
            }
        )

        for container in scheduler_workers:
            containers_by_id[container.id] = container

    except DockerException:
        pass

    # --------------------------------------------------------
    # Original Docker Compose worker
    # --------------------------------------------------------

    try:
        compose_workers = client.containers.list(
            filters={
                "label": [
                    "com.docker.compose.service=worker",
                    "com.docker.compose.project=distributedjobscheduler",
                ]
            }
        )

        for container in compose_workers:
            containers_by_id[container.id] = container

    except DockerException:
        pass

    return list(containers_by_id.values())


def get_database_url():
    """
    Get the database URL that dynamically created workers
    should use.
    """

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        database_url = (
            "postgresql+psycopg://"
            "scheduler:scheduler_password"
            "@db:5432/"
            "scheduler_db"
        )

    return database_url


def create_worker_container(
    client,
    database_url,
):
    """
    Create one dynamically managed worker container.
    """

    container_name = (
        "distributedjobscheduler-worker-"
        + uuid.uuid4().hex[:8]
    )

    try:
        container = client.containers.run(
            image=WORKER_IMAGE,

            command=[
                "python",
                "worker.py",
            ],

            name=container_name,

            detach=True,

            network=WORKER_NETWORK,

            environment={
                "DATABASE_URL": database_url,
            },

            labels={
                "scheduler.managed": "true",
                "scheduler.component": "worker",

                # Keep Compose-compatible labels so the
                # worker is visible to the scheduler.
                "com.docker.compose.service": "worker",
                "com.docker.compose.project": (
                    "distributedjobscheduler"
                ),
            },

            restart_policy={
                "Name": "unless-stopped",
            },
        )

        return container

    except DockerException as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create worker: {exc}",
        )


def free_orphaned_jobs_for_container(db: Session, container):
    """
    Free up any job that is pending/running on the given
    container so a live worker can pick it up instead of it
    getting stuck forever.

    IMPORTANT: this matches directly against Job.worker_id
    (which is always accurate), NOT via a lookup in the
    `workers` table first. The `workers` table can be stale or
    missing a row for a brand-new worker, and depending on it
    to decide which jobs to free creates a race condition where
    jobs can be silently left stuck.

    Returns (freed_count, failed_count).
    """

    candidates = {
        container.id,
        container.short_id,
        container.name,
    }

    active_jobs = (
        db.query(Job)
        .filter(
            Job.worker_id.isnot(None),
            Job.status.in_(["pending", "running"]),
        )
        .all()
    )

    freed_count = 0
    failed_count = 0

    for job in active_jobs:
        job_worker_id = job.worker_id or ""

        belongs_to_target = (
            job_worker_id in candidates
            or any(
                job_worker_id.startswith(f"{candidate}-")
                for candidate in candidates
                if candidate
            )
        )

        if not belongs_to_target:
            continue

        if job.attempts >= job.max_retries:
            # Retries exhausted — mark as failed instead of
            # requeuing forever.
            job.status = "failed"
            job.last_error = (
                f"Worker {container.name} was removed "
                "before the job could complete"
            )
            failed_count += 1

        else:
            # Free it up so a live worker claims it next.
            job.status = "pending"
            job.worker_id = None
            job.claimed_at = None
            freed_count += 1

    if freed_count or failed_count:
        db.commit()

    # --------------------------------------------------------
    # BEST-EFFORT CLEANUP OF THE `workers` TABLE
    #
    # Not required for freeing jobs (handled above), just keeps
    # that table from accumulating stale rows.
    # --------------------------------------------------------

    try:
        db.query(Worker).filter(
            (Worker.worker_id.in_(candidates))
            | (Worker.worker_id.like(f"{container.short_id}-%"))
            | (Worker.worker_id.like(f"{container.id}-%"))
        ).delete(synchronize_session=False)
        db.commit()
    except Exception:
        db.rollback()

    return freed_count, failed_count


# ============================================================
# WORKER STATUS
# ============================================================

@app.get("/workers")
def get_workers():
    """
    Return all currently running scheduler workers.
    """

    client = get_docker_client()

    try:
        containers = get_worker_containers(client)

        workers = []

        for container in containers:

            try:
                container.reload()

                # Only report active containers.
                if container.status != "running":
                    continue

                workers.append(
                    {
                        "id": container.short_id,
                        "name": container.name,
                        "status": container.status,
                    }
                )

            except DockerException:
                continue

        workers.sort(
            key=lambda worker: worker["name"]
        )

        return {
            "count": len(workers),
            "workers": workers,
        }

    finally:
        client.close()


# ============================================================
# DELETE SINGLE WORKER
# ============================================================

@app.delete(
    "/workers/{worker_id}",
    dependencies=[Depends(verify_api_key)],
)
def delete_worker(
    worker_id: str,
    db: Session = Depends(get_db),
):
    """
    Stop and remove one Docker worker.

    Any job that was pending or running on this worker is
    freed up first, so the remaining live workers pick it up
    instead of it getting stuck forever.

    worker_id can be:
    - container ID
    - short container ID
    - container name
    """

    client = get_docker_client()

    try:
        containers = get_worker_containers(client)

        target = None

        for container in containers:
            if (
                container.id == worker_id
                or container.short_id == worker_id
                or container.name == worker_id
            ):
                target = container
                break

        if target is None:
            raise HTTPException(
                status_code=404,
                detail="Worker not found",
            )

        # Keep at least one worker alive.
        if len(containers) <= 1:
            raise HTTPException(
                status_code=400,
                detail="Cannot delete the last worker",
            )

        worker_name = target.name
        worker_short_id = target.short_id

        # --------------------------------------------------------
        # STOP THE CONTAINER FIRST
        #
        # This must happen BEFORE freeing jobs. If we free jobs
        # first and stop the container after, there's a race
        # window where the worker inside the container is still
        # alive and can claim a brand-new job in between — that
        # job would then get orphaned anyway once the container
        # is killed. Stopping first guarantees no new job can be
        # claimed by this worker after this point.
        # --------------------------------------------------------

        try:
            target.reload()
        except Exception:
            pass

        if target.status == "running":
            target.stop(timeout=10)

        # --------------------------------------------------------
        # NOW FREE UP ORPHANED JOBS
        #
        # Safe to query/free now — the worker process is stopped
        # and cannot claim any further jobs.
        # --------------------------------------------------------

        freed_count, failed_count = free_orphaned_jobs_for_container(
            db, target
        )

        # --------------------------------------------------------
        # REMOVE THE CONTAINER
        # --------------------------------------------------------

        target.remove(force=True)

        remaining = get_worker_containers(client)

        return {
            "success": True,
            "message": f"Worker {worker_name} deleted successfully",
            "deleted_worker": {
                "id": worker_short_id,
                "name": worker_name,
            },
            "jobs_requeued": freed_count,
            "jobs_failed": failed_count,
            "current_count": len(remaining),
            "workers": [
                {
                    "id": container.short_id,
                    "name": container.name,
                    "status": container.status,
                }
                for container in remaining
            ],
        }

    except HTTPException:
        raise

    except Exception as exc:
        db.rollback()

        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete worker: {exc}",
        )

    finally:
        client.close()


# ============================================================
# WORKER SCALING
# ============================================================

@app.post(
    "/workers/scale",
    dependencies=[Depends(verify_api_key)],
)
def scale_workers(
    request: WorkerScaleRequest,
    db: Session = Depends(get_db),
):
    """
    Scale the Docker worker pool.

    Example:

        workers = 1
        workers = 2
        workers = 4
        workers = 8
        workers = 16
        workers = 32
    """

    desired_workers = request.workers

    # Prevent two simultaneous scaling operations.
    with worker_scale_lock:

        client = get_docker_client()

        try:
            # ------------------------------------------------
            # CURRENT WORKERS
            # ------------------------------------------------

            containers = get_worker_containers(client)

            current_workers = len(containers)

            # ------------------------------------------------
            # NO CHANGE REQUIRED
            # ------------------------------------------------

            if current_workers == desired_workers:

                return {
                    "success": True,
                    "message": (
                        "Worker count already matches"
                    ),
                    "previous_count": current_workers,
                    "requested_count": desired_workers,
                    "current_count": current_workers,
                }

            # ------------------------------------------------
            # DATABASE URL
            # ------------------------------------------------

            database_url = get_database_url()

            # =================================================
            # SCALE UP
            # =================================================

            if desired_workers > current_workers:

                workers_to_create = (
                    desired_workers - current_workers
                )

                created_workers = []

                for _ in range(workers_to_create):

                    container = create_worker_container(
                        client,
                        database_url,
                    )

                    created_workers.append(
                        {
                            "id": container.short_id,
                            "name": container.name,
                            "status": container.status,
                        }
                    )

                # Get final worker list.
                final_containers = (
                    get_worker_containers(client)
                )

                return {
                    "success": True,
                    "message": (
                        "Workers scaled up successfully"
                    ),
                    "previous_count": current_workers,
                    "requested_count": desired_workers,
                    "current_count": len(final_containers),
                    "created": workers_to_create,
                    "workers": [
                        {
                            "id": container.short_id,
                            "name": container.name,
                            "status": container.status,
                        }
                        for container in final_containers
                    ],
                }

            # =================================================
            # SCALE DOWN
            # =================================================

            workers_to_remove = (
                current_workers - desired_workers
            )

            # ------------------------------------------------
            # Separate dynamic workers from Compose worker.
            #
            # Dynamic workers are removed first so the original
            # Compose worker normally remains alive.
            # ------------------------------------------------

            dynamic_workers = []
            compose_workers = []

            for container in containers:

                try:
                    labels = container.labels

                    if (
                        labels.get("scheduler.managed")
                        == "true"
                    ):
                        dynamic_workers.append(container)

                    else:
                        compose_workers.append(container)

                except Exception:
                    compose_workers.append(container)

            # Newest dynamically created workers are removed
            # first.
            dynamic_workers.sort(
                key=lambda container: (
                    container.attrs.get("Created", "")
                ),
                reverse=True,
            )

            # Compose workers are fallback removal candidates.
            removal_candidates = (
                dynamic_workers + compose_workers
            )

            removed_workers = 0
            total_jobs_requeued = 0
            total_jobs_failed = 0

            for container in removal_candidates:

                if removed_workers >= workers_to_remove:
                    break

                # ------------------------------------------
                # STOP THE CONTAINER FIRST
                #
                # Must happen before freeing jobs — otherwise
                # the worker inside could claim a brand-new job
                # in the gap between the free-jobs query and the
                # container actually being stopped, orphaning
                # that job anyway.
                # ------------------------------------------

                try:
                    container.reload()
                except Exception:
                    pass

                try:
                    if container.status == "running":
                        container.stop(timeout=10)
                except Exception:
                    pass

                # ------------------------------------------
                # NOW FREE UP ORPHANED JOBS
                #
                # Safe now — the worker process is stopped and
                # cannot claim any further jobs.
                # ------------------------------------------

                try:
                    freed, failed = free_orphaned_jobs_for_container(
                        db, container
                    )
                    total_jobs_requeued += freed
                    total_jobs_failed += failed
                except Exception:
                    db.rollback()

                # ------------------------------------------
                # REMOVE THE CONTAINER
                # ------------------------------------------

                try:
                    container.remove(force=True)
                    removed_workers += 1
                except Exception:
                    pass

            # ------------------------------------------------
            # FINAL COUNT
            # ------------------------------------------------

            final_containers = (
                get_worker_containers(client)
            )

            return {
                "success": True,
                "message": (
                    "Workers scaled down successfully"
                ),
                "previous_count": current_workers,
                "requested_count": desired_workers,
                "current_count": len(final_containers),
                "removed": removed_workers,
                "jobs_requeued": total_jobs_requeued,
                "jobs_failed": total_jobs_failed,
                "workers": [
                    {
                        "id": container.short_id,
                        "name": container.name,
                        "status": container.status,
                    }
                    for container in final_containers
                ],
            }

        finally:
            client.close()


# ============================================================
# PUBLIC ROUTES
# ============================================================

@app.get("/")
def dashboard():
    """
    Serve the dashboard.
    """

    frontend_path = (
        Path(__file__).resolve().parent.parent
        / "frontend"
        / "index.html"
    )

    if not frontend_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Frontend index.html not found",
        )

    return FileResponse(
        frontend_path,
        media_type="text/html",
    )


@app.get("/health")
def health_check(
    db: Session = Depends(get_db),
):
    """
    Check API and database health.
    """

    db.execute(
        text("SELECT 1")
    )

    return {
        "status": "healthy",
        "database": "connected",
    }


# ============================================================
# JOB ROUTES
# ============================================================

@app.get("/jobs")
def list_jobs(
    db: Session = Depends(get_db),
):
    """
    Return all jobs.
    """

    jobs = (
        db.query(Job)
        .order_by(Job.id.desc())
        .all()
    )

    return [
        {
            "id": job.id,
            "name": job.name,
            "command": job.command,
            "status": job.status,
            "attempts": job.attempts,
            "max_retries": job.max_retries,
            "priority": job.priority,
            "next_run_at": job.next_run_at,
            "worker_id": job.worker_id,
            "claimed_at": job.claimed_at,
            "last_error": job.last_error,
            "created_at": job.created_at,
        }
        for job in jobs
    ]




# ============================================================
# CLEAR ALL JOBS
# ============================================================

@app.delete(
    "/jobs/all",
    dependencies=[Depends(verify_api_key)],
)
def clear_all_jobs(
    db: Session = Depends(get_db),
):
    try:
        result = db.execute(
            text("DELETE FROM jobs")
        )

        deleted_count = result.rowcount or 0

        db.commit()

        return {
            "success": True,
            "message": f"Deleted {deleted_count} jobs",
            "deleted_count": deleted_count,
        }

    except Exception as exc:
        db.rollback()

        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear jobs: {exc}",
        )




@app.get("/jobs/{job_id}")
def get_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """
    Return one job.
    """

    job = (
        db.query(Job)
        .filter(Job.id == job_id)
        .first()
    )

    if job is None:
        raise HTTPException(
            status_code=404,
            detail="Job not found",
        )

    return {
        "id": job.id,
        "name": job.name,
        "command": job.command,
        "status": job.status,
        "attempts": job.attempts,
        "max_retries": job.max_retries,
        "priority": job.priority,
        "next_run_at": job.next_run_at,
        "worker_id": job.worker_id,
        "claimed_at": job.claimed_at,
        "last_error": job.last_error,
        "created_at": job.created_at,
    }


# ============================================================
# CREATE JOB
# ============================================================

@app.post(
    "/jobs",
    status_code=201,
    dependencies=[Depends(verify_api_key)],
)
def create_job(
    job_data: JobCreate,
    db: Session = Depends(get_db),
):
    """
    Create a new job.
    """

    job = Job(
        name=job_data.name,
        command=job_data.command,
        status="pending",
        attempts=0,
        max_retries=job_data.max_retries,
        priority=job_data.priority,
        next_run_at=None,
        last_error=None,
        created_at=datetime.utcnow(),
    )

    db.add(job)
    db.commit()
    db.refresh(job)

    return {
        "id": job.id,
        "name": job.name,
        "command": job.command,
        "status": job.status,
        "attempts": job.attempts,
        "max_retries": job.max_retries,
        "priority": job.priority,
        "next_run_at": job.next_run_at,
        "worker_id": job.worker_id,
        "claimed_at": job.claimed_at,
        "last_error": job.last_error,
        "created_at": job.created_at,
    }


# ============================================================
# DELETE JOB
# ============================================================

@app.delete(
    "/jobs/{job_id}",
    dependencies=[Depends(verify_api_key)],
)
def delete_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """
    Delete an existing job.
    """

    job = (
        db.query(Job)
        .filter(Job.id == job_id)
        .first()
    )

    if job is None:
        raise HTTPException(
            status_code=404,
            detail="Job not found",
        )

    db.delete(job)
    db.commit()

    return {
        "message": "Job deleted",
        "id": job_id,
    }