import logging
import os
import secrets
import sys
import time
import uuid
import docker

from docker.errors import DockerException
from threading import Lock
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, Header, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware
from pythonjsonlogger import jsonlogger
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CONTENT_TYPE_LATEST,
    generate_latest,
)

from .database import engine, get_db, Base
from .models import Job, Worker


# ============================================================
# STRUCTURED (JSON) LOGGING
# ============================================================
#
# Plain `print()` statements are fine for a laptop demo but are
# painful to search/alert on in any real deployment. Emitting JSON
# lines lets this be shipped straight into something like
# CloudWatch, Loki, or the ELK stack without a custom parser.

logger = logging.getLogger("scheduler.api")
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

_log_handler = logging.StreamHandler(sys.stdout)
_log_handler.setFormatter(
    jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s"
    )
)
logger.handlers = [_log_handler]
logger.propagate = False


# ============================================================
# PROMETHEUS METRICS
# ============================================================

JOBS_CREATED_TOTAL = Counter(
    "scheduler_jobs_created_total",
    "Number of jobs submitted through the API",
)

JOBS_DELETED_TOTAL = Counter(
    "scheduler_jobs_deleted_total",
    "Number of jobs deleted through the API",
)

WORKER_SCALE_REQUESTS_TOTAL = Counter(
    "scheduler_worker_scale_requests_total",
    "Number of worker scale requests",
    ["direction"],
)

HTTP_REQUESTS_TOTAL = Counter(
    "scheduler_http_requests_total",
    "Total HTTP requests handled by the API",
    ["method", "path", "status_code"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "scheduler_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "path"],
)

JOB_QUEUE_DEPTH = Gauge(
    "scheduler_job_queue_depth",
    "Number of jobs currently in each status",
    ["status"],
)


# ============================================================
# LIGHTWEIGHT RATE LIMITING
# ============================================================
#
# In-memory sliding-window limiter for write endpoints. This is
# per-process (not shared across API replicas) -- fine for a
# single-instance deployment; swap for a Redis-backed limiter
# (e.g. `slowapi` + Redis) if you run the API horizontally.

RATE_LIMIT_MAX_REQUESTS = int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "60"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))

_rate_limit_lock = Lock()
_rate_limit_hits: dict[str, deque] = defaultdict(deque)


def rate_limit(request: Request):
    """
    Simple sliding-window rate limiter keyed by client IP.
    Raises 429 if the caller exceeds RATE_LIMIT_MAX_REQUESTS
    within RATE_LIMIT_WINDOW_SECONDS.
    """

    client_key = request.client.host if request.client else "unknown"
    now = time.monotonic()

    with _rate_limit_lock:
        hits = _rate_limit_hits[client_key]

        while hits and now - hits[0] > RATE_LIMIT_WINDOW_SECONDS:
            hits.popleft()

        if len(hits) >= RATE_LIMIT_MAX_REQUESTS:
            raise HTTPException(
                status_code=429,
                detail=(
                    "Rate limit exceeded: "
                    f"{RATE_LIMIT_MAX_REQUESTS} requests per "
                    f"{RATE_LIMIT_WINDOW_SECONDS}s"
                ),
            )

        hits.append(now)


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

# Passed through to every dynamically-created worker so scaled-up
# workers get the same sandbox configuration as the Compose-managed
# one, rather than silently falling back to unsandboxed subprocess
# execution.
WORKER_SANDBOX_MODE = os.getenv("SANDBOX_MODE", "subprocess")
WORKER_JOB_RUNNER_IMAGE = os.getenv("JOB_RUNNER_IMAGE", "python:3.12-slim")
WORKER_JOB_NETWORK_DISABLED = os.getenv("JOB_NETWORK_DISABLED", "true")

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
# REQUEST LOGGING + METRICS MIDDLEWARE
# ============================================================

@app.middleware("http")
async def observability_middleware(request: Request, call_next):
    start = time.monotonic()

    response = await call_next(request)

    duration = time.monotonic() - start
    route_path = request.scope.get("route")
    path_label = (
        route_path.path if route_path is not None else request.url.path
    )

    HTTP_REQUESTS_TOTAL.labels(
        method=request.method,
        path=path_label,
        status_code=response.status_code,
    ).inc()

    HTTP_REQUEST_DURATION_SECONDS.labels(
        method=request.method,
        path=path_label,
    ).observe(duration)

    logger.info(
        "http_request",
        extra={
            "method": request.method,
            "path": path_label,
            "status_code": response.status_code,
            "duration_ms": round(duration * 1000, 2),
            "client_ip": (
                request.client.host if request.client else None
            ),
        },
    )

    return response


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

    Uses `secrets.compare_digest` instead of `==` so the comparison
    takes constant time regardless of where the strings first differ
    -- a plain `==` short-circuits character-by-character and can
    leak timing information an attacker could use to guess the key
    byte-by-byte.
    """

    expected_api_key = os.getenv("API_KEY")

    if not expected_api_key:
        raise HTTPException(
            status_code=500,
            detail="API_KEY is not configured",
        )

    if not x_api_key or not secrets.compare_digest(
        x_api_key, expected_api_key
    ):
        logger.warning(
            "auth_failed",
            extra={"reason": "invalid_or_missing_api_key"},
        )
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

    # Optional idempotency key. If a job with this dedupe_key already
    # exists, POST /jobs returns the existing job (200) instead of
    # creating a duplicate (201). Lets clients safely retry a job
    # submission after a network timeout without double-running it.
    dedupe_key: str | None = Field(
        default=None,
        max_length=255,
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
                "SANDBOX_MODE": WORKER_SANDBOX_MODE,
                "JOB_RUNNER_IMAGE": WORKER_JOB_RUNNER_IMAGE,
                "JOB_NETWORK_DISABLED": WORKER_JOB_NETWORK_DISABLED,
            },

            volumes=(
                {
                    "/var/run/docker.sock": {
                        "bind": "/var/run/docker.sock",
                        "mode": "rw",
                    }
                }
                if WORKER_SANDBOX_MODE == "docker"
                else {}
            ),

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
    dependencies=[Depends(verify_api_key), Depends(rate_limit)],
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

    logger.info(
        "worker_scale_requested",
        extra={"desired_workers": desired_workers},
    )

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

                WORKER_SCALE_REQUESTS_TOTAL.labels(direction="up").inc()

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

            WORKER_SCALE_REQUESTS_TOTAL.labels(direction="down").inc()

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

    Kept for backwards compatibility -- prefer /healthz (liveness)
    and /readyz (readiness) for container orchestrators.
    """

    db.execute(
        text("SELECT 1")
    )

    return {
        "status": "healthy",
        "database": "connected",
    }


@app.get("/healthz")
def liveness():
    """
    Liveness probe: does NOT touch the database.

    A load balancer / orchestrator uses this to decide whether the
    process itself is alive and should keep receiving traffic. It
    should stay fast and dependency-free -- if it queried the DB, a
    slow database would make a perfectly healthy API process look
    dead and get killed for the wrong reason.
    """

    return {"status": "alive"}


@app.get("/readyz")
def readiness(
    db: Session = Depends(get_db),
):
    """
    Readiness probe: confirms the API can actually serve traffic
    (i.e. the database is reachable). Orchestrators use this to
    decide whether to route requests to this instance.
    """

    try:
        db.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Database not ready: {exc}",
        )

    return {"status": "ready", "database": "connected"}


@app.get("/metrics")
def metrics():
    """
    Prometheus scrape endpoint.
    """

    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


# ============================================================
# JOB ROUTES
# ============================================================

def serialize_job(job: Job) -> dict:
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
        "dedupe_key": job.dedupe_key,
    }


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

    status_counts = defaultdict(int)
    for job in jobs:
        status_counts[job.status] += 1

    for status_label, count in status_counts.items():
        JOB_QUEUE_DEPTH.labels(status=status_label).set(count)

    return [serialize_job(job) for job in jobs]




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

    return serialize_job(job)


# ============================================================
# CREATE JOB
# ============================================================

@app.post(
    "/jobs",
    status_code=201,
    dependencies=[Depends(verify_api_key), Depends(rate_limit)],
)
def create_job(
    job_data: JobCreate,
    response: Response,
    db: Session = Depends(get_db),
):
    """
    Create a new job.

    If `dedupe_key` is supplied and a job with that key already
    exists, the existing job is returned (status 200) instead of a
    new one being created (status 201). This makes job submission
    safe to retry -- e.g. if a client times out waiting for the
    response but the request actually succeeded, resubmitting with
    the same dedupe_key will not double-run the job.
    """

    if job_data.dedupe_key:
        existing = (
            db.query(Job)
            .filter(Job.dedupe_key == job_data.dedupe_key)
            .first()
        )

        if existing is not None:
            logger.info(
                "job_create_deduped",
                extra={
                    "job_id": existing.id,
                    "dedupe_key": job_data.dedupe_key,
                },
            )
            response.status_code = 200
            return serialize_job(existing)

    job = Job(
        name=job_data.name,
        command=job_data.command,
        status="pending",
        attempts=0,
        max_retries=job_data.max_retries,
        priority=job_data.priority,
        next_run_at=None,
        last_error=None,
        created_at=datetime.now(timezone.utc),
        dedupe_key=job_data.dedupe_key,
    )

    db.add(job)

    try:
        db.commit()
    except IntegrityError:
        # Race: another request created a job with the same
        # dedupe_key between our lookup above and this commit.
        # Return that job instead of erroring out.
        db.rollback()

        existing = (
            db.query(Job)
            .filter(Job.dedupe_key == job_data.dedupe_key)
            .first()
        )

        if existing is not None:
            response.status_code = 200
            return serialize_job(existing)

        raise HTTPException(
            status_code=409,
            detail="Job with this dedupe_key already exists",
        )

    db.refresh(job)

    JOBS_CREATED_TOTAL.inc()

    logger.info(
        "job_created",
        extra={
            "job_id": job.id,
            "job_name": job.name,
            "priority": job.priority,
            "dedupe_key": job.dedupe_key,
        },
    )

    return serialize_job(job)


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

    JOBS_DELETED_TOTAL.inc()

    logger.info("job_deleted", extra={"job_id": job_id})

    return {
        "message": "Job deleted",
        "id": job_id,
    }
