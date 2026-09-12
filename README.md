# ⚡ Distributed Job Scheduler

A distributed background job scheduling system with **live worker fleet control**, built with **Python, FastAPI, PostgreSQL, Docker, and a real-time web dashboard**.

Submit jobs (shell commands) through a REST API or dashboard. Jobs are stored in PostgreSQL and picked up by one or more **worker containers**, which atomically claim, execute, and report on them. Scale the number of workers up or down live — Docker containers are created or removed on demand, and any job that was mid-execution on a removed worker is automatically requeued onto a surviving worker instead of getting lost.

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Python](https://img.shields.io/badge/python-3.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.141-009688)
![Docker](https://img.shields.io/badge/docker-required-2496ED)
![Tests](https://img.shields.io/badge/tests-14%20passing-brightgreen)

---

## Table of contents

- [What this project does](#what-this-project-does)
- [Why it matters](#why-it-matters)
- [Architecture](#architecture)
- [Features](#features)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Quick start (Docker)](#quick-start-docker)
- [Manual setup (without Docker)](#manual-setup-without-docker)
- [API reference](#api-reference)
- [Environment variables](#environment-variables)
- [Database migrations](#database-migrations)
- [Running tests](#running-tests)
- [Design decisions](#design-decisions)
- [Security notes](#security-notes)
- [Roadmap](#roadmap)
- [License](#license)

---

## What this project does

Think of it as a self-hosted task queue — similar in spirit to **Celery** or **Sidekiq** — built from scratch to demonstrate how distributed job scheduling actually works under the hood:

1. A client creates a **job** — a name plus a shell command (e.g. `echo hello`, or a script) — via the REST API or dashboard.
2. The job lands in **PostgreSQL** with status `pending`.
3. One or more **worker processes**, each in its own Docker container, continuously poll for pending jobs.
4. A worker **atomically claims** a job (via `SELECT ... FOR UPDATE SKIP LOCKED`), so two workers can never run the same job at once.
5. The worker executes the command and reports the result.
6. Failed jobs are **automatically retried with exponential backoff**, up to a configurable limit.
7. If a worker dies mid-job (detected via missed heartbeats), the job is safely **returned to the queue** and picked up by a surviving worker.
8. From the dashboard, workers can be scaled up/down live, or deleted mid-job with the same safe-requeue guarantee.

## Why it matters

"Submit work, let a pool of workers process it" is one of the most common backend patterns in production software:

| Industry | Example |
|---|---|
| E-commerce | Sending order confirmation emails, generating invoices asynchronously |
| Social media | Resizing/transcoding uploaded photos and videos in the background |
| Data engineering | Nightly batch jobs, ETL pipelines (the same problem Airflow/Prefect solve at a higher level) |
| AI/ML | Queuing training or inference jobs across a worker fleet |

Building this from scratch — rather than reaching for Celery — forces engagement with the actual mechanics those tools abstract away: atomic claiming, idempotency, failure detection, retry backoff, and horizontal scaling. That's exactly the systems-design understanding backend/infra interviews probe for.

## Architecture

```
                         ┌────────────────────┐
                         │   Web Dashboard     │
                         │ (HTML/CSS/JS)       │
                         └──────────┬──────────┘
                                    │ REST + polling
                                    ▼
                         ┌────────────────────┐
                         │     FastAPI API     │
                         │  (app/main.py)      │
                         └──────────┬──────────┘
                                    │
                 ┌──────────────────┼───────────────────┐
                 ▼                  ▼                    ▼
        ┌────────────────┐ ┌───────────────┐   ┌──────────────────┐
        │   PostgreSQL    │ │ Docker Engine │   │   Job Table       │
        │  (job + worker  │ │ (via socket)  │   │  pending/running/ │
        │   state)        │ │ scale workers │   │  completed/failed │
        └───────┬─────────┘ └───────┬───────┘   └──────────────────┘
                │                   │
                │           creates/removes
                │                   ▼
                │        ┌─────────────────────┐
                └───────►│  Worker Container 1  │
                │        ├─────────────────────┤
                └───────►│  Worker Container 2  │
                │        ├─────────────────────┤
                └───────►│  Worker Container N  │
                         └─────────────────────┘
```

- The **API never executes jobs itself** — it only manages state and tells Docker to create/remove worker containers.
- **Workers are stateless and disposable.** Scaling down or deleting a worker stops and removes its container; any job actively running on it is reset to `pending` first — nothing is silently lost.
- **The database is the coordination point** between workers — an atomic "claim" operation guarantees two workers never grab the same pending job.

---

## Features

- 🖥️ **Web dashboard** — create jobs, watch the queue live, scale workers, see per-worker stats
- 📦 **Bulk job creation** — paste many jobs at once
- 🐳 **Dynamic worker scaling** — spin Docker worker containers up/down live (1–32 workers)
- 🔁 **Requeue-safe worker deletion** — deleting a worker mid-job frees that job instead of stranding it
- ⚖️ **Job priority + priority aging** — higher-priority jobs run first; long-waiting jobs get a priority boost over time, preventing starvation
- ♻️ **Automatic retries with exponential backoff**, up to `max_retries`
- 🩺 **Stale-job recovery / failover** — a dead worker's job is detected via missed heartbeats and reclaimed
- 🛑 **Graceful worker shutdown** — on `SIGTERM`/`SIGINT`, a worker finishes its current job before exiting, rather than being killed mid-execution
- 🔑 **API key authentication** on all write endpoints (constant-time comparison, avoiding timing side-channels)
- 🎯 **Idempotent job submission** — an optional `dedupe_key` (backed by a real unique DB constraint, not just an app-level check) makes retried submissions safe
- 🚦 **Rate limiting** on job-creation and worker-scaling endpoints
- 🧱 **Per-job resource limits** (CPU/memory) and a denylist for obviously destructive commands — defense in depth, not a full sandbox
- 📈 **Structured JSON logging + Prometheus metrics** (`/metrics`), with separate liveness (`/healthz`) and readiness (`/readyz`) probes
- 🗃️ **Alembic migrations** for schema changes
- 📊 **REST API** with full OpenAPI/Swagger docs at `/docs`
- ✅ **Automated tests**, including real-database integration tests for the failover and idempotency paths, with CI on every push

---

## Tech stack

| Layer | Technology |
|---|---|
| API | Python 3.12, FastAPI, Uvicorn |
| Database | PostgreSQL 16 |
| ORM | SQLAlchemy 2.0 |
| Migrations | Alembic |
| Worker orchestration | Docker SDK for Python |
| Observability | Structured JSON logging, Prometheus metrics |
| Frontend | Vanilla HTML/CSS/JavaScript (no build step) |
| Containerization | Docker, Docker Compose |
| Testing | pytest (unit + real-database integration tests) |
| CI | GitHub Actions |

---

## Project structure

```
distributed-job-scheduler/
│
├── app/
│   ├── main.py              # FastAPI app: all REST endpoints
│   ├── database.py           # DB engine/session setup
│   └── models.py             # SQLAlchemy models (Job, Worker)
│
├── alembic/
│   └── versions/              # Migration scripts
│
├── frontend/                  # Dashboard (HTML/CSS/JS, no build step)
│
├── tests/
│   ├── test_auth.py                    # API key auth (mocked)
│   ├── test_worker.py                  # Worker logic (mocked)
│   └── test_integration_failover.py    # Real-DB: failover + idempotency
│
├── .github/workflows/tests.yml   # CI: migrations + pytest vs real Postgres
├── worker.py                      # Worker process (runs per container)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## Quick start (Docker)

**Requirements:** Docker Desktop, Git.

```bash
git clone https://github.com/itsmehimanshu11/distributed-job-scheduler.git
cd distributed-job-scheduler
cp .env.example .env      # then set your own API_KEY inside
docker compose up -d --build
```

Check everything started:

```bash
docker compose ps
```

Open the dashboard at **http://localhost:8000**, or the interactive API docs at **http://localhost:8000/docs**.

> **Note:** the dashboard (`frontend/app.js`) uses its own copy of the API key for browser requests, separate from `.env`. Make sure both match — see the comment at the top of `.env.example`.

To stop:

```bash
docker compose down        # keeps data
docker compose down -v     # wipes data too
```

## Manual setup (without Docker)

```bash
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # macOS/Linux
pip install -r requirements.txt

docker compose up -d db      # just the database
python -m alembic upgrade head

python -m uvicorn app.main:app --reload     # terminal 1
python worker.py                             # terminal 2 (repeat for more workers)
```

---

## API reference

All write endpoints require an `X-API-Key` header.

### Jobs

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/jobs` | List all jobs |
| `POST` | `/jobs` | Create a job (supports `dedupe_key` for idempotency) |
| `GET` | `/jobs/{job_id}` | Get one job |
| `DELETE` | `/jobs/{job_id}` | Delete one job |
| `DELETE` | `/jobs/all` | Delete every job |

```bash
curl -X POST http://localhost:8000/jobs \
  -H "X-API-Key: your-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
        "name": "nightly-report",
        "command": "python generate_report.py",
        "priority": 100,
        "max_retries": 3,
        "dedupe_key": "nightly-report-2026-09-12"
      }'
```

### Workers

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/workers` | List running worker containers |
| `POST` | `/workers/scale` | Scale to a target worker count (1–32) |
| `DELETE` | `/workers/{worker_id}` | Remove one specific worker (requeues its job) |

### System

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/healthz` | Liveness probe (no DB dependency) |
| `GET` | `/readyz` | Readiness probe (confirms DB connectivity) |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/docs` | Interactive Swagger docs |

---

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | SQLAlchemy connection string | — |
| `API_KEY` | Secret required for write requests | — |
| `LOG_LEVEL` | Logging verbosity | `INFO` |
| `RATE_LIMIT_MAX_REQUESTS` | Requests per client IP per window | `60` |
| `RATE_LIMIT_WINDOW_SECONDS` | Rate-limit window length | `60` |
| `JOB_MAX_MEMORY_MB` | Memory cap per job process | `512` |
| `JOB_MAX_CPU_SECONDS` | CPU-time cap per job process | `280` |
| `JOB_TIMEOUT_SECONDS` | Wall-clock timeout before a job is killed | `300` |

## Database migrations

```bash
alembic upgrade head                                  # apply migrations
alembic revision --autogenerate -m "describe change"  # after a model change
```

## Running tests

```bash
python -m pytest -v
```

14 tests: mocked unit tests (`test_auth.py`, `test_worker.py`) plus real-database integration tests (`test_integration_failover.py`) covering stale-worker failover, no-double-claim guarantees, and idempotent submission under a concurrent-insert race.

---

## Design decisions

- **Atomic claiming via `SELECT ... FOR UPDATE SKIP LOCKED`** rather than an application-level lock, so the database — the single source of truth — enforces exclusivity even under concurrent workers.
- **Polling over pub/sub**: simpler to reason about and debug than `LISTEN/NOTIFY` or a message broker, at the cost of a small fixed latency. A documented, deliberate trade-off (see Roadmap for the alternative).
- **Priority aging** prevents starvation: a job's effective priority increases the longer it waits, so a constant stream of high-priority jobs can't indefinitely block low-priority ones.
- **Idempotency via a real unique constraint**, not just an app-level lookup — closes the race where two near-simultaneous requests with the same `dedupe_key` could otherwise both insert.
- **Graceful shutdown**: a worker stops claiming new jobs on `SIGTERM` but lets its current job finish, rather than killing it mid-execution and leaving external side effects half-done.

## Security notes

Being upfront about what is and isn't handled:

- **Job commands run as real shell commands.** The API key is the only real gate on `POST /jobs` — treat it like a root credential. CPU/memory limits and a small denylist exist as defense-in-depth, not a security boundary. Real isolation would mean container-per-job execution.
- **Single static API key** is fine for personal/internal use; a multi-tenant deployment would need per-client, revocable, hashed-at-rest keys.
- **No TLS termination here** — put this behind a reverse proxy in any real deployment.

## Roadmap

- Container-per-job execution (real sandboxing)
- Redis or `LISTEN/NOTIFY`-based push instead of polling
- Job scheduling (cron-style/delayed jobs), job dependencies/DAGs
- Dead-letter queue for permanently failed jobs
- Per-client API keys / OAuth
- Grafana dashboard on top of `/metrics`
- Kubernetes manifests, horizontal autoscaling on queue depth

## License

MIT
