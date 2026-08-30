# ⚡ Distributed Job Scheduler

A distributed background job scheduling system with **live worker fleet control**, built with **Python, FastAPI, PostgreSQL, Docker, and a real-time web dashboard**.

You submit jobs (shell commands) through a REST API or the dashboard. Jobs are stored in PostgreSQL and picked up by one or more **worker containers**, which claim, execute, and report on them. You can scale the number of workers up or down live from the dashboard — Docker containers are created or removed on demand, and any job that was mid-execution on a removed worker is automatically requeued onto a surviving worker instead of getting lost.

![Status](https://img.shields.io/badge/status-active-brightgreen)
![Python](https://img.shields.io/badge/python-3.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.141-009688)
![Docker](https://img.shields.io/badge/docker-required-2496ED)

---

## Table of contents

- [What this project does](#what-this-project-does)
- [Screenshot](#screenshot)
- [Architecture](#architecture)
- [Features](#features)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Requirements](#requirements)
- [Quick start (Docker — recommended)](#quick-start-docker--recommended)
- [Manual setup (without Docker)](#manual-setup-without-docker)
- [Using the dashboard](#using-the-dashboard)
- [API reference](#api-reference)
- [Environment variables](#environment-variables)
- [Running tests](#running-tests)
- [Continuous integration](#continuous-integration)
- [Troubleshooting](#troubleshooting)
- [Design notes](#design-notes)
- [Roadmap](#roadmap)

---

## What this project does

Think of it as a tiny, self-hosted version of a task queue like Celery or Sidekiq, but built from scratch to understand how distributed job scheduling actually works under the hood:

1. You create a **job** — a name plus a shell command to run (e.g. `echo hello`, or a Python script).
2. The job is stored in **PostgreSQL** with status `pending`.
3. One or more **worker processes** (each running in its own Docker container) continuously poll the database for pending jobs.
4. A worker **atomically claims** a job (so two workers never run the same job at once), executes the command, and writes the result back.
5. Failed jobs are automatically **retried with backoff**, up to a configurable limit.
6. From the **dashboard**, you can scale the number of workers up or down at any time. If a worker is removed while it's actively running a job, that job is safely **freed and picked up by a remaining worker** instead of being lost.

---

## Screenshot

*(Add a screenshot of your dashboard here once deployed — e.g. drag an image into this section on GitHub, or reference `docs/dashboard.png`.)*

```
![Dashboard screenshot](docs/dashboard.png)
```

---

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
- **Workers are disposable.** Scaling down (or deleting a worker from the dashboard) stops and removes its container. Any job that was actively running on it is reset to `pending` so another worker can finish it — nothing gets silently lost.
- The **database is the coordination point** between workers, using an atomic "claim" operation so two workers can never grab the same pending job.

---

## Features

- 🖥️ **Web dashboard** — create jobs, watch the queue live, scale workers, see per-worker stats, all without touching the terminal
- 📦 **Bulk job creation** — paste many jobs at once (`name,command,priority,max_retries`, one per line)
- 🧪 **Built-in job type presets** — echo, timed sleeps, random duration, always-fail, flaky (50%), CPU burn, large output — useful for testing the scheduler itself
- 🐳 **Dynamic worker scaling** — spin Docker worker containers up or down live from the dashboard or via API (1–32 workers)
- 🔁 **Requeue-safe worker deletion** — deleting/scaling down a worker mid-job frees that job instead of stranding it
- ⚖️ **Job priority + priority aging** — higher-priority jobs run first; long-waiting jobs get a priority boost over time
- ♻️ **Automatic retries with backoff** — failed jobs retry with increasing delay, up to `max_retries`
- 🩺 **Stale job recovery / failover** — if a worker dies mid-job without cleanup, another worker can pick the job back up
- 🔑 **API key authentication** on all write endpoints
- 📊 **REST API** with full OpenAPI/Swagger docs at `/docs`
- ✅ **Automated tests** (pytest) with CI running on every push via GitHub Actions

---

## Tech stack

| Layer | Technology |
|---|---|
| API | Python 3.12, FastAPI, Uvicorn |
| Database | PostgreSQL 16 |
| ORM | SQLAlchemy 2.0 |
| Worker orchestration | Docker SDK for Python |
| Frontend | Vanilla HTML/CSS/JavaScript (no build step) |
| Containerization | Docker, Docker Compose |
| Testing | pytest |
| CI | GitHub Actions |

---

## Project structure

```
Distributed Job Scheduler/
│
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app: all REST endpoints
│   ├── database.py           # DB engine/session setup
│   └── models.py             # SQLAlchemy models (Job, Worker)
│
├── frontend/
│   ├── index.html            # Dashboard markup
│   ├── style.css             # Dashboard styling
│   └── app.js                # Dashboard logic (polling, forms, tables)
│
├── tests/
│   ├── test_auth.py          # API key auth tests
│   └── test_worker.py        # Worker logic tests
│
├── .github/workflows/
│   └── tests.yml             # CI: runs pytest against a real Postgres service
│
├── worker.py                  # The worker process (runs inside each container)
├── migrate_worker_schema.py   # One-off DB migration helper for the workers table
├── docker-compose.yml         # Defines api, db, and worker services
├── Dockerfile                 # Shared image for both api and worker
├── requirements.txt
├── .env.example                # Template for your local .env
└── README.md
```

---

## Requirements

You only need **one** of the two setups below.

### Docker setup (recommended — easiest)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Git

### Manual setup (no Docker)
- Python 3.12+
- PostgreSQL running locally (or via Docker just for the database)
- Git

---

## Quick start (Docker — recommended)

This is the easiest way to run the whole system — API, database, and one worker — with a single command. **Beginner-friendly, step by step:**

### Step 1 — Install Docker Desktop

Download and install it from [docker.com](https://www.docker.com/products/docker-desktop/), then **open Docker Desktop and make sure it's running** (you'll see a whale icon in your system tray/menu bar).

### Step 2 — Clone the project

```powershell
git clone https://github.com/itsmehimanshu11/distributed-job-scheduler.git
cd distributed-job-scheduler
```

### Step 3 — Create your environment file

Copy the example file and fill in your own values:

```powershell
copy .env.example .env
```

*(On macOS/Linux use `cp .env.example .env` instead.)*

Open the new `.env` file in any text editor and set:

```env
API_KEY=choose-any-secret-string-you-like
```

You can leave the database values as-is — Docker Compose uses them automatically.

### Step 4 — Start everything

```powershell
docker compose up -d --build
```

This single command:
- Builds the API and worker Docker images
- Starts PostgreSQL
- Starts the API server
- Starts one worker

Wait about 10–15 seconds for everything to boot, then check that all three containers are running:

```powershell
docker compose ps
```

You should see `api`, `db`, and `worker`, all with status `Up` (or `healthy` for the database).

### Step 5 — Open the dashboard

Open your browser and go to:

```
http://localhost:8000
```

You now have a live dashboard where you can create jobs, scale workers, and watch everything happen in real time.

### Step 6 — (Optional) Explore the raw API

FastAPI auto-generates interactive API docs. Open:

```
http://localhost:8000/docs
```

Here you can test every endpoint directly in the browser.

### Stopping the project

```powershell
docker compose down
```

Your data stays saved (in a Docker volume) — running `docker compose up -d` again picks up right where you left off. To wipe the database completely, add `-v`:

```powershell
docker compose down -v
```

---

## Manual setup (without Docker)

Use this if you want to run the API and worker directly with Python (useful for development/debugging), while still using Docker just for the database.

### 1. Clone the project

```powershell
git clone https://github.com/itsmehimanshu11/distributed-job-scheduler.git
cd distributed-job-scheduler
```

### 2. Create and activate a virtual environment

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

If PowerShell blocks the activation script:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\venv\Scripts\Activate.ps1
```

You should see `(venv)` at the start of your terminal prompt.

### 3. Install dependencies

```powershell
pip install -r requirements.txt
```

### 4. Set up your `.env` file

```powershell
copy .env.example .env
```

Edit `.env` and set an `API_KEY` of your choice.

### 5. Start just the database with Docker

```powershell
docker compose up -d db
```

### 6. Start the API

```powershell
python -m uvicorn app.main:app --reload
```

The API is now running at `http://127.0.0.1:8000`.

### 7. Start a worker (in a new terminal)

```powershell
.\venv\Scripts\Activate.ps1
python worker.py
```

You should see:

```
[WORKER] Registered worker: YOUR-MACHINE-1234
[WORKER] Worker started
[WORKER] Waiting for pending jobs...
```

You can repeat step 7 in additional terminals to run more workers manually — each one registers with a unique ID and independently claims jobs.

### 8. Open the dashboard

```
http://127.0.0.1:8000
```

---

## Using the dashboard

Once the dashboard is open at `http://localhost:8000`:

- **Dispatch a job** — fill in a name, pick a job type (or write a custom command), and click **Create jobs**. Use **Bulk** mode to create several different jobs at once by pasting a list.
- **Cluster capacity** — use `+`/`−` or a preset button, then click **Apply** to scale the number of live worker containers.
- **Worker fleet** — see every live worker, how many jobs it's completed, and its current status. Select one or more and click **Delete selected** to remove them (their in-flight jobs are automatically handed off to a remaining worker).
- **Job queue** — search, filter by status, and watch job status update live every couple of seconds.

---

## API reference

All write endpoints (creating/deleting jobs or workers) require an `X-API-Key` header matching your `.env` value.

### Jobs

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/jobs` | List all jobs |
| `POST` | `/jobs` | Create a job |
| `GET` | `/jobs/{job_id}` | Get one job |
| `DELETE` | `/jobs/{job_id}` | Delete one job |
| `DELETE` | `/jobs/all` | Delete every job |

**Create a job:**

```bash
curl -X POST http://localhost:8000/jobs \
  -H "X-API-Key: your-secret-key" \
  -H "Content-Type: application/json" \
  -d '{
        "name": "hello-world",
        "command": "echo Hello from the scheduler!",
        "priority": 100,
        "max_retries": 3
      }'
```

### Workers

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/workers` | List currently running worker containers |
| `POST` | `/workers/scale` | Scale to a target worker count (1–32) |
| `DELETE` | `/workers/{worker_id}` | Remove one specific worker |

**Scale to 4 workers:**

```bash
curl -X POST http://localhost:8000/workers/scale \
  -H "X-API-Key: your-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"workers": 4}'
```

### System

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Dashboard (HTML) |
| `GET` | `/health` | Health check |
| `GET` | `/docs` | Interactive Swagger API docs |

---

## Environment variables

Copy `.env.example` to `.env` and configure:

| Variable | Description | Example |
|---|---|---|
| `POSTGRES_USER` | Database username | `scheduler` |
| `POSTGRES_PASSWORD` | Database password | *(choose your own)* |
| `POSTGRES_DB` | Database name | `scheduler_db` |
| `POSTGRES_HOST` | Database host (Docker Compose sets this automatically for containers) | `localhost` |
| `POSTGRES_PORT` | Database port | `5432` |
| `DATABASE_URL` | Full SQLAlchemy connection string | `postgresql+psycopg://scheduler:...@localhost:5432/scheduler_db` |
| `API_KEY` | Secret key required for all write requests | *(choose your own secret)* |

**Never commit your real `.env` file.** It's already excluded via `.gitignore` — only `.env.example` (with placeholder values) should ever be committed.

---

## Running tests

```powershell
python -m pytest -q
```

Tests require a reachable PostgreSQL database (matching your `DATABASE_URL`), since the app connects to it on import. If you're running tests locally, make sure `docker compose up -d db` is running first.

---

## Continuous integration

Every push to `main` automatically runs the test suite via **GitHub Actions** (`.github/workflows/tests.yml`). The workflow spins up a temporary PostgreSQL service container so tests run against a real database, exactly like production — no manual setup needed on GitHub's side. Check the **Actions** tab on the repository to see build status.

---

## Troubleshooting

**"Docker Engine unavailable" error from the API**
Make sure Docker Desktop is actually running before starting the API — worker scaling talks to Docker directly through its socket.

**Dashboard shows "API Offline"**
Check `docker compose ps` — if the `api` container isn't `Up`, check its logs: `docker compose logs api`.

**Changes to `frontend/` files don't show up in the browser**
The frontend is baked into the Docker image at build time. After editing `index.html`/`style.css`/`app.js`, rebuild:
```powershell
docker compose up -d --build
```
Then hard-refresh your browser (`Ctrl+Shift+R`).

**Port 8000 or 5432 already in use**
Something else on your machine is using that port. Either stop it, or change the port mapping in `docker-compose.yml` (e.g. `"8001:8000"`).

**"Cannot delete the last worker" error**
By design — the system always keeps at least one worker alive so jobs don't get permanently stranded. Scale up first if you need to replace the last remaining worker.

---

## Design notes

- **Atomic job claiming** prevents two workers from ever executing the same pending job — the database transaction guarantees exclusivity.
- **Requeue-on-delete**: when a worker is removed (via scale-down or explicit delete), the API first stops the container, *then* checks for any job that was actively running on it and resets that job to `pending` before finally removing the container. Stopping before requeuing closes a race condition where the worker could otherwise grab a brand-new job in the gap.
- **Priority aging** slowly increases the effective priority of jobs that have been waiting a long time, so low-priority jobs don't starve indefinitely behind a constant stream of high-priority ones.
- **Workers are stateless and disposable** — any worker can be killed and replaced at any time without losing job data, since all state lives in PostgreSQL, not in the worker process.

---

## Roadmap

Ideas for future improvement (not yet implemented):

- Redis-based queueing as an alternative backend
- WebSocket-based live updates (replacing polling)
- Job scheduling (cron-style / delayed jobs)
- Job dependencies / DAGs
- Dead-letter queue for permanently failed jobs
- Prometheus metrics + Grafana dashboard
- Kubernetes deployment manifests
- Horizontal autoscaling based on queue depth

---

## License

*(Add your license here — e.g. MIT, or "All rights reserved" if private.)*

## Author

Built by [Himanshu](https://github.com/itsmehimanshu11) as a hands-on project exploring distributed systems, background job queues, and container orchestration.