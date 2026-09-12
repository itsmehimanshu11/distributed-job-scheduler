# ⚡ Distributed Job Scheduler — Beginner's Guide

This is a simple explanation of what this project is, why it exists, and exactly how to get it running on your computer, step by step. No prior experience with Docker, databases, or backend systems is assumed.

---

## 1. What is this, in plain English?

Imagine you have a long list of tasks you need done — like sending 1,000 emails, resizing 500 images, or generating reports every night. You don't want to run them one at a time on your own computer and wait. Instead, you want a system where:

- You **submit** each task ("job") to a queue
- A pool of **workers** (helper programs) picks up jobs from that queue and runs them
- If a task fails, it **automatically retries**
- If a worker crashes halfway through a job, another worker **picks up where it left off** instead of the job being lost forever
- You can add or remove workers on demand, like adding more cashiers at a supermarket when the line gets long

That's exactly what this project does. It's a miniature, self-hosted version of tools like **Celery** or **Sidekiq** — the same kind of system that powers background tasks at real companies — built from scratch so you can see exactly how it works under the hood.

## 2. Why does this matter? (Real-world use)

This pattern — "submit work, let a pool of workers process it" — is everywhere in real software:

- **E-commerce**: sending order confirmation emails, generating invoices
- **Social media**: resizing/processing uploaded photos and videos in the background
- **Data teams**: nightly batch jobs, ETL pipelines
- **AI/ML**: queuing training runs across a fleet of machines

Any time an app does something too slow to make you wait for it, there's a system like this working behind the scenes.

## 3. How it's built (the pieces)

| Piece | What it does | Analogy |
|---|---|---|
| **PostgreSQL (the database)** | Stores every job and its status (pending, running, completed, failed) | A shared to-do list on a whiteboard everyone can see |
| **API (FastAPI, Python)** | The front desk — lets you submit jobs and check their status over the web | The receptionist who writes new tasks on the whiteboard |
| **Worker(s)** | Programs that continuously check the whiteboard, grab a task, do it, and report back | The staff actually doing the work |
| **Dashboard (web page)** | A visual way to submit jobs and watch them happen, instead of typing commands | A TV screen showing the whiteboard live |
| **Docker** | Packages everything (API, workers, database) into portable containers so it runs the same on any computer | Shipping containers — the contents don't change no matter which ship (computer) carries them |

**How a job flows through the system:**
1. You submit a job through the dashboard or API → it's saved in the database as `pending`
2. A worker notices it, "claims" it (marks it `running` so no other worker also grabs it), and executes it
3. If it succeeds → marked `completed`. If it fails → automatically retried a few times before being marked `failed`
4. If the worker dies mid-job, the system notices (via a heartbeat, like a pulse check) and returns the job to `pending` so another worker can finish it

## 4. What you need before starting

You only need **one** thing installed:

- **Docker Desktop** — download it free from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)

That's it. Docker Desktop includes everything else needed (Python, the database, etc.) already packaged inside the project — you don't need to install Python or PostgreSQL separately for the easy path below.

## 5. Running it from scratch — step by step

### Step 1: Install and open Docker Desktop

Download and install Docker Desktop from the link above. Once installed, **open it** and wait until it says "Docker Desktop is running" (look for a whale icon in your system tray, bottom-right of your screen on Windows).

> ⚠️ **This is the single most common thing people forget.** If Docker Desktop isn't open and running, every command below will fail with an error like "cannot connect to the Docker daemon."

### Step 2: Get the project files

If you downloaded this as a ZIP, extract it somewhere simple, like `C:\Projects\dist-job-scheduler` (avoid deeply nested folders like Desktop\OneDrive, which can sometimes cause path issues).

Open a terminal (PowerShell on Windows, Terminal on Mac) and navigate into the folder:

```powershell
cd path\to\dist-job-scheduler
```

### Step 3: Create your configuration file

Every project like this needs a small file of settings (database password, secret key, etc.) called `.env`. A template is already provided. Copy it:

**Windows (PowerShell):**
```powershell
copy .env.example .env
```

**Mac/Linux:**
```bash
cp .env.example .env
```

Now open the new `.env` file in any text editor. You only need to change **one** line — set your own secret API key:

```
API_KEY=choose-any-secret-word-you-like
```

Leave everything else as-is — the database settings are already configured to match what Docker will set up automatically.

> ⚠️ **Important:** the web dashboard (the visual page in your browser) has its own hardcoded copy of the API key baked into `frontend/app.js` (line 2), separate from `.env`. If you want the dashboard's "Create job" button to work, either leave `API_KEY` in `.env` unset and instead copy the value **from** `app.js` into `.env`, or open `app.js` and change its hardcoded key to match yours. This is a known quirk of the original dashboard design — the API itself always respects `.env`; only the dashboard needs this extra step.

### Step 4: Start everything with one command

```powershell
docker compose up -d --build
```

This single command:
- Builds the API and worker programs
- Starts the database
- Starts the API server
- Starts one worker

The first time you run it, it may take 1-2 minutes (it's downloading and building things). After that, check everything started correctly:

```powershell
docker compose ps
```

You should see three rows — `api`, `db`, and `worker` — all showing `Up` (or `healthy` for the database).

### Step 5: Open the dashboard

Open your web browser and go to:

```
http://localhost:8000
```

You should see the Distributed Job Scheduler dashboard, showing "Connected" and a worker marked active. Try creating a job using one of the built-in presets (e.g. "Echo") and watch it move from Pending → Running → Completed.

### Step 6: Explore the technical API (optional)

FastAPI automatically builds interactive documentation. Visit:

```
http://localhost:8000/docs
```

Here you can try every API endpoint directly from your browser.

### Stopping everything

```powershell
docker compose down
```

Your data is kept safe in a Docker "volume" — running `docker compose up -d` again later picks up right where you left off. If you ever want to wipe everything and start completely fresh:

```powershell
docker compose down -v
```

## 6. How to verify everything actually works

Once it's running, here's a checklist to confirm each feature works:

1. **Create a job** on the dashboard → it should move to "Completed" within a couple of seconds
2. **Scale workers** using the +/− buttons under "Cluster capacity" → a new worker container should appear
3. **Delete a worker while it's running a job** → the job should not disappear; it gets picked up by a remaining worker instead (this is the core "no job left behind" feature)
4. **Visit `http://localhost:8000/healthz`** → should show `{"status":"alive"}`
5. **Visit `http://localhost:8000/metrics`** → should show a page of monitoring statistics

## 7. Common problems and fixes

| Problem | Likely cause | Fix |
|---|---|---|
| `docker compose up` fails with a "pipe" or "daemon" error | Docker Desktop isn't running | Open Docker Desktop and wait for it to fully start, then try again |
| API container shows a database error about a table "already existing" | An old project/volume with the same name already exists | Run `docker compose down -v` to wipe old data, then `docker compose up -d --build` again |
| Dashboard shows "401 Unauthorized" when creating a job | The dashboard's built-in key (in `frontend/app.js`) doesn't match your `.env` `API_KEY` | See the note in Step 3 above — make the two match |
| `password authentication failed for user "scheduler"` when running tests locally | Your `.env` database password doesn't match `docker-compose.yml`'s password | Make sure `.env`'s `DATABASE_URL` uses `scheduler_password` (the value hardcoded in `docker-compose.yml`) |
| `curl` commands don't work as expected on Windows | PowerShell's `curl` is an alias for a different tool with different syntax | Use `curl.exe` explicitly, or use PowerShell's own `Invoke-WebRequest` |
| Python commands can't find installed packages | You have multiple Python installations, and `pip install` used a different one than `python` runs | Run `where.exe python` (Windows) to see all installed copies, then call the correct one directly, e.g. `& "C:\path\to\python.exe" -m pytest` |
| Port 8000 or 5432 already in use | Another program on your computer is using that port | Stop that program, or edit `docker-compose.yml` to use a different port, e.g. `"8001:8000"` |

## 8. Running the automated tests (optional, for the curious)

This project includes automated tests that check the system works correctly, including simulating a worker crashing mid-job. To run them, you need Python installed separately (not just Docker):

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python -m alembic upgrade head
python -m pytest -v
```

You should see all tests pass (`14 passed`).

## 9. Glossary — jargon explained simply

- **API**: A way for programs (or your browser) to talk to the server using structured requests, instead of a visual interface
- **Container / Docker**: A self-contained package that includes an application and everything it needs to run, so it behaves the same on any computer
- **Database (PostgreSQL)**: A structured, permanent storage system — where all job and worker information lives
- **Worker**: A background program that does the actual work (running the job's command)
- **Migration**: A recorded, versioned change to the database's structure (e.g. "add a new column") — lets you upgrade a database safely over time instead of guessing what state it's in
- **Idempotent**: Doing something twice has the same effect as doing it once — used here so that accidentally submitting the same job twice doesn't create a duplicate
- **Rate limiting**: Capping how many requests someone can make in a given time period, to prevent overload or abuse
- **Failover**: When something breaks, work automatically shifts to a working replacement instead of being lost

---
