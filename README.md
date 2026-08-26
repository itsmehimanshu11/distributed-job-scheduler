# Distributed Job Scheduler

A distributed background job scheduling system built with **Python, FastAPI, PostgreSQL, Docker, and multiple worker processes**.

The system allows clients to create background jobs through a REST API. Jobs are stored in PostgreSQL and are independently claimed and executed by workers. Multiple workers can run simultaneously, allowing jobs to be distributed across workers.

---

## Features

- REST API using FastAPI
- PostgreSQL-backed persistent job storage
- Distributed job execution
- Multiple workers
- Atomic job claiming
- Job priorities
- Priority aging
- Automatic retries
- Exponential retry delays
- Maximum retry limits
- Failed-job tracking
- Worker identification
- Stale-job recovery / failover
- Job status tracking
- Job creation and deletion through API
- Swagger/OpenAPI documentation
- Docker-based PostgreSQL environment
- Worker load distribution
- Persistent job state in PostgreSQL

---

# Architecture

```text
                    Client
                      |
                      v
              +---------------+
              |   FastAPI API |
              +-------+-------+
                      |
                      v
              +---------------+
              |  PostgreSQL   |
              |   Job Queue   |
              +-------+-------+
                      |
          +-----------+-----------+
          |                       |
          v                       v
   +-------------+         +-------------+
   |   Worker 1  |         |   Worker 2  |
   +-------------+         +-------------+
          |                       |
          +-----------+-----------+
                      |
                      v
                Job Execution
```

The API does not execute jobs itself.

Instead:

1. The API creates a job.
2. PostgreSQL stores the job.
3. Workers continuously poll for pending jobs.
4. A worker atomically claims a job.
5. The worker executes the command.
6. The worker updates the job status.
7. Failed jobs can be retried.
8. Stale jobs can be recovered by another worker.

---

# Technology Stack

| Technology | Purpose |
|---|---|
| Python | Core application and workers |
| FastAPI | REST API |
| Uvicorn | ASGI server |
| PostgreSQL | Persistent job database |
| Docker | Database/container environment |
| SQL | Job storage and atomic scheduling |
| Swagger/OpenAPI | API testing and documentation |
| PowerShell | Windows development commands |

---

# Project Structure

```text
Distributed Job Scheduler/
│
├── app/
│   ├── __init__.py
│   ├── database.py
│   ├── main.py
│   └── models.py
│
├── worker.py
├── migrate_worker.py
├── docker-compose.yml
├── README.md
├── .gitignore
│
└── venv/
```

### Important files

### `app/main.py`

Contains the FastAPI application and REST API endpoints.

### `app/database.py`

Handles PostgreSQL database connectivity.

### `app/models.py`

Contains the database/job models and related structures.

### `worker.py`

Contains the worker process responsible for:

- registering the worker
- polling for jobs
- claiming jobs
- executing commands
- handling failures
- retrying failed jobs
- recovering stale jobs
- updating job status

### `migrate_worker.py`

Used for worker-related database migration/setup.

### `docker-compose.yml`

Used to start PostgreSQL through Docker.

---

# Requirements

Install the following:

- Python 3.12+
- Docker Desktop
- Git
- PowerShell on Windows

---

# 1. Clone the Project

```powershell
git clone <YOUR-GITHUB-REPOSITORY-URL>
cd "Distributed Job Scheduler"
```

If you already have the project:

```powershell
cd "Distributed Job Scheduler"
```

---

# 2. Create Virtual Environment

```powershell
python -m venv venv
```

Activate it:

```powershell
.\venv\Scripts\Activate.ps1
```

If PowerShell blocks activation:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\venv\Scripts\Activate.ps1
```

You should then see:

```text
(venv) PS C:\...\Distributed Job Scheduler>
```

---

# 3. Start PostgreSQL

Make sure Docker Desktop is running.

Then execute:

```powershell
docker compose up -d
```

Check containers:

```powershell
docker compose ps
```

The PostgreSQL container should be running.

---

# 4. Start the FastAPI Server

In a terminal with the virtual environment activated:

```powershell
python -m uvicorn app.main:app --reload
```

The API will run at:

```text
http://127.0.0.1:8000
```

---

# 5. Open Swagger API Documentation

Open:

```text
http://127.0.0.1:8000/docs
```

Swagger provides an interactive interface for testing the API.

You can test:

- health check
- create job
- list jobs
- get job
- delete job

---

# API Endpoints

## GET `/`

Returns the root API response.

---

## GET `/health`

Checks whether the API is running.

Example:

```text
GET http://127.0.0.1:8000/health
```

---

## GET `/jobs`

Returns the available jobs.

Example:

```text
GET http://127.0.0.1:8000/jobs
```

---

## POST `/jobs`

Creates a new job.

Example request:

```json
{
  "name": "API_SUCCESS_TEST",
  "command": "echo API_TEST_SUCCESS",
  "priority": 100,
  "max_retries": 3
}
```

Example response:

```json
{
  "id": 280,
  "name": "API_SUCCESS_TEST",
  "command": "echo API_TEST_SUCCESS",
  "status": "completed",
  "attempts": 1,
  "max_retries": 3,
  "priority": 100,
  "next_run_at": null,
  "last_error": null,
  "created_at": "2026-08-26T09:08:50.625430"
}
```

---

## GET `/jobs/{job_id}`

Returns a specific job.

Example:

```text
GET /jobs/280
```

A successfully completed job returns information such as:

```json
{
  "id": 280,
  "name": "API_SUCCESS_TEST",
  "command": "echo API_TEST_SUCCESS",
  "status": "completed",
  "attempts": 1,
  "max_retries": 3,
  "priority": 100,
  "next_run_at": null,
  "last_error": null
}
```

---

## DELETE `/jobs/{job_id}`

Deletes a job.

Example:

```text
DELETE /jobs/280
```

Response:

```json
{
  "message": "Job deleted",
  "id": 280
}
```

After deletion, requesting the same job returns:

```text
404 Not Found
```

with:

```json
{
  "detail": "Job not found"
}
```

---

# 6. Start a Worker

Open another PowerShell terminal.

Navigate to the project:

```powershell
cd "Distributed Job Scheduler"
```

Activate the virtual environment:

```powershell
.\venv\Scripts\Activate.ps1
```

Start the worker:

```powershell
python .\worker.py
```

You should see output similar to:

```text
[WORKER] Registered worker: HIMANSHU-XXXX
[WORKER] Worker started
[WORKER] Waiting for pending jobs...
```

The worker will continuously wait for jobs.

---

# 7. Run Multiple Workers

The scheduler supports multiple workers.

Open another terminal:

```powershell
cd "Distributed Job Scheduler"
.\venv\Scripts\Activate.ps1
python .\worker.py
```

For example:

```text
Worker 1 -> HIMANSHU-7192
Worker 2 -> HIMANSHU-9892
```

Both workers can independently claim and execute jobs.

---

# Distributed Execution

When multiple jobs are submitted, workers compete for pending jobs.

For example:

```text
Worker 1
    |
    +---- Job 1
    +---- Job 3
    +---- Job 5
    +---- Job 7

Worker 2
    |
    +---- Job 2
    +---- Job 4
    +---- Job 6
    +---- Job 8
```

The database is responsible for coordinating job ownership.

This prevents two workers from normally executing the same pending job simultaneously.

---

# Job Lifecycle

A job can move through states such as:

```text
pending
   |
   v
running
   |
   +----------+
   |          |
   v          v
completed   failed
              |
              v
           retry
              |
              v
           pending
```

If the maximum number of attempts is exhausted:

```text
failed
```

The job remains permanently failed.

---

# Job Priorities

Jobs have a priority value.

Example:

```json
{
  "name": "HIGH_PRIORITY_JOB",
  "command": "echo HIGH",
  "priority": 100,
  "max_retries": 3
}
```

Higher priority jobs should receive preference when the worker selects pending jobs.

Example priority values:

```text
PRIORITY_100
PRIORITY_80
PRIORITY_50
PRIORITY_20
PRIORITY_10
```

A priority test was performed with five jobs.

The jobs were stored and completed with their priority values visible:

```text
PRIORITY_FINAL_5    100
PRIORITY_FINAL_4     80
PRIORITY_FINAL_3     50
PRIORITY_FINAL_2     20
PRIORITY_FINAL_1     10
```

Worker logs also displayed:

```text
Priority: 100 | Aging bonus: 0 | Effective priority: 100
Priority: 80  | Aging bonus: 0 | Effective priority: 80
Priority: 50  | Aging bonus: 0 | Effective priority: 50
Priority: 20  | Aging bonus: 0 | Effective priority: 20
Priority: 10  | Aging bonus: 0 | Effective priority: 10
```

---

# Priority Aging

The scheduler also supports priority aging.

Aging increases the effective priority of jobs that remain waiting.

Conceptually:

```text
effective_priority
    =
base_priority
    +
aging_bonus
```

Example:

```text
Priority: 100
Aging bonus: 3
Effective priority: 103
```

This helps prevent low-priority jobs from waiting indefinitely.

---

# Automatic Retry

Failed jobs can automatically retry.

Example job:

```json
{
  "name": "API_RETRY_TEST",
  "command": "cmd /c exit 1",
  "priority": 100,
  "max_retries": 2
}
```

The command intentionally fails.

The worker produces output similar to:

```text
[WORKER] Job 281 failed with exit code 1
[WORKER] Job 281 will be retried (attempt 1 of 3)
[WORKER] Retry scheduled in 5 seconds

[WORKER] Job 281 failed with exit code 1
[WORKER] Job 281 will be retried (attempt 2 of 3)
[WORKER] Retry scheduled in 10 seconds

[WORKER] Job 281 failed with exit code 1
[WORKER] Job 281 permanently failed after 3 attempts
```

The database records:

```text
status       = failed
attempts     = 3
max_retries  = 2
last_error   = Exit code 1
```

---

# Retry Backoff

Retry delays increase between attempts.

Example:

```text
Attempt 1
    |
    +---- wait 5 seconds
    |
Attempt 2
    |
    +---- wait 10 seconds
    |
Attempt 3
```

This prevents a continuously failing job from being executed repeatedly without delay.

---

# Successful Job Example

A simple successful command:

```json
{
  "name": "API_SUCCESS_TEST",
  "command": "echo API_TEST_SUCCESS",
  "priority": 100,
  "max_retries": 3
}
```

Worker output:

```text
[WORKER] Executing job id=280 name=API_SUCCESS_TEST
[WORKER] Command: echo API_TEST_SUCCESS
[WORKER] Priority: 100 | Aging bonus: 0 | Effective priority: 100
[WORKER] Job 280 completed
[WORKER] Output: API_TEST_SUCCESS
[WORKER] Job 280 marked as completed in database
```

---

# Stale Job Recovery / Failover

The scheduler supports recovery of jobs that were claimed by a worker but became stale.

For example:

```text
Worker 1
   |
   +---- claims Job 303
   |
   +---- worker stops/fails
             |
             v
        stale job detected
             |
             v
Worker 2 claims Job 303
             |
             v
          completed
```

A failover test was performed where a stale job was recovered by another worker.

The database showed:

```text
303 | FINAL_FAILOVER_TEST | completed | HIMANSHU-7192 | attempts = 2
```

The worker log showed:

```text
[WORKER] Recovered stale job id=303
[WORKER] Executing job id=303 name=FINAL_FAILOVER_TEST
[WORKER] Job 303 completed
```

This demonstrates that a stale claimed job can be recovered and executed by another worker.

---

# Worker Distribution Test

The scheduler was also tested with two workers.

A batch of 20 jobs was submitted.

Database verification showed:

```text
total_jobs | completed_jobs | failed_jobs | workers_used
-----------+----------------+-------------+-------------
20         | 20             | 0           | 2
```

Worker distribution:

```text
worker_id       | jobs_completed
----------------+---------------
HIMANSHU-7192   | 10
HIMANSHU-9892   | 10
```

Therefore:

```text
20 jobs
20 completed
0 failed
2 workers
10 jobs per worker
```

This demonstrates actual distributed job execution across multiple worker processes.

---

# Database Verification

PostgreSQL can be accessed through Docker.

Example:

```powershell
docker exec -it distributedjobscheduler-db-1 psql -U scheduler -d scheduler_db
```

Useful query:

```sql
SELECT
    id,
    name,
    status,
    worker_id,
    attempts,
    last_error
FROM jobs
ORDER BY id DESC;
```

---

# Check Worker Distribution

```powershell
docker exec -it distributedjobscheduler-db-1 psql -U scheduler -d scheduler_db -P pager=off -c "SELECT worker_id, COUNT(*) AS jobs_completed FROM jobs WHERE status='completed' GROUP BY worker_id ORDER BY worker_id;"
```

Example result:

```text
worker_id       | jobs_completed
----------------+---------------
HIMANSHU-7192   | 10
HIMANSHU-9892   | 10
```

---

# Check Job Statistics

```powershell
docker exec -it distributedjobscheduler-db-1 psql -U scheduler -d scheduler_db -P pager=off -c "SELECT COUNT(*) AS total_jobs, COUNT(*) FILTER (WHERE status='completed') AS completed_jobs, COUNT(*) FILTER (WHERE status='failed') AS failed_jobs, COUNT(DISTINCT worker_id) AS workers_used FROM jobs;"
```

Example:

```text
total_jobs | completed_jobs | failed_jobs | workers_used
-----------+----------------+-------------+-------------
20         | 20             | 0           | 2
```

---

# Check Retry Information

```powershell
docker exec -it distributedjobscheduler-db-1 psql -U scheduler -d scheduler_db -P pager=off -c "SELECT id,name,status,attempts,max_retries,last_error,worker_id FROM jobs ORDER BY id DESC LIMIT 10;"
```

---

# Check a Specific Job

Example:

```powershell
docker exec -it distributedjobscheduler-db-1 psql -U scheduler -d scheduler_db -P pager=off -c "SELECT id,name,status,worker_id,claimed_at,attempts,last_error FROM jobs WHERE id=280;"
```

---

# Testing Through Swagger

Open:

```text
http://127.0.0.1:8000/docs
```

## Test 1 — Health

Open:

```text
GET /health
```

Click:

```text
Try it out
```

Then:

```text
Execute
```

Expected result:

```text
200 OK
```

---

## Test 2 — Create Successful Job

Open:

```text
POST /jobs
```

Use:

```json
{
  "name": "API_SUCCESS_TEST",
  "command": "echo API_TEST_SUCCESS",
  "priority": 100,
  "max_retries": 3
}
```

Click:

```text
Execute
```

Then check the worker terminal.

Expected:

```text
Job completed
Output: API_TEST_SUCCESS
```

---

## Test 3 — List Jobs

Open:

```text
GET /jobs
```

Click:

```text
Execute
```

The created jobs should appear.

---

## Test 4 — Get a Job

Open:

```text
GET /jobs/{job_id}
```

Enter a valid job ID:

```text
280
```

Click:

```text
Execute
```

Expected:

```text
200 OK
```

---

## Test 5 — Delete a Job

Open:

```text
DELETE /jobs/{job_id}
```

Enter:

```text
280
```

Click:

```text
Execute
```

Expected:

```json
{
  "message": "Job deleted",
  "id": 280
}
```

Trying to retrieve the same job afterward should return:

```text
404 Job not found
```

---

# Testing a Failed Job

Create:

```json
{
  "name": "FINAL_RETRY_TEST",
  "command": "cmd /c exit 1",
  "priority": 100,
  "max_retries": 2
}
```

The worker should retry the job.

Expected behavior:

```text
Attempt 1 -> failed
Attempt 2 -> failed
Attempt 3 -> failed
             |
             v
       permanently failed
```

---

# Testing Two Workers

Start Worker 1:

```powershell
python .\worker.py
```

Start Worker 2 in another terminal:

```powershell
python .\worker.py
```

Then create multiple jobs.

Check distribution:

```powershell
docker exec -it distributedjobscheduler-db-1 psql -U scheduler -d scheduler_db -P pager=off -c "SELECT worker_id, COUNT(*) AS jobs_completed FROM jobs WHERE status='completed' GROUP BY worker_id ORDER BY worker_id;"
```

Expected:

```text
Multiple worker IDs
```

and jobs distributed between them.

---

# Important Design Concepts

## Atomic Job Claiming

A worker must safely claim a job so that two workers do not execute the same pending job simultaneously.

The database is used as the coordination point.

---

## Worker Registration

Every worker receives a worker identifier.

Example:

```text
HIMANSHU-7192
HIMANSHU-9892
```

The worker ID is associated with claimed jobs.

---

## Job Ownership

When a worker claims a job, the job records information about the worker that owns it.

This allows the system to track:

```text
Which worker claimed the job?
Which worker executed the job?
How many jobs did each worker complete?
```

---

## Retry State

Each job tracks:

```text
attempts
max_retries
last_error
next_run_at
```

This allows failed jobs to be retried without losing their state.

---

# Recommended Startup Order

For a fresh run:

### Terminal 1 — Docker

```powershell
docker compose up -d
```

### Terminal 2 — API

```powershell
.\venv\Scripts\Activate.ps1
python -m uvicorn app.main:app --reload
```

### Terminal 3 — Worker 1

```powershell
.\venv\Scripts\Activate.ps1
python .\worker.py
```

### Terminal 4 — Worker 2

```powershell
.\venv\Scripts\Activate.ps1
python .\worker.py
```

Then open:

```text
http://127.0.0.1:8000/docs
```

---

# Stopping the Project

Stop the API and worker terminals with:

```text
Ctrl + C
```

Stop Docker services:

```powershell
docker compose down
```

---

# Useful Docker Commands

Show running containers:

```powershell
docker ps
```

Show Compose services:

```powershell
docker compose ps
```

View logs:

```powershell
docker compose logs
```

Stop services:

```powershell
docker compose down
```

Start services again:

```powershell
docker compose up -d
```

---

# Current Tested Capabilities

The following functionality has been tested during development:

| Feature | Status |
|---|---|
| FastAPI server | Tested |
| Swagger documentation | Tested |
| Health endpoint | Tested |
| Create job API | Tested |
| List jobs API | Tested |
| Get job API | Tested |
| Delete job API | Tested |
| Successful command execution | Tested |
| Failed command execution | Tested |
| Automatic retries | Tested |
| Retry backoff | Tested |
| Maximum retry handling | Tested |
| Job priority | Tested |
| Priority aging | Tested |
| Worker registration | Tested |
| Multiple workers | Tested |
| Distributed job execution | Tested |
| Worker distribution | Tested |
| Stale job recovery/failover | Tested |
| PostgreSQL persistence | Tested |

---

# Example End-to-End Flow

```text
1. Client sends POST /jobs
             |
             v
2. FastAPI validates request
             |
             v
3. Job inserted into PostgreSQL
             |
             v
4. Worker polls database
             |
             v
5. Worker claims job
             |
             v
6. Command executed
             |
       +-----+-----+
       |           |
    Success      Failure
       |           |
       v           v
  completed      retry
                   |
              +----+----+
              |         |
          attempts    max attempts
          remaining    exhausted
              |         |
              v         v
           retry      failed
```

---

# Why This Project Is Distributed

This is not simply a background-task API.

The scheduler supports multiple independent worker processes that share the same PostgreSQL job queue.

For example:

```text
                 PostgreSQL
                     |
          +----------+----------+
          |                     |
          v                     v
      Worker 1              Worker 2
          |                     |
       Job A                  Job B
       Job C                  Job D
       Job E                  Job F
```

Workers can be started independently, and jobs are coordinated through the shared database.

---

# Future Improvements

Possible future enhancements include:

- Redis-based queueing
- Worker heartbeat monitoring
- Worker health dashboard
- WebSocket live job updates
- Authentication and authorization
- Rate limiting
- Job cancellation
- Scheduled jobs
- Cron-style jobs
- Job dependencies
- Dead-letter queue
- Metrics with Prometheus
- Grafana dashboard
- Dockerized API and workers
- Kubernetes deployment
- Horizontal worker autoscaling
- Distributed tracing
- Structured logging
- CI/CD pipeline

These are **future improvements** and are not currently claimed as implemented features.

---

# Project Summary

The Distributed Job Scheduler demonstrates how a background job execution system can be designed using:

```text
Python
   +
FastAPI
   +
PostgreSQL
   +
Multiple Workers
   +
Docker
```

The system provides:

```text
REST API
   |
Job Persistence
   |
Distributed Workers
   |
Priority Scheduling
   |
Retry Handling
   |
Failure Recovery
   |
Worker Distribution
```

The project demonstrates practical concepts in:

- distributed systems
- backend development
- database concurrency
- job queues
- worker processes
- fault tolerance
- retry mechanisms
- scheduling
- REST API development
- PostgreSQL
- Docker