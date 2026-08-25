from datetime import datetime

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from .database import engine, get_db, Base
from .models import Job


app = FastAPI(
    title="Distributed Job Scheduler API",
    description="A distributed background job scheduling service.",
    version="1.0.0",
)


# Create database tables when the application starts
Base.metadata.create_all(bind=engine)


class JobCreate(BaseModel):
    name: str
    command: str
    priority: int = Field(default=0, ge=0, le=100)
    max_retries: int = Field(default=3, ge=0, le=10)


@app.get("/")
def root():
    return {
        "message": "Distributed Job Scheduler API",
        "status": "running",
    }


@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))

    return {
        "status": "healthy",
        "database": "connected",
    }


@app.post("/jobs", status_code=201)
def create_job(
    job_data: JobCreate,
    db: Session = Depends(get_db),
):
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
        "last_error": job.last_error,
        "created_at": job.created_at,
    }


@app.get("/jobs")
def list_jobs(db: Session = Depends(get_db)):
    jobs = db.query(Job).order_by(Job.id.desc()).all()

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
            "last_error": job.last_error,
            "created_at": job.created_at,
        }
        for job in jobs
    ]


@app.get("/jobs/{job_id}")
def get_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    job = db.query(Job).filter(Job.id == job_id).first()

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
        "last_error": job.last_error,
        "created_at": job.created_at,
    }


@app.delete("/jobs/{job_id}")
def delete_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    job = db.query(Job).filter(Job.id == job_id).first()

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