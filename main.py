from fastapi import FastAPI, BackgroundTasks
from uuid import uuid4
from datetime import datetime
import multiprocessing
from join_processor import perform_join
from contextlib import asynccontextmanager

# Global state for jobs
manager = None
jobs = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager, jobs
    manager = multiprocessing.Manager()
    jobs = manager.dict()
    yield
    manager.shutdown()

app = FastAPI(
    title="Scalable Data Processing API",
    description="API for triggering large CSV joins in the background",
    version="1.0.0",
    lifespan=lifespan
)


def run_join_job(job_id: str, jobs_dict):
    current = jobs_dict[job_id]
    current["status"] = "running"
    current["started_at"] = str(datetime.now())
    jobs_dict[job_id] = current

    try:
        perform_join()
        current = jobs_dict[job_id]
        current["status"] = "completed"
        current["finished_at"] = str(datetime.now())
        current["output_file"] = "result.csv"
        jobs_dict[job_id] = current

    except Exception as e:
        current = jobs_dict[job_id]
        current["status"] = "failed"
        current["error"] = str(e)
        current["finished_at"] = str(datetime.now())
        jobs_dict[job_id] = current


@app.get("/")
def home():
    return {
        "message": "Scalable Data Processing API is running"
    }


# Approach 1: FastAPI BackgroundTasks (Uses Threading under the hood)
@app.post("/trigger-join")
def trigger_join(background_tasks: BackgroundTasks):
    job_id = str(uuid4())

    jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "created_at": str(datetime.now()),
        "approach": "FastAPI BackgroundTasks (Thread)"
    }

    background_tasks.add_task(run_join_job, job_id, jobs)

    return {
        "message": "Join job triggered successfully",
        "job_id": job_id,
        "status": "queued",
        "approach": "FastAPI BackgroundTasks (Thread)"
    }


# Approach 2: Multiprocessing (Separate OS Process, Bypasses GIL)
@app.post("/trigger-join-process")
def trigger_join_process():
    job_id = str(uuid4())

    jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "created_at": str(datetime.now()),
        "approach": "Multiprocessing (Process)"
    }

    # Spawn a separate OS process for heavy CPU-bound data processing
    p = multiprocessing.Process(target=run_join_job, args=(job_id, jobs))
    p.start()

    return {
        "message": "Join job triggered successfully",
        "job_id": job_id,
        "status": "queued",
        "approach": "Multiprocessing (Process)"
    }


@app.get("/job-status/{job_id}")
def get_job_status(job_id: str):
    if job_id not in jobs:
        return {
            "error": "Invalid job_id"
        }

    # Convert Manager dict to standard dict for JSON serialization
    return dict(jobs[job_id])
