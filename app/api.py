import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.queue import enqueue_job, get_job, queue_length, get_dlq_length, get_dlq_jobs, requeue_dlq_job

VERBOSE = os.getenv("WORKER_VERBOSE", "false").lower() == "true"

def log(msg: str):
    if VERBOSE:
        print(msg)


app = FastAPI(title = "Distributed Job Queue")

class JobSubmitRequest(BaseModel):
    task : str
    payload : dict = {}
    priority : str = "normal"



@app.get('/health')
async def health():
    return {"status" : "ok"}

@app.post('/jobs')
async def submit_job(job : JobSubmitRequest):
    try:
        job_id = await enqueue_job(job.task, job.payload, job.priority)
    except ValueError as e:
        raise HTTPException(status_code = 400, detail = str(e))
    return {"job_id": job_id, "status":"queued"}

@app.get('/jobs/{job_id}')
async def get_job_status(job_id:str) :
    job = await get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get('/queue/length')
async def get_queue_length():
    return {"length": await queue_length()}


@app.get('dlq/length')
async def get_dlq_length():
    return {"length": await get_dlq_length()}

@app.get('/dlq')
async def list_dlq_jobs(limit:int = 50):
    return {"count": await get_dlq_length(), "jobs": await list_dlq_jobs(limit)}

@app.post('/dlq/retry/{job_id}')
async def requeue_from_dlq(job_id:str):
    success = await requeue_dlq_job(job_id)
    if not success:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": "requeued"}