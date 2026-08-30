from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.queue import enqueue_job, get_job, get_queue_length


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
