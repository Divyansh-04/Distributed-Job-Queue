import pytest

from app.queue import enqueue_job, get_job, claim_job
from app.worker import process_job


@pytest.mark.asyncio
async def test_process_job_success():
    job_id = await enqueue_job("send_email", {"to": "a@b.com"}, "high")
    await process_job(job_id, "test-worker")

    job = await get_job(job_id)
    assert job["status"] == "completed"
    assert "completed_at" in job


@pytest.mark.asyncio
async def test_process_job_unknown_task():
    await enqueue_job("nonexistent_task", {"random": "data", "payload":"random"}, "high")
    job_id = await claim_job()
    await process_job(job_id, "test-worker")

    job = await get_job(job_id)
    assert job["status"] == "failed"
