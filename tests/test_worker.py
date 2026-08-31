import pytest

from app.queue import enqueue_job, get_job, get_client
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
    job_id = await enqueue_job("nonexistent_task", {"random": "data", "payload":"random"}, "high")
    await process_job(job_id, "test-worker")

    job = await get_job(job_id)
    assert job["status"] == "failed"
