import pytest
import asyncio

from app.queue import enqueue_job, get_job, claim_job, requeue_dlq_job, get_dlq_length, get_dlq_jobs, claim_job
from app.worker import process_job



@pytest.mark.asyncio
async def test_job_moves_to_dlq(monkeypatch):
    monkeypatch.setattr("app.queue.compute_backoff_delay", lambda retry_count: 0.01)
    await enqueue_job("always_fail_task", {"random": "data"}, max_retries=0)
    job_id = await claim_job()
    await process_job(job_id, "test-worker")

    await asyncio.sleep(0.1) 

    job = await get_job(job_id)
    assert job["status"] == "failed"
    assert "failed_reason" in job
    assert await get_dlq_length() == 1


@pytest.mark.asyncio
async def test_requeue_from_dlq():
    await enqueue_job("always_fail", {}, "high", max_retries=0)
    job_id = await claim_job()
    await process_job(job_id, "test-worker")

    assert await get_dlq_length() == 1

    success = await requeue_dlq_job(job_id)
    assert success is True
    assert await get_dlq_length() == 0

    job = await get_job(job_id)
    assert job["status"] == "requeued"
    assert job["retry_count"] == "0"

    # and it's actually claimable again
    claimed = await claim_job()
    assert claimed == job_id
