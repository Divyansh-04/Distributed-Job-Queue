import pytest
import asyncio

from app.queue import enqueue_job, claim_job
from app.worker import process_job

@pytest.mark.asyncio
async def test_retrying_job_not_claimable_regardless_of_priority():
    await enqueue_job("always_fail", {"data": "test"}, priority="high", max_retries=3)
    job_id = await claim_job()
    await process_job(job_id, "test-worker")

    assert await claim_job() is None, "Job should not be claimable after failing and moving to DLQ"


@pytest.mark.asyncio
async def test_retry_regains_priority_after_requeue(monkeypatch):
    monkeypatch.setattr("app.queue.compute_backoff_delay", lambda retry_count: 0.01) 
    await enqueue_job("always_fail_task", {"data": "test"}, priority="high", max_retries=3)
    job_id = await claim_job()
    await process_job(job_id, "test-worker")

    assert job_id is not None

    low_priority_job_id = await enqueue_job("send_email", {"Content": "meaningless, senseless banter"}, priority="low")
    await asyncio.sleep(0.12)  # Wait for the retry to be scheduled

    claimed = await claim_job()
    assert claimed == job_id


