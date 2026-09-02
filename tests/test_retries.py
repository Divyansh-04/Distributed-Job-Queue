import pytest

from app.worker import process_job
from app.queue import enqueue_job, get_job, claim_job, compute_backoff_delay, MAX_BACKOFF_SECONDS


def test_backoff_delay_increases_and_cap():
    delays = [compute_backoff_delay(i) for i in range(5)]
    for i in range(1, len(delays)):
        assert delays[i] > delays[i-1], f"Delay did not increase for retry count {i}"

    assert delays[-1] <= MAX_BACKOFF_SECONDS+1, "Delay exceeded maximum backoff limit"

@pytest.mark.asyncio
async def test_job_retries_then_permanently_fails(monkeypatch):
    import asyncio
    monkeypatch.setattr("app.queue.compute_backoff_delay", lambda retry_count: 0.10)

    await enqueue_job("always_fail_task", {"test": "data"}, priority="normal", max_retries=2)

    job_id = await claim_job()
    await process_job(job_id, "test-worker")
    job = await get_job(job_id)
    assert job["status"] == "retrying"
    assert job['retry_count'] == "1"

    assert await claim_job() is None 

    await asyncio.sleep(0.12)  

    job_id = await claim_job()
    await process_job(job_id, "test-worker")
    job = await get_job(job_id)
    assert job["status"] == "retrying"
    assert job['retry_count'] == "2"

    await asyncio.sleep(0.12)  

    job_id = await claim_job()
    await process_job(job_id, "test-worker")
    job = await get_job(job_id)
    assert job["status"] == "failed"
    