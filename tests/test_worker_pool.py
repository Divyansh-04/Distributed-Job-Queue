import pytest
import asyncio

from app.queue import enqueue_job, claim_job, get_job

@pytest.mark.asyncio
async def test_concurrent_slots_no_duplicate_processing():
    job_ids = [await enqueue_job("send_email", {"job_number": i}, priority="normal") for i in range(10)]

    async def one_slot_claim_all():
        claimed_jobs = []
        while True:
            job_id = await claim_job()
            if job_id is None:
                break
            claimed_jobs.append(job_id)
        return claimed_jobs


    results = await asyncio.gather(*[one_slot_claim_all() for _ in range(3)])
    all_claimed_jobs = [job_id for sublist in results for job_id in sublist]

    assert len(all_claimed_jobs) == len(job_ids), "Not all jobs were claimed"
    assert set(all_claimed_jobs) == set(job_ids), "Claimed jobs do not match enqueued jobs"
    assert len(all_claimed_jobs) == len(set(all_claimed_jobs)), "Duplicate jobs found in concurrent processing"