import pytest
import asyncio


from app.queue import enqueue_job,  claim_job


@pytest.mark.asyncio
async def test_atomic_claim_no_duplicates_under_concurrency():
    NUM_JOBS = 20
    for i in range(NUM_JOBS):
        await enqueue_job("send_email", {"job_number": i}, priority="normal")

    results = await asyncio.gather(*[claim_job() for _ in range(NUM_JOBS * 2)])
    claimed = [r for r in results if r is not None]

    print(claimed)

    assert(len(claimed) == len(set(claimed))), "Duplicate job claims detected"
    assert(len(claimed) == NUM_JOBS), f"Expected {NUM_JOBS} jobs to be claimed, but got {len(claimed)}"


