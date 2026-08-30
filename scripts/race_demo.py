import asyncio

from app.queue import enqueue_job, naive_claim_job, get_client, claim_job


NUM_JOBS = 20
NUM_CONCURRENT_WORKERS = 20

async def run_demo(claim_fn, label):
    client = get_client()
    await client.flushdb()

    for i in range(NUM_JOBS):
        await enqueue_job("send_email", {"job_number": i}, priority="normal")


    results = await asyncio.gather(*[claim_fn() for _ in range(NUM_CONCURRENT_WORKERS)])
    claimed = [r for r in results if r is not None]
    duplicates = len(claimed) - len(set(claimed))

    print(f"\n=== {label} ===")
    print(f"Jobs enqueued:        {NUM_JOBS}")
    print(f"Concurrent claimers:  {NUM_CONCURRENT_WORKERS}")
    print(f"Successful claims:    {len(claimed)}")
    print(f"Unique jobs claimed:  {len(set(claimed))}")
    print(f"Duplicate claims:     {duplicates}")


async def main():
    await run_demo(naive_claim_job, "Naive Claim Job (peek+zrem)")
    await asyncio.sleep(1)  # wait a bit before running the next demo
    await run_demo(claim_job, "Atomic Claim Job (lua script)")


    client = get_client()
    await client.aclose()

if __name__ == "__main__":
    asyncio.run(main())

