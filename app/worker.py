import asyncio
import time 

from app.queue import get_client, get_job, _job_key, claim_job
from app.tasks import TASK_REGISTRY

POLL_INTERVAL_SECONDS = 0.2


async def process_job(job_id:str):
    client = get_client()
    job = await get_job(job_id)

    if job is None:
        print("[worker] job not found:", job_id)
        return

    task_name = job["task"]
    handler = TASK_REGISTRY.get(task_name)

    if handler is None:
        print(f"[worker] unknown task type '{task_name}', marking failed")
        await client.hset(_job_key(job_id), "status", "failed")
        return

    try:
        await handler(job["payload"])
        await client.hset(_job_key(job_id), "status", "completed")
        await client.hset(_job_key(job_id), "completed_at", str(time.time()))
    except Exception as e:
        print(f"[worker] job {job_id} failed: {e}")
        await client.hset(_job_key(job_id), "status", "failed")


async def worker_loop():
    client = get_client()
    print("[worker] starting, polling for jobs...")
    while True:
        job_id = await claim_job()

        if job_id is None:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            continue

        print("[worker] processing job:", job_id) 
        await process_job(job_id)


if __name__ == "__main__":
    asyncio.run(worker_loop())