import asyncio
import time 
import os

from app.queue import get_client, get_job, _job_key, claim_job, schedule_retry, move_job_to_dlq
from app.tasks import TASK_REGISTRY


POLL_INTERVAL_SECONDS = 0.5
CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", '4'))
VERBOSE = os.getenv("WORKER_VERBOSE", "false").lower() == "true"

def log(msg: str):
    if VERBOSE:
        print(msg)


async def process_job(job_id:str, worker_tag: str):
    client = get_client()
    job = await get_job(job_id)

    if job is None:
        log(f"[{worker_tag}] job not found:", job_id)
        return

    task_name = job["task"]
    handler = TASK_REGISTRY.get(task_name)

    if handler is None:
        log(f"[{worker_tag}] unknown task type '{task_name}', marking failed")
        await move_job_to_dlq(job_id, f"unknown task type '{task_name}'")
        return

    log(f"[{worker_tag}] running job {job_id} ({task_name})")
    try:
        await handler(job["payload"])
        await client.hset(_job_key(job_id), "status", "completed")
        await client.hset(_job_key(job_id), "completed_at", str(time.time()))
        log(f"[{worker_tag}] job {job_id} completed")

    except Exception as e:
        retry_count = int(job.get("retry_count", "0"))+1
        max_retries = int(job.get("max_retries"))

        if retry_count <= max_retries:
            delay = await schedule_retry(job_id, retry_count)
            log(f"[{worker_tag}] job {job_id} failed: {e}, retrying in {delay:.2f} seconds (retry {retry_count}/{max_retries})")
        else:
            await move_job_to_dlq(job_id, str(e))
            log(f"[{worker_tag}] job {job_id} failed: {e}, after {retry_count-1} retries, moved to DLQ")


async def worker_slot(slot_it:int):
    tag = f"worker-slot-{slot_it}"
    log(f"{tag} starting")

    while True:
        job_id = await claim_job()

        if job_id is None:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            continue

        log(f"{tag} processing job:", job_id) 
        await process_job(job_id, tag)


async def worker_loop():
    log(f"[worker] starting {CONCURRENCY} concurrent slots...")
    await asyncio.gather(*[worker_slot(i) for i in range(CONCURRENCY)])


if __name__ == "__main__":
    asyncio.run(worker_loop())