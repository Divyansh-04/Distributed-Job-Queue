import time
import asyncio

async def do_job(job_id):
    print(f"Job {job_id}: starting")
    await asyncio.sleep(2)
    print(f"Job {job_id}: done")


async def main():
    start = time.time()
    await asyncio.gather(
        do_job(1),
        do_job(2),
        do_job(3),
        )
    print(f"Total time: {time.time() - start:.1f}s")

asyncio.run(main())
