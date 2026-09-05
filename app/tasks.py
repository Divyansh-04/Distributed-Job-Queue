import asyncio
import random
import os
VERBOSE = os.getenv("WORKER_VERBOSE", "false").lower() == "true"

def log(msg: str):
    if VERBOSE:
        print(msg)

async def send_email(payload: dict):
    log(f"  [task] sending email to {payload.get('to')}")
    await asyncio.sleep(0.3)  # Simulate variable processing time
    log("  [task] Email sent!")


async def resize_image(payload: dict):
    await asyncio.sleep(random.uniform(0.5, 1.2)) 
    log("  [task] Image resized!")

async def process_data(payload: dict):
    await asyncio.sleep(random.uniform(1, 3))
    log("  [task] Data processed!")

async def compute_heavy_task(payload: dict):
    n = payload.get("n", 500_000)
    f = 1
    for i in range(n):
        f *= f
        f %= 1000000007
    return f
    log("  [task] Heavy task computed!")

async def flaky_task(payload: dict):

    log(f"  [task] running flaky_task")
    await asyncio.sleep(random.uniform(0, 0.5))
    if random.random() < 0.5:
        raise RuntimeError("flaky_task failed randomly")
    log(f"  [task] flaky_task succeeded")

async def always_fail_task(payload: dict):
    log(f"[task] running always_fail_task")
    await asyncio.sleep(0.1)
    raise RuntimeError("always_fail_task failed, always fails for testing")


TASK_REGISTRY = {
    "send_email": send_email,
    "flaky_task": flaky_task,
    "always_fail_task": always_fail_task,
    "resize_image": resize_image,
    "process_data": process_data,
    "compute_heavy_task": compute_heavy_task,
}


BENCHMARK_TASK_WEIGHTS = {
    "send_email": 0.6,
    "resize_image": 0.25,
    "generate_report": 0.15,
    "compute_heavy_task": 0.00,
}


