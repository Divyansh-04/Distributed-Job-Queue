import asyncio
import random

async def send_email(payload: dict):
    print(f"  [task] sending email to {payload.get('to')}")
    await asyncio.sleep(random.uniform(0.5, 1.5))  # Simulate variable processing time
    print("  [task] Email sent!")


async def flaky_task(payload: dict):
    """Fails ~50% of the time -- useful for testing retries in Step 8."""
    print(f"  [task] running flaky_task")
    await asyncio.sleep(0.5)
    if random.random() < 0.5:
        raise RuntimeError("flaky_task failed randomly")
    print(f"  [task] flaky_task succeeded")

async def always_fail_task(payload: dict):
    print(f"[task] running always_fail_task")
    await asyncio.sleep(0.1)
    raise RuntimeError("always_fail_task failed, always fails for testing")


TASK_REGISTRY = {
    "send_email": send_email,
    "flaky_task": flaky_task,
    "always_fail_task": always_fail_task,
}

