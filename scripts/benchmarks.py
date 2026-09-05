import asyncio
import random
import time
import httpx


API_BASE = "http://localhost:8000"

from app.tasks import BENCHMARK_TASK_WEIGHTS


def pick_task() -> str:
    """Randomly pick a task based on the weights defined in BENCHMARK_TASK_WEIGHTS."""
    tasks, weights = zip(*BENCHMARK_TASK_WEIGHTS.items())
    return random.choices(tasks, weights=weights, k=1)[0]

async def submit_job(client : httpx.AsyncClient) -> str:
    task = pick_task()
    resp = await client.post(f"{API_BASE}/jobs", json={"task": task, "payload": {}})
    resp.raise_for_status()
    return resp.json()["job_id"]

async def wait_for_completion(client : httpx.AsyncClient, job_id: str, poll_interval: float = 0.1) -> dict:
    while True:
        resp = await client.get(f"{API_BASE}/jobs/{job_id}")
        resp.raise_for_status()
        job_data = resp.json()
        if job_data["status"] in ("completed", "failed"):
            return job_data
        await asyncio.sleep(poll_interval)  


async def run_benchmark(num_jobs: int, submit_concurrency: int = 20):
    async with httpx.AsyncClient(timeout=30.0) as client:
        start = time.time()

        # Submit jobs with bounded concurrency, not all at once -- avoids
        # overwhelming the API with num_jobs simultaneous connections.
        semaphore = asyncio.Semaphore(submit_concurrency)

        async def submit_bounded():
            async with semaphore:
                return await submit_job(client)

        job_ids = await asyncio.gather(*[submit_bounded() for _ in range(num_jobs)])
        submit_done = time.time()

        results = await asyncio.gather(*[wait_for_completion(client, jid) for jid in job_ids])
        end = time.time()

        return results, start, submit_done, end


def compute_metrics(results: list[dict], start: float, end: float, num_jobs: int):
    completed = [r for r in results if r["status"] == "completed"]
    failed = [r for r in results if r["status"] == "failed"]

    latencies = [
        float(r["completed_at"]) - float(r["enqueued_at"])
        for r in completed
        if "completed_at" in r and "enqueued_at" in r
    ]
    latencies.sort()

    total_elapsed = end - start
    throughput = num_jobs / total_elapsed if total_elapsed > 0 else 0

    def percentile(data, p):
        if not data:
            return None
        idx = int(len(data) * p) 
        return data[min(idx, len(data) - 1)]

    return {
        "total_jobs": num_jobs,
        "completed": len(completed),
        "failed": len(failed),
        "total_elapsed_sec": round(total_elapsed, 2),
        "throughput_jobs_per_sec": round(throughput, 2),
        "latency_p50_sec": round(percentile(latencies, 0.50), 3) if latencies else None,
        "latency_p95_sec": round(percentile(latencies, 0.95), 3) if latencies else None,
        "latency_p99_sec": round(percentile(latencies, 0.99), 3) if latencies else None,
    }


async def main():
    NUM_JOBS = 1
    results, start, submit_done, end = await run_benchmark(NUM_JOBS)
    metrics = compute_metrics(results, start, end, NUM_JOBS)

    print(f"\n=== Benchmark Results ({NUM_JOBS} jobs) ===")
    for k, v in metrics.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    asyncio.run(main())
