import json
import uuid
import time 
import random

import redis.asyncio as redis

from app.config import REDIS_URL

QUEUE_KEY = "jobs:queue"

PRIORITY_OFFSET_SECONDS = {
        "high" : -20,
        "normal" : -10,
        "low": 0,
    }

DEFAULT_MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1
MAX_BACKOFF_SECONDS = 30


_client : redis.Redis | None = None

def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(REDIS_URL, decode_responses = True)
    return  _client


def _job_key(job_id: str) ->str:
    return f"job:{job_id}"

async def enqueue_job(task:str, payload:dict, priority:str = "normal", max_retries:int = DEFAULT_MAX_RETRIES)->str:
    if priority not in PRIORITY_OFFSET_SECONDS:
       raise ValueError(f"invalid priority:{priority}")

    client = get_client()
    job_id = str(uuid.uuid4())
    now = time.time()
    score = now + PRIORITY_OFFSET_SECONDS[priority]


    job_data = {
        "task"  : task, 
        "payload" : json.dumps(payload), 
        "priority" : priority,
        "status" : "enqueued",
        "enqueued_at" : str(now),
        "retry_count" : "0",
        "max_retries" : str(max_retries),
    }

    await client.hset(_job_key(job_id), mapping = job_data)
    await client.zadd(QUEUE_KEY, {job_id:score})

    return job_id

def compute_backoff_delay(retry_count:int)->float:
    jitter = random.uniform(0, 1)
    delay = BASE_BACKOFF_SECONDS * (2 ** retry_count) * (jitter+1)
    return min(delay, MAX_BACKOFF_SECONDS + jitter)

async def schedule_retry(job_id:str, retry_count:int):
    client = get_client()
    delay = compute_backoff_delay(retry_count)
    score = time.time() + delay

    await client.hset(_job_key(job_id), mapping = {
        "status": "retrying",
        "retry_count": str(retry_count),
        })
    await client.zadd(QUEUE_KEY, {job_id:score})

    return delay


async def get_job(job_id : str)->dict | None:
    client = get_client()
    data = await client.hgetall(_job_key(job_id))
    if not data:
        return None
    data["payload"] = json.loads(data["payload"])
    return data


async def peek_next_job_id()->str | None:
    client = get_client()
    result = await client.zrange(QUEUE_KEY, 0, 0)
    return result[0] if result else None

async def queue_length()->int:
    client = get_client()
    return await client.zcard(QUEUE_KEY)


async def naive_claim_job() ->str | None:
    client = get_client()
    job_id = await peek_next_job_id()
    if job_id is None:
        return None
    # await asyncio.sleep(0.1)  
    await client.zrem(QUEUE_KEY, job_id)
    return job_id

CLAIM_SCRIPT = """
local queue_key = KEYS[1]
local job_prefix = ARGV[1]
local now = tonumber(ARGV[2])

local result = redis.call('zrange', queue_key, 0, 0)
local result = redis.call('ZRANGEBYSCORE', queue_key, '-inf', now, 'LIMIT', 0, 1)
if #result == 0 then
    return nil
end

local job_id = result[1]
redis.call('zrem', queue_key, job_id)
redis.call('hset', job_prefix .. job_id, 'status', 'processing')

return job_id

"""

async def claim_job() -> str | None:
    client = get_client()
    job_id = await client.eval(CLAIM_SCRIPT, 1, QUEUE_KEY, "job:", str(time.time()))
    return job_id

