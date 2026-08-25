import json
import uuid
import time 

import redis.asyncio as redis

from app.config import REDIS_URL

QUEUE_KEY = "jobs:queue"

PRIORITY_OFFSET_SECONDS = {
        "high" : 0,
        "normal" : 5,
        "low": 20
        }

_client : redis.Redis | None = None

def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(REDIS_URL, decode_responses = True)
    return  _client


def _job_key(job_id: str) ->str:
    return f"job: {job_id}"

async def enqueue_job(task:str, payload:dict, priority:str = "normal")->str:
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
        "enqueued_at" : now
    }

    await client.hset(_job_key(job_id), mapping = job_data)
    await client.zadd(QUEUE_KEY, {job_id:score})

    return job_id


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



