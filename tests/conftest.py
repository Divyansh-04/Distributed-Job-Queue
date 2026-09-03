import redis as sync_redis
import redis.asyncio as redis
import pytest

from app.config import REDIS_URL
from app.queue import get_client

@pytest.fixture(autouse=True)
def cleanup_redis():
    sync_client = sync_redis.from_url(REDIS_URL)
    sync_client.flushdb()
    yield
    sync_client.flushdb()
    sync_client.close()
