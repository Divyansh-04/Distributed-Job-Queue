import redis as sync_redis
import pytest


from app.config import REDIS_URL
import app.queue as queue_module


@pytest.fixture(autouse=True)
def cleanup_redis():
    sync_client = sync_redis.from_url(REDIS_URL)
    sync_client.flushdb()
    yield
    sync_client.flushdb()
    sync_client.close()


    if queue_module._client is not None:
        queue_module._client.aclose()
        queue_module._client = None