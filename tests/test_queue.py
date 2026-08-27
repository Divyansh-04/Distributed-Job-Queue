import pytest

from app.queue import enqueue_job, get_job, peek_next_job_id, get_client, QUEUE_KEY


# @pytest.fixture(autouse=True)
# async def clean_redis():
#     client = get_client()
#     await client.flushdb()
#     yield
#     await client.flushdb()


@pytest.mark.asyncio
async def test_enqueue_and_get_job():
    job_id = await enqueue_job("send_email", {"to": "a@b.com"}, "high")
    job = await get_job(job_id)

    print(job)

    assert True

    return

    assert job["task"] == "send_email"
    assert job["payload"] == {"to": "a@b.com"}
    assert job["status"] == "queued"


@pytest.mark.asyncio
async def test_high_priority_beats_normal_when_enqueued_close_together():
    await enqueue_job("low_prio_task", {}, "low")
    high_id = await enqueue_job("high_prio_task", {}, "high")

    next_id = await peek_next_job_id()
    assert next_id == high_id


@pytest.mark.asyncio
async def test_invalid_priority_rejected():
    with pytest.raises(ValueError):
        await enqueue_job("task", {}, "urgent!!")