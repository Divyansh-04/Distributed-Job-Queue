import pytest
from httpx import AsyncClient, ASGITransport
from app.api import app

@pytest.mark.asyncio
async def test_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport = transport, base_url = "http://test") as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
async def test_submit_job():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport = transport, base_url = "http://test") as client:
        response = await client.post("/jobs", json = {"task":"send_email", "payload":{"to":"a@b.com"}})
    assert response.status_code == 200


