from unittest.mock import AsyncMock

import pytest

from rotisserie import AsyncKeyPool, KeyConfig


class FakeResponse:
    def __init__(self, status=200, headers=None):
        self.status = status
        self.headers = headers or {}
        self.closed = False

    async def release(self):
        self.closed = True


@pytest.mark.asyncio
async def test_aiohttp_query_injection(monkeypatch):
    pool = AsyncKeyPool(
        [KeyConfig("k", "T")], auth_in="query", auth_query_param="api_key"
    )
    session = AsyncMock()
    session.request.return_value = FakeResponse(200, {})

    async with pool.aiohttp_client(
        "ep", reserve=1, priority=1, session=session
    ) as client, client.get("https://example.com") as resp:
        assert isinstance(resp, FakeResponse)
        # check that session.request was called with query param
        args, kwargs = session.request.call_args
        assert kwargs["params"]["api_key"] == "T"
