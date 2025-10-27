from unittest.mock import AsyncMock, MagicMock

import pytest

from rotisserie import AsyncKeyPool, KeyConfig


@pytest.mark.asyncio
async def test_httpx_injection_header(monkeypatch):
    pool = AsyncKeyPool(
        [KeyConfig("k", "T")], auth_header="X-Auth", auth_scheme="Token"
    )

    # Mock httpx.AsyncClient
    async_client = AsyncMock()
    resp = MagicMock()
    resp.status_code = 200
    resp.headers = {}
    async_client.request.return_value = resp

    async with pool.httpx_client("ep", reserve=1, priority=1) as client:
        # inject our client
        client.client = async_client
        r = await client.get("https://example.com")
        assert r is resp
        args, kwargs = async_client.request.call_args
        assert kwargs["headers"]["X-Auth"].startswith("Token ")


@pytest.mark.asyncio
async def test_httpx_injection_query(monkeypatch):
    pool = AsyncKeyPool(
        [KeyConfig("k", "T")], auth_in="query", auth_query_param="api_key"
    )
    async_client = AsyncMock()
    resp = MagicMock()
    resp.status_code = 200
    resp.headers = {}
    async_client.request.return_value = resp
    async with pool.httpx_client("ep", reserve=1, priority=1) as client:
        client.client = async_client
        _ = await client.get("https://example.com")
        args, kwargs = async_client.request.call_args
        assert kwargs["params"]["api_key"] == "T"
