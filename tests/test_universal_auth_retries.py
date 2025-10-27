import pytest


def _make_response(httpx, status, req):
    return httpx.Response(status, request=req, headers={})


def test_httpx_sync_retries_on_429(monkeypatch):
    import httpx  # noqa: PLC0415

    from rotisserie import KeyConfig, KeyPool  # noqa: PLC0415

    pool = KeyPool([KeyConfig("k1", "T1"), KeyConfig("k2", "T2")])
    with pool.auth(endpoint="ep", reserve=1, priority=1) as auth, httpx.Client() as client:  # noqa: E501
        auth.wrap_httpx(client)
        inner = client._transport._inner

        # First attempt returns 429, second returns 200
        calls = {"n": 0}

        def _handle(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return _make_response(httpx, 429, request)
            return _make_response(httpx, 200, request)

        monkeypatch.setattr(inner, "handle_request", _handle)
        r = client.get("https://example.com")
        assert r.status_code == 200  # noqa: PLR2004
        # cleanup not required; client context closes transport


@pytest.mark.asyncio
async def test_httpx_async_retries_on_429(monkeypatch):
    import httpx  # noqa: PLC0415

    from rotisserie import AsyncKeyPool, KeyConfig  # noqa: PLC0415

    pool = AsyncKeyPool([KeyConfig("k1", "T1"), KeyConfig("k2", "T2")])
    async with pool.auth(endpoint="ep", reserve=1, priority=1) as auth:
        async with httpx.AsyncClient() as client:
            auth.wrap_httpx(client)
            inner = client._transport._inner
            calls = {"n": 0}

            async def _handle(request):
                calls["n"] += 1
                if calls["n"] == 1:
                    return _make_response(httpx, 429, request)
                return _make_response(httpx, 200, request)

            monkeypatch.setattr(inner, "handle_async_request", _handle)
            r = await client.get("https://example.com")
            assert r.status_code == 200  # noqa: PLR2004
        monkeypatch.setattr(inner, "handle_async_request", inner.handle_async_request)
