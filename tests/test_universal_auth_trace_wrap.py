import pytest


def test_trace_config_injects_headers(monkeypatch):
    from rotisserie import AsyncKeyPool, KeyConfig  # noqa: PLC0415

    pool = AsyncKeyPool([KeyConfig("k1", "T1")])
    auth = pool.auth(endpoint="ep", reserve=1, priority=1)
    tc = auth.trace_config()
    assert tc is not None


@pytest.mark.asyncio
async def test_wrap_session_retries_on_429(monkeypatch):
    import aiohttp  # noqa: PLC0415

    from rotisserie import AsyncKeyPool, KeyConfig  # noqa: PLC0415

    pool = AsyncKeyPool([KeyConfig("k1", "T1"), KeyConfig("k2", "T2")])
    async with (
        pool.auth(endpoint="ep", reserve=1, priority=1) as auth,
        aiohttp.ClientSession() as s,
    ):
        auth.wrap_session(s)

        class FakeResp:
            def __init__(self, status):
                self.status = status
                self.headers = {}
                self.closed = False

            async def release(self):
                self.closed = True

        calls = {"n": 0}

        async def fake_request(method, url, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                return FakeResp(429)
            return FakeResp(200)

        monkeypatch.setattr(s, "_rotisserie_orig_request", fake_request, raising=False)
        resp = await s._request("GET", "https://example.com")
        assert resp.status == 200  # noqa: PLR2004
