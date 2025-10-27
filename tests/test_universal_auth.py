from unittest.mock import MagicMock

import pytest

from rotisserie import AsyncKeyPool, KeyConfig, KeyPool


def test_requests_universal_auth_header_injection(monkeypatch):
    import requests  # noqa: PLC0415

    pool = KeyPool([KeyConfig("k", "T")])
    sess = MagicMock(spec=requests.Session)
    resp = MagicMock()
    resp.status_code = 200
    resp.headers = {}
    # ensure connection.send used by response hook path exists
    resp.connection = MagicMock()
    resp.connection.send.return_value = resp

    def _request(method, url, **kwargs):
        r = requests.Request(method=method, url=url, headers=kwargs.get("headers"))
        prep = requests.Session().prepare_request(r)
        prep.headers.update(kwargs.get("headers", {}))
        resp.request = prep
        return resp

    sess.request.side_effect = _request

    with pool.auth(endpoint="ep", reserve=1, priority=1) as auth:
        r = requests.get("https://example.com", auth=auth, hooks={})
        assert r.status_code == 200  # noqa: PLR2004


def test_httpx_sync_auth_flow_injection(monkeypatch):
    import httpx  # noqa: PLC0415

    pool = KeyPool([KeyConfig("k", "T")])
    with pool.auth(endpoint="ep", reserve=1, priority=1) as auth, httpx.Client(auth=auth) as client:  # noqa: E501
        # transport is real; request will fail without network, so mock send
        orig = client._transport.handle_request

        def _handle(request):
            assert request.headers.get("Authorization", "").endswith(" T")
            return httpx.Response(200, request=request)

        monkeypatch.setattr(client._transport, "handle_request", _handle)
        resp = client.get("https://example.com")
        assert resp.status_code == 200  # noqa: PLR2004
        # restore
        monkeypatch.setattr(client._transport, "handle_request", orig)


@pytest.mark.asyncio
async def test_httpx_async_auth_flow_injection(monkeypatch):
    import httpx  # noqa: PLC0415

    pool = AsyncKeyPool([KeyConfig("k", "T")])
    async with (
        pool.auth(endpoint="ep", reserve=1, priority=1) as auth,
        httpx.AsyncClient(auth=auth) as client,
    ):  # noqa: E501
        orig = client._transport.handle_async_request

        async def _handle(request):
            assert request.headers.get("Authorization", "").endswith(" T")
            return httpx.Response(200, request=request)

        monkeypatch.setattr(client._transport, "handle_async_request", _handle)
        resp = await client.get("https://example.com")
        assert resp.status_code == 200  # noqa: PLR2004
        monkeypatch.setattr(client._transport, "handle_async_request", orig)
