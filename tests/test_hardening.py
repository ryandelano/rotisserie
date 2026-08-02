from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from rotisserie import AsyncKeyPool, KeyConfig, KeyPool, RetryConfig, load_keyconfigs_from_env

STATUS_OK = 200
RETRY_COUNT = 2
ERROR_CAP = 8.0
RETRY_BASE = 3.0
LEASE_COUNT = 2


def test_retry_config_is_not_replaced_by_pool_defaults():
    config = RetryConfig(
        retry_after_base=RETRY_BASE,
        retry_after_growth=1.0,
        retry_after_cap=9.0,
        error_base=4.0,
        error_growth=1.0,
        error_cap=ERROR_CAP,
        retry_attempts=RETRY_COUNT,
        retry_for_methods=["POST"],
    )
    pool = KeyPool([KeyConfig("k", "token")], retry_config=config)

    assert pool._retry_config.retry_after_base == RETRY_BASE
    assert pool._retry_config.error_cap == ERROR_CAP
    assert pool._max_attempts == RETRY_COUNT
    assert pool._retry_methods == {"POST"}
    with pool.auth("ep", reserve=1) as auth:
        assert auth.retry_attempts == RETRY_COUNT
        assert auth.retry_for_methods == {"POST"}


def test_from_tokens_supports_simple_lists_and_named_mappings():
    generated = KeyPool.from_tokens(["token-a", "token-b"])
    assert [key.name for key in generated._keys] == ["key_1", "key_2"]

    named = KeyPool.from_tokens({"primary": "token-a", "backup": "token-b"})
    assert [(key.name, key.token) for key in named._keys] == [
        ("primary", "token-a"),
        ("backup", "token-b"),
    ]


def test_unknown_pool_options_fail_with_an_actionable_error():
    with pytest.raises(TypeError, match="unexpected pool option"):
        KeyPool.from_tokens(["token"], distrubute=True)


def test_sync_httpx_auth_supports_query_credentials():
    pool = KeyPool([KeyConfig("k", "token")])

    def handler(request):
        assert request.url.params["api_key"] == "token"
        assert "Authorization" not in request.headers
        return httpx.Response(STATUS_OK, request=request)

    with pool.auth("ep", reserve=1, auth_in="query") as auth, httpx.Client(
        auth=auth, transport=httpx.MockTransport(handler)
    ) as client:
        assert client.get("https://example.com?existing=value").status_code == STATUS_OK


@pytest.mark.asyncio
async def test_async_httpx_auth_supports_query_credentials():
    pool = AsyncKeyPool([KeyConfig("k", "token")])

    async def handler(request):
        assert request.url.params["api_key"] == "token"
        assert "Authorization" not in request.headers
        return httpx.Response(STATUS_OK, request=request)

    async with pool.auth("ep", reserve=1, auth_in="query") as auth, httpx.AsyncClient(
        auth=auth, transport=httpx.MockTransport(handler)
    ) as client:
        response = await client.get("https://example.com?existing=value")
        assert response.status_code == STATUS_OK


@pytest.mark.asyncio
async def test_async_endpoint_alias_matches_sync_pool_api():
    pool = AsyncKeyPool.from_tokens(["token"])

    async with pool.endpoint("ep", reserve=1) as client:
        key = await pool.take_key(client.endpoint)
        pool.mark_result(key, STATUS_OK, {}, None)

    assert pool._endpoint_refcounts == {}


def test_requests_retry_preserves_headers_and_params():
    pool = KeyPool(
        [KeyConfig("k1", "t1"), KeyConfig("k2", "t2")],
        retry_config=RetryConfig(retry_attempts=RETRY_COUNT, retry_after_cap=0),
    )
    session = MagicMock()
    first = MagicMock(status_code=429, headers={})
    second = MagicMock(status_code=200, headers={})
    session.request.side_effect = [first, second]

    with pool.requests_client("ep", reserve=1, session=session) as client:
        response = client.get(
            "https://example.com",
            headers={"X-Caller": "yes"},
            params={"page": "2"},
        )

    assert response is second
    assert session.request.call_count == RETRY_COUNT
    for call in session.request.call_args_list:
        assert call.kwargs["headers"]["X-Caller"] == "yes"
        assert call.kwargs["params"]["page"] == "2"


def test_requests_does_not_retry_non_idempotent_method_by_default():
    pool = KeyPool([KeyConfig("k", "token")], retry_attempts=3)
    session = MagicMock()
    response = MagicMock(status_code=429, headers={})
    session.request.return_value = response

    with pool.requests_client("ep", reserve=1, session=session) as client:
        assert client.post("https://example.com") is response

    assert session.request.call_count == 1


@pytest.mark.asyncio
async def test_httpx_transport_error_releases_key():
    pool = AsyncKeyPool([KeyConfig("k", "token")], retry_attempts=1)
    client = AsyncMock()
    client.request.side_effect = httpx.ConnectError("offline")

    with pytest.raises(httpx.ConnectError):
        async with pool.httpx_client("ep", reserve=1, client=client) as wrapper:
            await wrapper.get("https://example.com")

    assert all(key.in_use_by is None for key in pool._keys)


@pytest.mark.asyncio
async def test_aiohttp_wrapper_accepts_synchronous_release():
    class Response:
        status = 200
        headers = {}
        closed = False

        def release(self):
            self.closed = True

    pool = AsyncKeyPool([KeyConfig("k", "token")])
    session = AsyncMock()
    response = Response()
    session.request.return_value = response

    async with pool.aiohttp_client("ep", reserve=1, session=session) as client, client.get(
        "https://example.com"
    ) as actual:
        assert actual is response

    assert response.closed
    assert all(key.in_use_by is None for key in pool._keys)


def test_empty_and_closed_pools_fail_instead_of_waiting_forever():
    empty = KeyPool([])
    with pytest.raises(RuntimeError, match="empty pool"):
        empty.take_key("ep")

    closed = KeyPool([KeyConfig("k", "token")])
    closed.close()
    with pytest.raises(RuntimeError, match="closed"):
        closed.take_key("ep")

    pool = KeyPool([KeyConfig("k", "token")])
    with pytest.raises(ValueError, match="reserve"):
        pool.endpoint("ep", reserve=-1)


def test_nested_same_endpoint_leases_are_reference_counted():
    pool = KeyPool([KeyConfig("k", "token")])
    first = pool.endpoint("ep", reserve=1, priority=1)
    second = pool.endpoint("ep", reserve=1, priority=1)

    with first:
        with second:
            assert pool._endpoint_refcounts["ep"] == LEASE_COUNT
        assert "ep" in pool._endpoints
    assert "ep" not in pool._endpoints


def test_env_loader_deduplicates_explicit_and_prefix_matches(monkeypatch):
    monkeypatch.setenv("API_KEY", "token")

    configs = load_keyconfigs_from_env(names="API_KEY", prefix="API_KEY")
    assert [(config.name, config.token) for config in configs] == [("API_KEY", "token")]


def test_wrapped_httpx_transport_error_releases_key():
    pool = KeyPool(
        [KeyConfig("k", "token")],
        retry_config=RetryConfig(retry_attempts=1),
    )
    with pool.auth("ep", reserve=1) as auth, httpx.Client() as client:
        auth.wrap_httpx(client)
        inner = client._transport._inner

        def fail(_request):
            raise httpx.ConnectError("offline")

        inner.handle_request = fail
        with pytest.raises(httpx.ConnectError):
            client.get("https://example.com")

    assert all(key.in_use_by is None for key in pool._keys)


def test_header_only_auth_does_not_leave_key_in_use():
    pool = KeyPool([KeyConfig("k", "token")])
    with pool.auth("ep", reserve=1) as auth:
        assert auth.headers()["Authorization"] == "Bearer token"
        assert pool._keys[0].in_use_by is None


def test_aiohttp_trace_config_rejects_unsupported_query_injection():
    pool = AsyncKeyPool([KeyConfig("k", "token")])
    auth = pool.auth("ep", reserve=1, auth_in="query")

    with pytest.raises(RuntimeError, match="TraceConfig cannot add query"):
        auth.trace_config()
