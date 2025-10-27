from unittest.mock import MagicMock

from rotisserie import KeyConfig, KeyPool


def test_requests_injection_header(monkeypatch):
    pool = KeyPool([KeyConfig("k", "T")], auth_header="X-Auth", auth_scheme="Token")
    _req = MagicMock()
    sess = MagicMock()
    # simulate Response object
    resp = MagicMock()
    resp.status_code = 200
    sess.request.return_value = resp

    with pool.requests_client("ep", reserve=1, priority=1) as client:
        r = client.get("https://example.com", session=sess)
        assert r is resp
        # ensure header injected
        args, kwargs = sess.request.call_args
        assert kwargs["headers"]["X-Auth"].startswith("Token ")


def test_requests_injection_query(monkeypatch):
    pool = KeyPool([KeyConfig("k", "T")], auth_in="query", auth_query_param="api_key")
    sess = MagicMock()
    resp = MagicMock()
    resp.status_code = 200
    sess.request.return_value = resp
    with pool.requests_client("ep", reserve=1, priority=1) as client:
        _ = client.get("https://example.com", session=sess)
        args, kwargs = sess.request.call_args
        assert kwargs["params"]["api_key"] == "T"
