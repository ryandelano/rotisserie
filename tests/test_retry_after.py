from rotisserie import KeyConfig, KeyPool


def test_retry_after_seconds(monkeypatch):
    pool = KeyPool([KeyConfig("k", "t")])
    with pool.endpoint("ep", reserve=1, priority=1) as _client:
        key = pool.take_key("ep")
        pool.mark_result(key, 429, {"Retry-After": "2"}, None)
        # next acquisition should sleep at least ~2s; we just ensure cooldown is set
        assert key.cooldown_until >= pool._now() + 1.5
    pool.close()


def test_retry_after_http_date(monkeypatch):
    pool = KeyPool([KeyConfig("k", "t")])
    with pool.endpoint("ep", reserve=1, priority=1) as _client:
        key = pool.take_key("ep")
        # a date 2 seconds in the future
        import email.utils  # noqa: PLC0415
        import time as t  # noqa: PLC0415

        future = email.utils.formatdate(t.time() + 2, usegmt=True)
        pool.mark_result(key, 429, {"Retry-After": future}, None)
        assert key.cooldown_until >= pool._now() + 1.5
    pool.close()
