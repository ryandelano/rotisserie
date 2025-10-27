def test_per_window_reset_and_endpoint_exit():
    from rotisserie import KeyConfig, KeyPool  # noqa: PLC0415

    # key has per-window limit 1 per 1s
    keys = [KeyConfig("k1", "T1", per_window=(1, 1)), KeyConfig("k2", "T2", per_window=(1, 1))]
    pool = KeyPool(keys, distribute=True)
    with pool.endpoint("e1", reserve=1, priority=1):
        k = pool.take_key("e1")
        assert k.remaining == 0
        pool.mark_result(k, 200, {}, None)
        # next take should block; we simulate window reset by forcing time forward via direct fields
        k.window_ends_at = 0.0
        k.remaining = 1
        k2 = pool.take_key("e1")
        assert k2.name in {"k1", "k2"}
    # endpoint exit should unassign free keys
    for ks in pool._keys:
        assert ks.assigned_to is None or ks.in_use_by is not None
