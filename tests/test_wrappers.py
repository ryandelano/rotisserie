from rotisserie import KeyConfig, KeyPool


def test_take_key_and_mark_result_success_counts():
    pool = KeyPool([KeyConfig("a", "A"), KeyConfig("b", "B")])
    with pool.endpoint("x", reserve=1, priority=1):
        k = pool.take_key("x")
        assert k.in_use_by == "x"
        pool.mark_result(k, 200, {}, None)
        assert k.in_use_by is None
        assert k.successes >= 1
    pool.close()
