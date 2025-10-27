from collections import Counter

from rotisserie import KeyConfig, KeyPool


def _assigned_counts(pool):
    # Count soft assignments among free keys
    names = [
        k.assigned_to
        for k in pool._keys
        if k.assigned_to is not None and k.in_use_by is None
    ]
    return Counter(names)


def test_distribution_round_robin_across_priorities():
    # 6 keys, 3 endpoints with priorities 1,1,2 and reserve=1 each
    keys = [KeyConfig(f"k{i}", f"T{i}") for i in range(6)]
    pool = KeyPool(keys, distribute=True)
    with (
        pool.endpoint("e1", reserve=1, priority=1),
        pool.endpoint("e2", reserve=1, priority=1),
        pool.endpoint("e3", reserve=1, priority=2),
    ):
        counts = _assigned_counts(pool)
        # Expect relatively even: each gets 2 in this setup (1 reserve + 1 extra)
        assert counts["e1"] == 2  # noqa: PLR2004
        assert counts["e2"] == 2  # noqa: PLR2004
        assert counts["e3"] == 2  # noqa: PLR2004
    pool.close()


def test_distribution_funnel_highest_priority_first():
    # 6 keys, same endpoints but distribute=False funnels extras to highest priority first
    keys = [KeyConfig(f"k{i}", f"T{i}") for i in range(6)]
    pool = KeyPool(keys, distribute=False)
    with (
        pool.endpoint("e1", reserve=1, priority=1),
        pool.endpoint("e2", reserve=1, priority=1),
        pool.endpoint("e3", reserve=1, priority=2),
    ):
        counts = _assigned_counts(pool)
        # Reserves: e1=1, e2=1, e3=1; extras (3) funneled to first P1 endpoint (e1)
        assert counts["e1"] == 4  # noqa: PLR2004
        assert counts["e2"] == 1  # noqa: PLR2004
        assert counts["e3"] == 1  # noqa: PLR2004
    pool.close()
