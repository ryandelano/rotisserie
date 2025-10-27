import time

from rotisserie import EvenSplitPolicy, WorkWeightedPolicy, coerce_policy


def test_string_policies():
    assert isinstance(coerce_policy("even"), EvenSplitPolicy)
    assert isinstance(coerce_policy("weighted"), WorkWeightedPolicy)


def test_callable_selection_policy():
    class K:
        def __init__(self, name, when, rem=1):
            self.name = name
            self._when = when
            self.remaining = rem
            self.in_use_by = None
            self.failures = 0
            self.successes = 0

        def next_available_at(self, now):
            return self._when

    def pick(available, n, endpoint):
        now = time.time()
        return sorted(available, key=lambda k: k.next_available_at(now))[:n]

    pol = coerce_policy(pick)
    keys = [K("a", 5), K("b", 1), K("c", 3)]
    chosen = pol.select_keys(keys, 1, "ep")
    assert chosen[0].name == "b"
