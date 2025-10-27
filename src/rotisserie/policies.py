import inspect
import random
import time
from typing import Callable, Union

from .state import KeyState

# Defaults used when inspect.signature cannot determine argument counts
DEFAULT_SELECTION_ARGC = 3  # select_fn(available, n, endpoint)
DEFAULT_KEYFN_ARGC = 2  # key_fn(key, endpoint)
DEFAULT_COERCE_ARGC = 1  # assume key function: key_fn(key)

# Thresholds for dispatch decisions
SELECTION_WITH_ENDPOINT_ARGC = 3  # selection fns receive endpoint at 3+ args
KEYFN_WITH_ENDPOINT_ARGC = 2  # key fns receive endpoint at 2+ args
COERCE_SELECTION_MIN_ARGC = 2  # treat callable as selection fn if it has >=2 args


def _count_positional_args(fn, default: int) -> int:
    """Return count of positional params for fn; fall back to default on failure."""
    try:
        sig = inspect.signature(fn)
        return len(
            [
                p
                for p in sig.parameters.values()
                if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            ]
        )
    except (TypeError, ValueError):
        return default


class AllocationPolicy:
    """Decides which *keys* (not endpoints) to prefer when multiple are eligible."""

    def select_keys(self, available: list[KeyState], n: int, endpoint: str) -> list[KeyState]:
        random.shuffle(available)
        return available[:n]


class EvenSplitPolicy(AllocationPolicy):
    pass


class WorkWeightedPolicy(AllocationPolicy):
    def select_keys(self, available: list[KeyState], n: int, endpoint: str) -> list[KeyState]:
        ranked = sorted(available, key=lambda k: (k.successes, k.next_available_at(time.time())))
        return ranked[:n]


class FunctionalPolicy(AllocationPolicy):
    """Wrap a user-supplied selection function into an AllocationPolicy.

    Accepted function signatures:
    query:
        - select_fn(available, n, endpoint) -> list[KeyState]
        - select_fn(available, n) -> list[KeyState]
    """

    def __init__(self, select_fn: Callable):
        self.select_fn = select_fn

    def select_keys(self, available, n, endpoint):
        argc = _count_positional_args(self.select_fn, DEFAULT_SELECTION_ARGC)
        if argc >= SELECTION_WITH_ENDPOINT_ARGC:
            selected = self.select_fn(available, n, endpoint)
        else:
            selected = self.select_fn(available, n)
        if not isinstance(selected, list):
            raise TypeError("Custom select function must return a List[KeyState]")
        if len(selected) > n:
            raise ValueError("Custom select function returned more keys than requested")
        if any(k not in available for k in selected):
            raise ValueError("Custom select function returned keys not in 'available'")
        return selected


class KeyFunctionPolicy(AllocationPolicy):
    """Wrap a user-supplied key function for ranking available keys.

    Accepted function signatures:
        - key_fn(key) -> comparable
        - key_fn(key, endpoint) -> comparable
    """

    def __init__(self, key_fn: Callable):
        self.key_fn = key_fn

    def select_keys(self, available, n, endpoint):
        argc = _count_positional_args(self.key_fn, DEFAULT_KEYFN_ARGC)

        def _score(k):
            return self.key_fn(k, endpoint) if argc >= KEYFN_WITH_ENDPOINT_ARGC else self.key_fn(k)

        ranked = sorted(available, key=_score)
        return ranked[:n]


def coerce_policy(policy: Union[object, None]) -> AllocationPolicy:
    """Turn None | str | AllocationPolicy | callable into an AllocationPolicy.

    Accepted inputs:
      - None        -> EvenSplitPolicy
      - "even"     -> EvenSplitPolicy
      - "weighted" -> WorkWeightedPolicy
      - AllocationPolicy instance (returned as-is)
      - callable: either a selection function (available, n, [endpoint]) or a key function
        (key[, endpoint]); wrapped into FunctionalPolicy or KeyFunctionPolicy.
    """
    if policy is None:
        return EvenSplitPolicy()
    if isinstance(policy, AllocationPolicy):
        return policy
    if isinstance(policy, str):
        name = policy.lower()
        if name == "even":
            return EvenSplitPolicy()
        if name == "weighted":
            return WorkWeightedPolicy()
        raise ValueError(
            "Unknown policy string. Use 'even' or 'weighted', or pass a callable/AllocationPolicy."
        )
    if callable(policy):
        argc = _count_positional_args(policy, DEFAULT_COERCE_ARGC)
        return (
            FunctionalPolicy(policy)
            if argc >= COERCE_SELECTION_MIN_ARGC
            else KeyFunctionPolicy(policy)
        )
    raise TypeError("policy must be None, 'even'|'weighted', AllocationPolicy, or a callable")
