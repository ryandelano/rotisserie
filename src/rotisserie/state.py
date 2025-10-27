from dataclasses import dataclass
from typing import Union


@dataclass
class KeyState:
    name: str
    token: str
    remaining: Union[int, None] = None
    window_ends_at: float = 0.0
    cooldown_until: float = 0.0
    in_use_by: Union[str, None] = None  # endpoint currently using the key
    assigned_to: Union[str, None] = None  # soft assignment preference
    successes: int = 0
    failures: int = 0

    def next_available_at(self, now: float) -> float:
        w = self.window_ends_at if (self.remaining is not None and self.remaining <= 0) else now
        return max(now, self.cooldown_until, w)
