"""Seeded 'dice rolls' for fault injection.

Every decision comes from hashing stable inputs and never from a shared
random number generator, so it doesn't matter which process asks first or
in what order. A call looks like this.

    roll(seed, "discover_host:host-4", attempt, call_no)

The same inputs always give the same number in [0, 1). A retry has a new
attempt number, so it gets a new roll.
"""

import hashlib

# The ways an HTTP call to the mock network can fail on purpose.
FAULT_KINDS = ("server_error", "rate_limited", "timeout", "empty_response")


def roll(seed: int, *parts: object) -> float:
    """A deterministic number in [0, 1) for these inputs.

    It uses sha256 and not Python's hash(), because hash() of a string changes
    on every process start (PYTHONHASHSEED), which would break reproducibility.
    """
    text = "|".join(str(p) for p in (seed, *parts))
    digest = hashlib.sha256(text.encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def pick_fault(r: float, fault_rate: float) -> str | None:
    """Map a roll to a fault kind, or None (no fault) for most rolls."""
    if r >= fault_rate:
        return None
    # Spread the faulty rolls evenly over the kinds.
    index = int(r / fault_rate * len(FAULT_KINDS))
    return FAULT_KINDS[min(index, len(FAULT_KINDS) - 1)]
