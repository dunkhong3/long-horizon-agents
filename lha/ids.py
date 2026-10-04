"""UUIDv7 ids, which are globally unique and sort by creation time.

Python 3.11 has no uuid7() (it arrives in 3.14) and Postgres 16 has no
built-in v7, so we build one from 48 bits of Unix milliseconds, then the
version and variant bits, then random bits (RFC 9562, section 5.7).
"""

import os
import time
import uuid


def uuid7() -> uuid.UUID:
    unix_ms = time.time_ns() // 1_000_000
    rand = int.from_bytes(os.urandom(10), "big")  # 80 random bits
    rand_a = rand >> 68  # 12 bits
    rand_b = rand & ((1 << 62) - 1)  # 62 bits
    value = (
        (unix_ms & ((1 << 48) - 1)) << 80
        | 0x7 << 76  # version 7
        | rand_a << 64
        | 0b10 << 62  # RFC 4122 variant
        | rand_b
    )
    return uuid.UUID(int=value)
