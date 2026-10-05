"""The ContextPacket, which is everything one task attempt is allowed to see.

It is built fresh from Postgres at the start of each attempt, used once, and
logged to `events` so we can always see exactly what an agent saw. The raw
event log itself never goes into a prompt.
"""

from typing import Any

from pydantic import BaseModel

# Fact statuses, the same in every domain.
OBSERVED = "observed"  # read from a tool response
INFERRED = "inferred"  # a claim that is not confirmed yet
VERIFIED = "verified"  # confirmed by reads that agree
REFUTED = "refuted"  # checked and found wrong
SUPERSEDED = "superseded"  # replaced by a newer row for the same (subject, key)


class Pinned(BaseModel):
    """Always included, never cut."""

    goal: str
    task_type: str
    task_key: str
    attempt: int
    input: dict[str, Any]


class FactView(BaseModel):
    id: str
    subject: str
    key: str
    value: Any
    status: str


class EventView(BaseModel):
    """A short summary of an earlier event of this task (no raw payloads)."""

    kind: str
    attempt: int | None
    summary: str


class PointerView(BaseModel):
    """Where an earlier raw tool output is, and what it was, without the output itself."""

    id: str  # the tool_call event
    tool: str
    path: str


class ContextPacket(BaseModel):
    # Layers, from highest to lowest priority.
    pinned: Pinned
    facts: list[FactView]
    recent: list[EventView]
    pointers: list[PointerView]  # earlier raw tool outputs, payloads left out
    # What didn't fit, so the model knows the view is partial.
    omitted: dict[str, int]
    tokens: int  # estimated size of this packet
