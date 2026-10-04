"""The ContextPacket, which is everything one model call is allowed to see.

It is built fresh from Postgres right before each call, used once, and
logged to `events` so we can always see exactly what an agent saw. The raw
event log itself never goes into a prompt.
"""

from typing import Any

from pydantic import BaseModel


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


class ContextPacket(BaseModel):
    # Layers, from highest to lowest priority.
    pinned: Pinned
    facts: list[FactView]
    recent: list[EventView]
    pointers: list[str]  # ids of raw tool-call events, payloads left out
    # What didn't fit, so the model knows the view is partial.
    omitted: dict[str, int]
    tokens: int  # estimated size of this packet

    def facts_with(self, key: str) -> list[FactView]:
        return [f for f in self.facts if f.key == key]
