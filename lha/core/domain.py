"""What a domain plugs into the core, which is everything that knows what the agents are doing.

The core (the coordinator loop, the task queue with its leases and fencing,
the context packet builder, the worker process, retries, splits, the circuit
breaker, stall detection and the supervisor) knows nothing about networks or
documents. A domain supplies the rest, which is its mock world, its task
types with their schemas, roles and agents, its tools, and the rules the
coordinator follows for its results. Two domains live in lha/domains, the
replica audit and the research brief, and they share every line of the core.
"""

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.core.tools import EventLogger, PointerLoader, ToolBox
from lha.db import crud

if TYPE_CHECKING:
    from lha.core.agent import AgentContext
    from lha.core.coordinator import Coordinator


class Rejected(Exception):
    """A result failed the coordinator's checks (for example a fact doesn't match its source)."""


def cited_response(cited: dict[str, dict], event_id: str, tool: str) -> dict:
    """The response of a successful tool call from this attempt, or Rejected.

    `cited` is what Coordinator.cited_responses returned for the attempt.
    """
    payload = cited.get(event_id)
    if payload is None or payload.get("tool") != tool:
        raise Rejected(f"{tool} event {event_id} is not a successful call from this attempt")
    return payload["response"]


@dataclass(frozen=True)
class TaskType:
    input: type[BaseModel]
    output: type[BaseModel]
    role: str
    agent: Callable[["AgentContext"], Awaitable[BaseModel]]
    network: bool = False  # whether its failures count against its resource's circuit breaker
    rereads: bool = False  # whether it reads again on purpose, so its reads are never 'repeated'


class Domain:
    """The base class every domain fills in. The defaults are what a domain
    gets when it has no use for a mechanism, such as splitting."""

    name: str
    roles: tuple[str, ...]  # worker roles, one process or more each
    task_types: dict[str, TaskType]
    report_type: str  # the task type that writes the final report
    default_size: int  # the size of the world when --size isn't given
    goals: dict[str, str]  # goal kind -> goal text, with {start} for the starting points
    done: dict[str, str]  # goal kind -> what the coordinator says when the goal is met

    # --- the world ----------------------------------------------------------

    def make_world(self, seed: int, size: int, goal_kind: str) -> Any:
        raise NotImplementedError

    def create_app(self, world: Any, fault_rate: float) -> FastAPI:
        raise NotImplementedError

    def tools(
        self, http: httpx.AsyncClient, task_key: str, attempt: int, log: EventLogger, load: PointerLoader
    ) -> ToolBox:
        return ToolBox(http, task_key, attempt, log, load)

    # --- tasks ----------------------------------------------------------------

    def start(self, world: Any) -> list[str]:
        """The starting points named in the goal, such as the starting hosts."""
        raise NotImplementedError

    def goal_args(self, world: Any) -> dict[str, str]:
        """Anything else the goal text names, besides the starting points."""
        return {}

    def start_tasks(self, world: Any) -> list[tuple[str, BaseModel]]:
        raise NotImplementedError

    def task_key(self, task_type: str, inp: BaseModel) -> str:
        raise NotImplementedError

    def scope(self, task_type: str, inp: BaseModel) -> list[str]:
        raise NotImplementedError

    def resource(self, task_type: str, inp: BaseModel) -> str | None:
        """What a task works on, such as a host. Tasks on one resource share a
        circuit breaker and a coordinator partition, and None means neither."""
        return None

    def report_input(self, partial: bool) -> BaseModel:
        raise NotImplementedError

    # --- coordinator rules ------------------------------------------------------

    async def accept(self, coord: "Coordinator", conn: AsyncConnection, task: Row, out: BaseModel) -> None:
        """Check a valid result against its sources and commit its facts and
        follow-ups, or raise Rejected."""
        raise NotImplementedError

    async def replan(
        self, coord: "Coordinator", conn: AsyncConnection, task: Row, kind: str, delay: float, probe: bool
    ) -> None:
        """What to do about a task that failed for good."""
        await coord.decide(conn, task, "replan", "out of rounds; given up")

    async def split(self, coord: "Coordinator", conn: AsyncConnection, task: Row, reason: str) -> bool:
        """Split a task too big for one attempt into smaller ones, if it can be."""
        return False

    async def goal_met(self, coord: "Coordinator", conn: AsyncConnection, session: Row) -> bool:
        raise NotImplementedError

    async def reopen(self, coord: "Coordinator", conn: AsyncConnection) -> int:
        """Give work that was put off one more round, and return how many tasks that made."""
        return 0

    # --- context ---------------------------------------------------------------

    def related(self, fact: Row) -> list[tuple[str, str]]:
        """(subject, key) pairs that go into a packet along with a fact matched
        by a 'key:' scope entry, such as the registry entry behind a drift."""
        return []

    def pointer_wanted(self, task: Row, path: str) -> bool:
        """Whether a raw output at this path is worth pointing a task at."""
        return True

    # --- scoring ---------------------------------------------------------------

    async def score(self, conn: AsyncConnection, session: Row, world: Any) -> list[tuple[str, bool]]:
        raise NotImplementedError

    async def stats(self, conn: AsyncConnection, sid: UUID, world: Any) -> dict[str, Any]:
        return {}

    async def progress(self, conn: AsyncConnection, sid: UUID) -> str:
        return ""


def task_partition(resource: str | None, partitions: int) -> int:
    """Which coordinator decides a task's results, out of `partitions`.

    Tasks are split by resource, with a stable hash so every process agrees,
    which keeps every fact about a resource with one coordinator. Tasks
    without a resource, such as the report, go to the leader, partition 0.
    """
    if partitions <= 1 or resource is None:
        return 0
    return int.from_bytes(hashlib.sha256(resource.encode()).digest()[:4], "big") % partitions


async def create_task(
    conn: AsyncConnection,
    session_id: UUID,
    domain: Domain,
    task_type: str,
    inp: BaseModel,
    parent_task_id: UUID | None = None,
    delay_seconds: float = 0.0,
    partitions: int = 1,
) -> UUID | None:
    """Create a task with everything the domain says about it (see crud.create_task)."""
    resource = domain.resource(task_type, inp)
    return await crud.create_task(
        conn,
        session_id,
        task_type,
        inp.model_dump(),
        key=domain.task_key(task_type, inp),
        role=domain.task_types[task_type].role,
        scope=domain.scope(task_type, inp),
        resource=resource,
        partition=task_partition(resource, partitions),
        parent_task_id=parent_task_id,
        delay_seconds=delay_seconds,
    )


def get_domain(name: str) -> Domain:
    from lha.domains import DOMAINS

    return DOMAINS[name]
