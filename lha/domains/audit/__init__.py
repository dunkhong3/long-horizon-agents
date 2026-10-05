"""The replica audit, where agents explore a mock cloud deployment and find replica drift.

Its world is a network of hosts whose documents mention other hosts, and
the goal is to find the services that run a different number of replicas
than registry.json says, confirmed by independent reads. See 'The goal the
agents pursue' in docs/design.md.
"""

from typing import Any
from uuid import UUID

from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import func, literal, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.config import ALL_DRIFTS, DEFAULT_HOSTS
from lha.core.domain import Domain, TaskType
from lha.db import models
from lha.domains.audit import agents, rules
from lha.domains.audit import facts as F
from lha.domains.audit.app import create_app
from lha.domains.audit.tasks import (
    CompareInput,
    CompareOutput,
    DiscoverInput,
    DiscoverOutput,
    ReportInput,
    ReportOutput,
    VerifyInput,
    VerifyOutput,
    task_key,
    task_scope,
)
from lha.domains.audit.tools import AuditTools
from lha.domains.audit.world import START_HOSTS, World, generate_world


class AuditDomain(Domain):
    name = "audit"
    roles = ("discovery", "analysis", "reporter")
    task_types = {
        "discover_host": TaskType(
            DiscoverInput, DiscoverOutput, "discovery", agents.run_discover, network=True
        ),
        "compare_service": TaskType(CompareInput, CompareOutput, "analysis", agents.run_compare),
        "verify_drift": TaskType(
            VerifyInput, VerifyOutput, "analysis", agents.run_verify, network=True, rereads=True
        ),
        "write_report": TaskType(ReportInput, ReportOutput, "reporter", agents.run_report),
    }
    report_type = "write_report"
    default_size = DEFAULT_HOSTS
    goals = {
        "one": (
            "Audit the deployment. Exactly one service runs a different number of "
            "replicas than registry.json says. Find it, confirm it with independent "
            "reads, and write a finding backed by verified facts. You start knowing "
            "only these hosts: {start}."
        ),
        "all": (
            "Audit the deployment. Some services run a different number of replicas "
            "than registry.json says. Explore every host, give every service a "
            "verdict, confirm every drift with independent reads, and write a "
            "finding listing every verified drift. You start knowing only these "
            "hosts: {start}."
        ),
    }
    done = {
        "one": "goal met: a verified drift backed by the registry",
        "all": "goal met: every host explored and every service has a verdict",
    }

    def make_world(self, seed: int, size: int, goal_kind: str) -> World:
        return generate_world(seed, size, ALL_DRIFTS if goal_kind == "all" else 1)

    def create_app(self, world: World, fault_rate: float) -> FastAPI:
        return create_app(world, fault_rate)

    def tools(self, http, task_key, attempt, log, load) -> AuditTools:
        return AuditTools(http, task_key, attempt, log, load)

    def start(self, world: World) -> list[str]:
        return list(START_HOSTS)

    def start_tasks(self, world: World) -> list[tuple[str, BaseModel]]:
        return [("discover_host", DiscoverInput(host=host)) for host in START_HOSTS]

    def task_key(self, task_type: str, inp: BaseModel) -> str:
        return task_key(task_type, inp)

    def scope(self, task_type: str, inp: BaseModel) -> list[str]:
        return task_scope(inp)

    def resource(self, task_type: str, inp: BaseModel) -> str | None:
        return getattr(inp, "host", None)

    def report_input(self, partial: bool) -> ReportInput:
        return ReportInput(partial=partial)

    async def accept(self, coord, conn, task, out) -> None:
        await rules.ACCEPT[task.type](coord, conn, task, out)

    async def replan(self, coord, conn, task, kind, delay, probe) -> None:
        await rules.replan(coord, conn, task, kind, delay, probe)

    async def split(self, coord, conn, task, reason) -> bool:
        return await rules.split(coord, conn, task, reason)

    async def goal_met(self, coord, conn, session) -> bool:
        return await rules.goal_met(coord, conn, session)

    async def reopen(self, coord, conn) -> int:
        return await rules.reopen(coord, conn)

    def related(self, fact: Row) -> list[tuple[str, str]]:
        """The registry entry and latest read behind a verified drift, for the reporter."""
        service, _ = F.split_service_subject(fact.subject)
        return [(F.registry_subject(service), F.EXPECTED), (fact.subject, F.REPLICAS)]

    def pointer_wanted(self, task: Row, path: str) -> bool:
        """Whether a batch of a split discovery would read this path, so pointers
        for the other batches' reads don't take up its budget."""
        inp = task.input
        if inp.get("services") is None:
            return True  # not a batch, so it reads everything on its host
        host = inp["host"]
        if path == f"/hosts/{host}" or path in {f"/hosts/{host}/services/{s}" for s in inp["services"]}:
            return True
        for doc in inp.get("documents") or []:
            page = inp.get("page")
            prefix = f"/hosts/{host}/documents/{doc}?page="
            if path == f"{prefix}{page}" or (page is None and path.startswith(prefix)):
                return True
        return False

    async def score(self, conn: AsyncConnection, session: Row, world: World) -> list[tuple[str, bool]]:
        findings = (session.report or {}).get("findings", [])
        if session.goal_kind == "all":
            planted = {(s.name, s.host, s.expected, s.actual) for s in world.drifts}
            reported = {(f["service"], f["host"], f["expected"], f["actual"]) for f in findings}
            return [
                (f"found all {len(planted)} planted drifts", planted <= reported),
                ("no finding that isn't a planted drift", reported <= planted),
            ]
        return answer_checks(world, findings[0] if len(findings) == 1 else {})

    async def stats(self, conn: AsyncConnection, sid: UUID, world: World) -> dict[str, Any]:
        async def count(*where) -> int:
            q = select(func.count()).select_from(models.facts).where(models.facts.c.session_id == sid, *where)
            return (await conn.execute(q)).scalar_one()

        hosts = await count(
            models.facts.c.key == F.EXISTS,
            models.facts.c.value == literal(True, JSONB),
            models.facts.c.status != F.SUPERSEDED,
        )
        refuted = await count(models.facts.c.key == F.DRIFT, models.facts.c.status == F.REFUTED)
        return {
            "hosts_checked": f"{hosts}/{world.n_hosts}",
            "decoys_refuted": f"{refuted}/{len(world.decoys)}",
            "drifts_planted": len(world.drifts),
        }

    async def progress(self, conn: AsyncConnection, sid: UUID) -> str:
        q = (
            select(models.facts.c.status, func.count())
            .where(models.facts.c.session_id == sid, models.facts.c.key == F.DRIFT)
            .group_by(models.facts.c.status)
        )
        claims = dict((await conn.execute(q)).all())
        q = select(func.count()).where(
            models.facts.c.session_id == sid,
            models.facts.c.key == F.EXISTS,
            models.facts.c.value == literal(True, JSONB),
            models.facts.c.status != F.SUPERSEDED,
        )
        hosts = (await conn.execute(q)).scalar_one()
        return (
            f"hosts found: {hosts} | drift claims: {claims.get(F.INFERRED, 0)} inferred, "
            f"{claims.get(F.VERIFIED, 0)} verified, {claims.get(F.REFUTED, 0)} refuted"
        )


def answer_checks(world: World, finding: dict[str, Any]) -> list[tuple[str, bool]]:
    """Whether a finding names the planted drift, which is shared with the naive baseline."""
    drift = world.drift
    return [
        (f"found the drifted service ({drift.name})", finding.get("service") == drift.name),
        (f"on the right host ({drift.host})", finding.get("host") == drift.host),
        (
            f"right counts (expected {drift.expected}, actual {drift.actual})",
            (finding.get("expected"), finding.get("actual")) == (drift.expected, drift.actual),
        ),
    ]


AUDIT = AuditDomain()
