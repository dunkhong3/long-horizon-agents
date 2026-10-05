"""The research brief's rules, which are what the coordinator does with each kind of result.

A read must match its source word for word, because the coordinator parses
the cited raw text itself and the claims and citations reported must be
exactly the ones it finds. A reconciliation is worked out again from the
claim facts, so a project's year only becomes a verified answer when two or
more sources agree and outnumber every other year. The brief must list
exactly the verified answers.
"""

from collections import Counter
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.config import MAX_ROUNDS
from lha.core.domain import Rejected, cited_response
from lha.core.schemas import SUPERSEDED, VERIFIED
from lha.db import crud
from lha.db.models import facts
from lha.domains.research import facts as F
from lha.domains.research.tasks import BriefOutput, ReadInput, ReadOutput, ReconcileInput, ReconcileOutput
from lha.domains.research.world import Library, cites_in, claims_in

if TYPE_CHECKING:
    from lha.core.coordinator import Coordinator


async def accept_read(
    coord: "Coordinator", conn: AsyncConnection, task: Row, out: ReadOutput, library: Library
) -> None:
    inp = ReadInput(**task.input)
    r = cited_response(await coord.cited_responses(conn, task), out.event_id, "get_source")
    if out.source != inp.source or r["source"] != inp.source:
        raise Rejected(f"source mismatch: {out.source}")
    claims = [(c.project, c.year, c.quote) for c in out.claims]
    if claims != claims_in(r["text"]):
        raise Rejected("the claims don't match the source's text")
    if sorted(out.cites) != cites_in(r["text"], inp.source):
        raise Rejected(f"cites {out.cites} don't match the source's text")

    event = UUID(out.event_id)
    writes = [
        (F.source_subject(inp.source), F.READ, True, event),
        (F.source_subject(inp.source), F.CITES, out.cites, event),
        *((F.claim_subject(c.project, inp.source), F.YEAR, c.year, event) for c in out.claims),
    ]
    await crud.upsert_facts(conn, coord.sid, writes, source_task_id=task.id)

    for source in out.cites:
        await coord.create(conn, "read_source", ReadInput(source=source), task.id)
    # A project asked about gets reconciled again whenever it has a new
    # claim, from its second one on. Sources about the same project can be
    # read by different coordinators at once, so the count is taken under a
    # lock per project, otherwise two reads could each miss the other.
    for project in sorted({c.project for c in out.claims} & set(library.asked)):
        key = f"claims:{coord.sid}:{project}"
        await conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": key})
        n = len(await claim_years(coord, conn, project))
        if n >= 2 and not await crud.current_fact(conn, coord.sid, F.answer_subject(project), F.YEAR):
            await coord.create(conn, "reconcile_project", ReconcileInput(project=project, claims=n), task.id)


async def claim_years(coord: "Coordinator", conn: AsyncConnection, project: str) -> dict[str, Row]:
    """The current claims about a project, by fact id."""
    q = select(facts).where(
        facts.c.session_id == coord.sid,
        facts.c.subject.like(f"claim:{project}@%"),
        facts.c.key == F.YEAR,
        facts.c.status != SUPERSEDED,
    )
    return {str(r.id): r for r in (await conn.execute(q)).all()}


def settle(years: list[int]) -> int | None:
    """The year two or more sources agree on and that outnumbers every other year, or None."""
    ranked = Counter(years).most_common()
    if ranked and ranked[0][1] >= 2 and (len(ranked) == 1 or ranked[0][1] > ranked[1][1]):
        return ranked[0][0]
    return None


async def accept_reconcile(
    coord: "Coordinator", conn: AsyncConnection, task: Row, out: ReconcileOutput
) -> None:
    inp = ReconcileInput(**task.input)
    claims = await claim_years(coord, conn, inp.project)
    year = settle([c.value for c in claims.values()])
    # The coordinator settles it again from the facts, and the model's answer
    # has to be the same, with citations that back it.
    if out.project != inp.project or out.year != year:
        raise Rejected(f"claimed {out.year}, the claims settle on {year}")
    if year is None:
        return  # not settled yet, and a later claim will bring a new reconciliation
    cited = [claims.get(i) for i in out.claim_fact_ids]
    if len(cited) < 2 or any(c is None or c.value != year for c in cited) or len(set(out.claim_fact_ids)) < 2:
        raise Rejected("the answer must cite two or more agreeing claims")
    evidence = [{"fact_id": i, "year": year} for i in sorted(set(out.claim_fact_ids))]
    await crud.upsert_fact(
        conn, coord.sid, F.answer_subject(inp.project), F.YEAR, year,
        status=VERIFIED, evidence=evidence, source_task_id=task.id,
    )  # fmt: skip


async def accept_brief(coord: "Coordinator", conn: AsyncConnection, task: Row, out: BriefOutput) -> None:
    """The brief must list exactly the verified answers, each matching its fact."""
    q = select(facts.c.id).where(
        facts.c.session_id == coord.sid,
        facts.c.subject.like("answer:%"),
        facts.c.key == F.YEAR,
        facts.c.status == VERIFIED,
    )
    verified = {str(i) for i in (await conn.execute(q)).scalars()}
    cited = [a.fact_id for a in out.findings]
    if sorted(cited) != sorted(verified):
        raise Rejected(f"the brief cites {len(cited)} answer(s), {len(verified)} are verified")
    for answer in out.findings:
        fact = await coord.cited_fact(conn, answer.fact_id, F.answer_subject(answer.project), F.YEAR)
        if fact.value != answer.year:
            raise Rejected(f"the brief says {answer.year} for {answer.project}, the fact says {fact.value}")
    await coord.finish(
        conn, out.model_dump(mode="json"), succeeded=bool(out.findings) and not task.input["partial"]
    )


async def replan(
    coord: "Coordinator", conn: AsyncConnection, task: Row, kind: str, delay: float, probe: bool
) -> None:
    if task.type == "read_source":
        inp = ReadInput(**task.input)
        subject = F.source_subject(inp.source)
        if kind == "not_found":
            await crud.upsert_fact(conn, coord.sid, subject, F.READ, False, source_task_id=task.id)
            return await coord.decide(conn, task, "replan", "source does not exist; dropped")
        await crud.upsert_fact(conn, coord.sid, subject, F.UNREACHABLE, True, source_task_id=task.id)
        if inp.round < MAX_ROUNDS:
            nxt = ReadInput(source=inp.source, round=inp.round + 1)
            await coord.create(conn, task.type, nxt, task.id, delay, probe=probe)
            return await coord.decide(conn, task, "replan", f"new round {nxt.round} later")
    elif task.type == "reconcile_project":
        inp = ReconcileInput(**task.input)
        if inp.round < MAX_ROUNDS:
            nxt = inp.model_copy(update={"round": inp.round + 1})
            await coord.create(conn, task.type, nxt, task.id, delay)
            return await coord.decide(conn, task, "replan", f"new round {nxt.round} later")
    await coord.decide(conn, task, "replan", "out of rounds; given up")


async def goal_met(coord: "Coordinator", conn: AsyncConnection, library: Library) -> bool:
    """Every project asked about has a verified answer."""
    for project in library.asked:
        answer = await crud.current_fact(conn, coord.sid, F.answer_subject(project), F.YEAR)
        if answer is None or answer.status != VERIFIED:
            return False
    return True


async def reopen(coord: "Coordinator", conn: AsyncConnection, library: Library) -> int:
    """One more round for sources given up as unreachable, and one more
    reconciliation for projects asked about with two or more claims and no
    answer yet, where nothing is already working on them."""
    working = {(t, i.get("source"), i.get("project")) for t, i in await coord.active_tasks(conn)}
    facts_now = await coord.current_facts(conn)
    created = 0
    for subject in facts_now.get(F.UNREACHABLE, {}):
        source = subject.removeprefix("source:")
        if ("read_source", source, None) not in working and subject not in facts_now.get(F.READ, {}):
            if await coord.create(conn, "read_source", ReadInput(source=source, round=MAX_ROUNDS + 1), None):
                created += 1
    for project in library.asked:
        n = len(await claim_years(coord, conn, project))
        answered = F.answer_subject(project) in facts_now.get(F.YEAR, {})
        if n >= 2 and not answered and ("reconcile_project", None, project) not in working:
            inp = ReconcileInput(project=project, claims=n, round=MAX_ROUNDS + 1)
            if await coord.create(conn, "reconcile_project", inp, None):
                created += 1
    return created
