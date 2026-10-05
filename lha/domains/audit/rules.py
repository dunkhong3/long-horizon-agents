"""The audit's rules, which are what the coordinator does with each kind of audit result.

Each accept handler checks a result against the raw responses or facts it
cites, raises Rejected if anything doesn't match, and otherwise writes the
facts and creates the follow-up tasks, all inside the coordinator's
transaction for that result. The rest are the audit's answers to the core's
questions, which are how to replan a task that failed for good, how to split
one that is too big, when the goal is met, and what to re-open after a stall.
"""

import re
from collections import Counter
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.config import MAX_ROUNDS, MAX_VERIFY_ROUNDS, SPLIT_BATCH
from lha.core.domain import Rejected, cited_response
from lha.db import crud
from lha.db.models import facts
from lha.domains.audit import facts as F
from lha.domains.audit.tasks import (
    CompareInput,
    CompareOutput,
    DiscoverInput,
    DiscoverOutput,
    ReportOutput,
    VerifyInput,
    VerifyOutput,
)
from lha.domains.audit.world import REGISTRY_DOC, registry_entries

if TYPE_CHECKING:
    from lha.core.coordinator import Coordinator


# --- accepting results --------------------------------------------------------------


async def accept_discover(
    coord: "Coordinator", conn: AsyncConnection, task: Row, out: DiscoverOutput
) -> None:
    inp = DiscoverInput(**task.input)
    cited = await coord.cited_responses(conn, task)

    # 1. Check every claim against the tool response it cites, and that
    # nothing this task was meant to read is missing.
    host_resp = cited_response(cited, out.host_event_id, "get_host")
    if out.host != inp.host or host_resp["host"] != inp.host:
        raise Rejected(f"host mismatch: {out.host}")
    wanted = inp.services if inp.services is not None else host_resp["services"]
    if sorted(s.service for s in out.services) != sorted(wanted):
        raise Rejected(f"services {[s.service for s in out.services]}, expected {wanted}")
    for s in out.services:
        r = cited_response(cited, s.event_id, "get_service")
        if (r["host"], r["service"], r["replicas"]) != (inp.host, s.service, s.replicas):
            raise Rejected(f"{s.service}: claimed {s.replicas}, response says {r['replicas']}")
    for d in out.documents:
        r = cited_response(cited, d.event_id, "fetch_document")
        named = set(re.findall(r"\bhost-\d+\b", r["content"]))
        if (r["host"], r["name"], r["page"], r["pages"]) != (inp.host, d.name, d.page, d.pages):
            raise Rejected(f"{d.name} page {d.page}: doesn't match the response it cites")
        if not set(d.mentions) <= named:
            raise Rejected(f"{d.name}: mentions {d.mentions} not all in the document")
        if d.name == REGISTRY_DOC and d.registry != registry_entries(r["content"]):
            raise Rejected("registry doesn't match registry.json")
    read = {(d.name, d.page) for d in out.documents}
    expected_pages = pages_to_read(inp, host_resp["documents"], out.documents)
    if read != expected_pages:
        raise Rejected(f"document pages {sorted(read)}, expected {sorted(expected_pages)}")

    # 2. Write facts, all in one batch.
    sid, tid = coord.sid, task.id
    host_event = UUID(out.host_event_id)
    listing = {"services": host_resp["services"], "documents": host_resp["documents"]}
    writes = [
        (F.host_subject(inp.host), F.EXISTS, True, host_event),
        (F.host_subject(inp.host), F.LISTING, listing, host_event),
    ]
    for s in out.services:
        writes.append((F.service_subject(s.service, inp.host), F.REPLICAS, s.replicas, UUID(s.event_id)))
    registry_found = False
    for d in out.documents:
        subject = F.doc_subject(d.name, inp.host, d.page)
        writes.append((subject, F.MENTIONS, d.mentions, UUID(d.event_id)))
        if d.page == 0:
            writes.append((subject, F.PAGES, d.pages, UUID(d.event_id)))
        for service, expected in (d.registry or {}).items():
            registry_found = True
            writes.append((F.registry_subject(service), F.EXPECTED, expected, UUID(d.event_id)))
    await crud.upsert_facts(conn, sid, writes, source_task_id=tid)

    # 3. Follow-ups. Creating a task that already exists does nothing.
    await registry_edge(coord, conn, exclusive=registry_found)
    for d in out.documents:
        for host in d.mentions:
            await coord.create(conn, "discover_host", DiscoverInput(host=host), tid)
    # A compare needs both the read and the registry entry, and when part of
    # the registry arrives, the services read before it get compared now.
    in_registry = await current_subjects(coord, conn, F.EXPECTED)
    reads = (
        await current_subjects(coord, conn, F.REPLICAS)
        if registry_found
        else {F.service_subject(s.service, inp.host) for s in out.services}
    )
    for subject in sorted(reads):
        service, host = F.split_service_subject(subject)
        if F.registry_subject(service) in in_registry:
            await coord.create(conn, "compare_service", CompareInput(service=service, host=host), tid)


async def current_subjects(coord: "Coordinator", conn: AsyncConnection, key: str) -> set[str]:
    q = select(facts.c.subject).where(
        facts.c.session_id == coord.sid, facts.c.key == key, facts.c.status != F.SUPERSEDED
    )
    return set((await conn.execute(q)).scalars())


async def registry_edge(coord: "Coordinator", conn: AsyncConnection, exclusive: bool) -> None:
    """The one place where two coordinators' results depend on each other.

    A read creates its compare only if the registry fact exists, and the
    registry creates compares for every read that exists. With several
    coordinators, a read and the registry committed at the same moment could
    each miss the other, so every discovery takes this lock in shared mode and
    the one that found the registry takes it exclusively, until its
    transaction ends. Whichever goes second then sees what the first committed.
    """
    lock = "pg_advisory_xact_lock" if exclusive else "pg_advisory_xact_lock_shared"
    await conn.execute(text(f"SELECT {lock}(hashtextextended(:key, 0))"), {"key": f"registry:{coord.sid}"})


async def accept_compare(coord: "Coordinator", conn: AsyncConnection, task: Row, out: CompareOutput) -> None:
    inp = CompareInput(**task.input)
    subject = F.service_subject(inp.service, inp.host)
    read = await coord.cited_fact(conn, out.read_fact_id, subject, F.REPLICAS)
    expected = await coord.cited_fact(conn, out.registry_fact_id, F.registry_subject(inp.service), F.EXPECTED)
    # Never take the model's word for something code can check exactly.
    if (read.value, expected.value) != (out.actual, out.expected):
        raise Rejected(f"claimed {out.actual} vs {out.expected}; facts say {read.value} vs {expected.value}")
    drift = read.value != expected.value
    if out.drift != drift:
        raise Rejected(f"claimed drift={out.drift}, comparison says {drift}")

    if not drift:
        await crud.upsert_fact(conn, coord.sid, subject, F.VERDICT, "match", source_task_id=task.id)
        return
    # A mismatch is only a hypothesis until independent reads agree.
    evidence = [{"event_id": str(read.source_event_id), "replicas": read.value}]
    await crud.upsert_fact(
        conn, coord.sid, subject, F.DRIFT,
        {"expected": expected.value, "actual": read.value},
        status=F.INFERRED, evidence=evidence, source_task_id=task.id,
    )  # fmt: skip
    await coord.create(
        conn, "verify_drift", VerifyInput(service=inp.service, host=inp.host, round=1), task.id
    )


async def accept_verify(coord: "Coordinator", conn: AsyncConnection, task: Row, out: VerifyOutput) -> None:
    inp = VerifyInput(**task.input)
    cited = await coord.cited_responses(conn, task)
    r = cited_response(cited, out.event_id, "get_service")
    if (r["host"], r["service"], r["replicas"]) != (inp.host, inp.service, out.replicas):
        raise Rejected(f"claimed {out.replicas}, response says {r['replicas']}")

    subject = F.service_subject(inp.service, inp.host)
    await crud.upsert_fact(
        conn, coord.sid, subject, F.REPLICAS, out.replicas,
        source_task_id=task.id, source_event_id=UUID(out.event_id),
    )  # fmt: skip
    drift = await crud.current_fact(conn, coord.sid, subject, F.DRIFT)
    if drift is None or drift.status != F.INFERRED:
        return  # already decided, so this read is just extra evidence

    # Two independent reads must agree. The newest read is not automatically
    # the truth, because it could be stale too.
    evidence = [*drift.evidence, {"event_id": out.event_id, "replicas": out.replicas}]
    value, votes = Counter(e["replicas"] for e in evidence).most_common(1)[0]
    expected = drift.value["expected"]
    if votes >= 2 and value == expected:
        await crud.update_fact(conn, coord.sid, drift.id, status=F.REFUTED, evidence=evidence)
    elif votes >= 2:
        new_value = {"expected": expected, "actual": value}
        await crud.update_fact(
            conn, coord.sid, drift.id, status=F.VERIFIED, evidence=evidence, value=new_value
        )
    else:
        # The reads disagree, so neither is trusted, and we break the tie with
        # another round, up to the cap.
        await crud.update_fact(conn, coord.sid, drift.id, status=F.INFERRED, evidence=evidence)
        if inp.round < MAX_VERIFY_ROUNDS:
            nxt = VerifyInput(service=inp.service, host=inp.host, round=inp.round + 1)
            await coord.create(conn, "verify_drift", nxt, task.id)


async def accept_report(coord: "Coordinator", conn: AsyncConnection, task: Row, out: ReportOutput) -> None:
    """The report must list exactly the verified drifts, each matching its fact."""
    q = select(facts.c.id).where(
        facts.c.session_id == coord.sid, facts.c.key == F.DRIFT, facts.c.status == F.VERIFIED
    )
    verified = {str(i) for i in (await conn.execute(q)).scalars()}
    cited = [f.fact_id for f in out.findings]
    if sorted(cited) != sorted(verified):
        raise Rejected(f"the report cites {len(cited)} drift(s), {len(verified)} are verified")
    for finding in out.findings:
        subject = F.service_subject(finding.service, finding.host)
        fact = await coord.cited_fact(conn, finding.fact_id, subject, F.DRIFT)
        if fact.value != {"expected": finding.expected, "actual": finding.actual}:
            raise Rejected(f"report says {finding.expected}/{finding.actual}, fact says {fact.value}")
    await coord.finish(
        conn, out.model_dump(mode="json"), succeeded=bool(out.findings) and not task.input["partial"]
    )


ACCEPT = {
    "discover_host": accept_discover,
    "compare_service": accept_compare,
    "verify_drift": accept_verify,
    "write_report": accept_report,
}


# --- replanning and splitting -------------------------------------------------------


async def replan(
    coord: "Coordinator", conn: AsyncConnection, task: Row, kind: str, delay: float, probe: bool
) -> None:
    """Fixed rules for a task that failed for good. Nothing downstream was ever
    created from it, because follow-ups only come from accepted results."""
    sid = coord.sid
    if task.type == "discover_host":
        inp = DiscoverInput(**task.input)
        subject = F.host_subject(inp.host)
        if kind == "not_found":
            await crud.upsert_fact(conn, sid, subject, F.EXISTS, False, source_task_id=task.id)
            return await coord.decide(conn, task, "replan", "host does not exist; dropped")
        await crud.upsert_fact(conn, sid, subject, F.UNREACHABLE, True, source_task_id=task.id)
        if inp.round < MAX_ROUNDS:
            nxt = inp.model_copy(update={"round": inp.round + 1})
            await coord.create(conn, task.type, nxt, task.id, delay, probe=probe)
            return await coord.decide(conn, task, "replan", f"new round {nxt.round} later")
    elif task.type == "compare_service":
        inp = CompareInput(**task.input)
        if inp.round < MAX_ROUNDS:
            nxt = CompareInput(service=inp.service, host=inp.host, round=inp.round + 1)
            await coord.create(conn, task.type, nxt, task.id, delay)
            return await coord.decide(conn, task, "replan", f"new round {nxt.round} later")
    elif task.type == "verify_drift":
        inp = VerifyInput(**task.input)
        if inp.round < MAX_VERIFY_ROUNDS:
            nxt = VerifyInput(service=inp.service, host=inp.host, round=inp.round + 1)
            await coord.create(conn, task.type, nxt, task.id, delay, probe=probe)
            return await coord.decide(conn, task, "replan", f"verify round {nxt.round} later")
    await coord.decide(conn, task, "replan", "out of rounds; given up")


async def split(coord: "Coordinator", conn: AsyncConnection, task: Row, reason: str) -> bool:
    """Split a discovery that didn't fit in one attempt into batches.

    A whole host is split into batches of services and one batch per document,
    and a document still too big for one attempt is split into its pages. What
    the host lists and how many pages a document has come from the failed
    attempt's own responses, which are raw tool output in `events`, and not
    from anything the worker claimed. A single page or a batch of services
    that still doesn't fit is not split again, so it fails like any other
    permanent error.
    """
    if task.type != "discover_host":
        return False
    inp = DiscoverInput(**task.input)
    responses = list((await coord.cited_responses(conn, task)).values())
    host_resp = next((p["response"] for p in responses if p["tool"] == "get_host"), None)
    if host_resp is None:
        return False
    plan: list[DiscoverInput] = []
    if not inp.parts:
        plan = batches(
            inp.host, inp.round, host_resp["services"], [(d, None) for d in host_resp["documents"]]
        )
    one_document = inp.documents and inp.page is None and not inp.services
    if one_document or len(plan) == 1 and plan[0].documents:
        # One document is too big on its own, so read it a page at a time.
        name = (inp.documents or plan[0].documents)[0]
        first = next(
            (
                p["response"]
                for p in responses
                if p["tool"] == "fetch_document" and p["response"]["name"] == name
            ),
            None,
        )
        if first is None:
            return False
        part, parts = (inp.part, inp.parts) if inp.parts else (1, 1)
        update = {"services": [], "documents": [name], "part": part, "parts": parts}
        plan = [inp.model_copy(update={**update, "page": page}) for page in range(first["pages"])]
    if len(plan) <= 1:
        return False
    for batch in plan:
        await coord.create(conn, "discover_host", batch, task.id)
    await coord.mark_split(conn, task, f"{reason}; split into {len(plan)} batches")
    return True


def batches(
    host: str, rnd: int, services: list[str], pages: list[tuple[str, int | None]]
) -> list[DiscoverInput]:
    """Discovery batches over these services and document pages, numbered as
    parts of one whole, where (document, None) stands for every page of it."""
    plan = [
        {"services": services[i : i + SPLIT_BATCH], "documents": []}
        for i in range(0, len(services), SPLIT_BATCH)
    ]
    plan += [{"services": [], "documents": [doc], "page": page} for doc, page in pages]
    return [DiscoverInput(host=host, round=rnd, part=n, parts=len(plan), **b) for n, b in enumerate(plan, 1)]


def pages_to_read(inp: DiscoverInput, listed: list[str], read: list) -> set[tuple[str, int]]:
    """The (document, page) pairs a discovery must report, which is every page of
    every document it was given, or the one page a page batch was given. How
    many pages a document has comes from the cited page responses."""
    if inp.page is not None:
        return {(name, inp.page) for name in inp.documents or []}
    pages = {d.name: d.pages for d in read}
    documents = inp.documents if inp.documents is not None else listed
    return {(name, page) for name in documents for page in range(pages.get(name, 1))}


# --- the goal and stalls -----------------------------------------------------------


async def goal_met(coord: "Coordinator", conn: AsyncConnection, session: Row) -> bool:
    if session.goal_kind == "all":
        return await everything_checked(coord, conn, session)
    q = select(facts.c.subject).where(
        facts.c.session_id == coord.sid, facts.c.key == F.DRIFT, facts.c.status == F.VERIFIED
    )
    for subject in (await conn.execute(q)).scalars():
        service, _ = F.split_service_subject(subject)
        if await crud.current_fact(conn, coord.sid, F.registry_subject(service), F.EXPECTED):
            return True
    return False


async def everything_checked(coord: "Coordinator", conn: AsyncConnection, session: Row) -> bool:
    """The 'find all drifts' goal, which is met when no host is left unexplored
    and every service has a verdict."""
    facts_now = await coord.current_facts(conn)
    if not facts_now.get(F.EXPECTED):
        return False  # the registry hasn't been found yet
    if unread(session, facts_now):
        return False
    for subject in facts_now.get(F.REPLICAS, {}):
        drift = facts_now.get(F.DRIFT, {}).get(subject)
        decided = drift is not None and drift.status in (F.VERIFIED, F.REFUTED)
        if subject not in facts_now.get(F.VERDICT, {}) and not decided:
            return False
    return True


def unread(session: Row, facts_now: dict[str, dict[str, Row]]) -> dict[str, tuple]:
    """What is known to exist but hasn't been read, as host -> (services, pages).

    `pages` holds (document, None) for a document not read at all and
    (document, page) for a later page that is missing. A host that some
    document mentions but nobody has read yet appears with (None, None),
    because its listing is not known yet.
    """
    exists, listings = facts_now.get(F.EXISTS, {}), facts_now.get(F.LISTING, {})
    mentions, n_pages = facts_now.get(F.MENTIONS, {}), facts_now.get(F.PAGES, {})
    reads = facts_now.get(F.REPLICAS, {})
    mentioned = set(session.start_points)
    for row in mentions.values():
        mentioned.update(row.value)
    found: dict[str, tuple] = {}
    for host in sorted(mentioned):
        if F.host_subject(host) not in exists:
            found[host] = (None, None)
    for subject, row in listings.items():
        host = subject.removeprefix("host:")
        services = [s for s in row.value["services"] if F.service_subject(s, host) not in reads]
        pages: list[tuple[str, int | None]] = []
        for doc in row.value["documents"]:
            first = F.doc_subject(doc, host)
            if first not in mentions:
                pages.append((doc, None))
                continue
            total = n_pages[first].value if first in n_pages else 1
            pages += [(doc, p) for p in range(1, total) if F.doc_subject(doc, host, p) not in mentions]
        if services or pages:
            found[host] = (services, pages)
    return found


async def reopen(coord: "Coordinator", conn: AsyncConnection) -> int:
    """Stall detection's second chance, which re-opens work that was put off.

    That is one more round for hosts given up as unreachable, for drifts still
    undecided and for services that never got a verdict, plus batches for
    whatever a host listed that nobody has read, where nothing is already
    working on them. Each re-opened task has a round past the usual cap, so
    its task_key is new, and a second re-open of the same thing does nothing
    because the key already exists.
    """
    working = {(t, i.get("host"), i.get("service")) for t, i in await coord.active_tasks(conn)}
    facts_now = await coord.current_facts(conn)
    has = {(subject, key) for key, rows in facts_now.items() for subject in rows}
    plan: list[tuple[str, BaseModel]] = []
    for subject in facts_now.get(F.UNREACHABLE, {}):
        host = subject.removeprefix("host:")
        if (subject, F.EXISTS) not in has and ("discover_host", host, None) not in working:
            plan.append(("discover_host", DiscoverInput(host=host, round=MAX_ROUNDS + 1)))
    for subject, row in facts_now.get(F.DRIFT, {}).items():
        service, host = F.split_service_subject(subject)
        if row.status == F.INFERRED and ("verify_drift", host, service) not in working:
            plan.append(
                ("verify_drift", VerifyInput(service=service, host=host, round=MAX_VERIFY_ROUNDS + 1))
            )
    for subject in facts_now.get(F.REPLICAS, {}):
        service, host = F.split_service_subject(subject)
        undecided = (subject, F.VERDICT) not in has and (subject, F.DRIFT) not in has
        in_registry = (F.registry_subject(service), F.EXPECTED) in has
        if undecided and in_registry and ("compare_service", host, service) not in working:
            plan.append(("compare_service", CompareInput(service=service, host=host, round=MAX_ROUNDS + 1)))
    # Hosts read only in part, because a batch of a split discovery failed for
    # good, get the rest read in batches of their own.
    session = await crud.get_session(conn, coord.sid)
    for host, (services, pages) in unread(session, facts_now).items():
        if services is not None and ("discover_host", host, None) not in working:
            plan += [("discover_host", inp) for inp in batches(host, MAX_ROUNDS + 1, services, pages)]
    created = 0
    for task_type, inp in plan:
        if await coord.create(conn, task_type, inp, None):
            created += 1
    return created
