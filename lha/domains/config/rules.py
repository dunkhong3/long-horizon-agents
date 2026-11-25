"""The rules of a domain loaded from configuration, which are what the coordinator does with each result.

A stage's read must cover the document's pages, every value's quote must be
in the page it cites, word for word, and a value must be whole words of its
quote, or for a field with options one of the options, which the quote
only has to support, and it must match the field's pattern when it has
one, so the table holds nothing its documents don't say. The pages of a
long document are put together as in batch extraction (lha/domains/extract/
rules.py), where the lowest page that states a field gives its value, and
only once every page is read for a stage are its values verified and a
field no page states recorded as missing. A stage that waits on another
with `needs` is skipped for a document the other one gave up on, which the
coordinator works out from task_deps ('upstream_failed').
"""

import csv
import io
import json
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select, text
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.config import MAX_ROUNDS
from lha.core.domain import Rejected
from lha.core.quotes import within
from lha.core.schemas import SUPERSEDED, VERIFIED
from lha.db import crud
from lha.db.models import facts
from lha.domains.config import facts as F
from lha.domains.config.tasks import StageInput, StageOutput
from lha.domains.research.facts import normal

if TYPE_CHECKING:
    from lha.core.coordinator import Coordinator
    from lha.domains.config import ConfigDomain


def domain_of(coord: "Coordinator") -> "ConfigDomain":
    return coord.domain  # type: ignore[return-value]


def docs_of(coord: "Coordinator") -> list[str]:
    return coord.session.params["docs"]


# --- reading a document for a stage ---------------------------------------------------


async def accept_stage(coord: "Coordinator", conn: AsyncConnection, task: Row, out: StageOutput) -> None:
    spec = domain_of(coord).spec
    inp = StageInput(**task.input)
    if out.doc != inp.doc:
        raise Rejected(f"read {out.doc}, the task is {inp.doc}")
    cited = await coord.cited_responses(conn, task)
    pages = {
        p["response"]["page"]: (eid, p["response"])
        for eid, p in cited.items()
        if p["tool"] == "read_document" and p["response"].get("doc") == inp.doc and "text" in p["response"]
    }
    if not pages:
        raise Rejected("the document was never read")
    total = next(iter(pages.values()))[1]["pages"]
    wanted = {inp.page} if inp.page is not None else set(range(total))
    if missing := wanted - set(pages):
        raise Rejected(f"pages {sorted(missing)} of {inp.doc} were not read")
    stated: dict[str, tuple[str, str, int]] = {}
    for v in out.values:
        if v.field not in inp.fields or v.value is None:
            continue  # fields nobody asked for are left out, and a null says nothing
        field = spec.field(v.field)
        source = cited.get(v.event_id)
        if source is None or source["tool"] != "read_document" or source["response"].get("doc") != inp.doc:
            raise Rejected(
                f"{v.field} cites {v.event_id!r}, which is not a read of {inp.doc} in this attempt"
            )
        if not within(v.quote, source["response"].get("text", "")):
            raise Rejected(f"the document doesn't say {v.quote[:80]!r}")
        if problem := field.problem(v.value):
            raise Rejected(problem)
        value = field.option(v.value) if field.one_of else v.value
        if field.one_of is None and not within(v.value, v.quote, whole_words=True):
            raise Rejected(f"the value {v.value!r} is not in its quote")
        stated.setdefault(v.field, (value, v.quote, source["response"]["page"]))

    # The pages of one document can be read by different tasks of one stage.
    subject = F.item_subject(inp.doc, inp.stage)
    await conn.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"), {"k": f"{coord.sid}:{subject}"}
    )
    read = await crud.current_fact(conn, coord.sid, subject, F.READ)
    done = sorted(set(read.value["done"] if read else []) | wanted)
    await crud.upsert_fact(
        conn, coord.sid, subject, F.READ, {"pages": total, "done": done}, source_task_id=task.id
    )
    for name, (value, quote, page) in stated.items():
        cell_subject = F.cell_subject(inp.doc, name)
        current = await crud.current_fact(conn, coord.sid, cell_subject, F.VALUE)
        if current is not None and normal(value) != normal(current.value["value"]):
            conflict = await crud.current_fact(conn, coord.sid, cell_subject, F.CONFLICT)
            seen = sorted({*(conflict.value if conflict else [current.value["value"]]), value})
            await crud.upsert_fact(conn, coord.sid, cell_subject, F.CONFLICT, seen, source_task_id=task.id)
        if current is None or page < current.value["page"]:  # the lowest page that states it wins
            cell, evidence = {"value": value, "quote": quote, "page": page}, [{"quote": quote, "page": page}]
            await crud.upsert_fact(
                conn, coord.sid, cell_subject, F.VALUE, cell, evidence=evidence, source_task_id=task.id
            )
    if len(done) == total:
        # Every page is read for this stage, so each of its values is settled
        # and verified, and a field no page states is missing.
        for name in inp.fields:
            cell_subject = F.cell_subject(inp.doc, name)
            cell = await crud.current_fact(conn, coord.sid, cell_subject, F.VALUE)
            if cell is None:
                await crud.upsert_fact(conn, coord.sid, cell_subject, F.MISSING, True, source_task_id=task.id)
            elif cell.status != VERIFIED:
                await crud.upsert_fact(
                    conn, coord.sid, cell_subject, F.VALUE, cell.value,
                    status=VERIFIED, evidence=cell.evidence, source_task_id=task.id,
                )  # fmt: skip
    await update_progress(coord, conn, task)


async def update_progress(coord: "Coordinator", conn: AsyncConnection, task: Row | None) -> dict[str, int]:
    """The counts the writer and the progress line work from, worked out again
    from the facts every time, which only the leader keeps as a fact, because
    a fact has one writer and every coordinator accepts reads."""
    counts = await count_progress(coord, conn)
    if coord.partition == 0:
        source = task.id if task is not None else None
        await crud.upsert_fact(conn, coord.sid, F.PROGRESS, F.COUNTS, counts, source_task_id=source)
    return counts


async def count_progress(coord: "Coordinator", conn: AsyncConnection) -> dict[str, int]:
    """The counts, in one query however many documents there are."""
    row = (
        await conn.execute(
            text(
                """
                SELECT
                  count(*) FILTER (
                    WHERE key = :read AND subject LIKE 'item:%'
                      AND jsonb_array_length(value->'done') = (value->>'pages')::int
                  ),
                  count(*) FILTER (WHERE key = :unreadable AND subject LIKE 'item:%'),
                  count(*) FILTER (WHERE key = :skipped AND subject LIKE 'item:%'),
                  count(*) FILTER (WHERE key = :value AND subject LIKE 'cell:%' AND status = :verified),
                  count(*) FILTER (WHERE key = :missing AND subject LIKE 'cell:%'),
                  count(*) FILTER (WHERE key = :conflict AND subject LIKE 'cell:%')
                FROM facts WHERE session_id = :sid AND status != :superseded
                """
            ),
            {"read": F.READ, "unreadable": F.UNREADABLE, "skipped": F.SKIPPED, "value": F.VALUE,
             "missing": F.MISSING, "conflict": F.CONFLICT, "sid": coord.sid, "superseded": SUPERSEDED,
             "verified": VERIFIED},
        )
    ).one()  # fmt: skip
    keys = ("done", "unreadable", "skipped", "values", "missing", "conflicts")
    docs = len(docs_of(coord))
    return {
        "docs": docs,
        "items": docs * len(domain_of(coord).spec.stages),
        **dict(zip(keys, row, strict=True)),
    }


# --- the report -------------------------------------------------------------------------


async def accept_report(coord: "Coordinator", conn: AsyncConnection, task: Row, out) -> None:
    """The table, its findings and its files are all written from the facts,
    and only the summary on top comes from the writer."""
    spec = domain_of(coord).spec
    names = [f.name for f in spec.fields()]
    cells = await _cells(coord, conn)
    rows = []
    for doc in docs_of(coord):
        row = {"document": doc}
        for name in names:
            cell = cells.get((doc, name), {})
            row[name] = cell["value"]["value"] if "value" in cell else ""
        rows.append(row)
    out_dir = Path("runs") / str(coord.sid)
    out_dir.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["document", *names])
    writer.writeheader()
    writer.writerows(rows)
    (out_dir / "table.csv").write_text(buffer.getvalue())
    detail = [
        {"document": doc, "field": name, **({"value": c["value"]["value"], "quote": c["value"]["quote"]}
         if "value" in c else {"value": None, "missing": "missing" in c, "conflict": c.get("conflict")})}
        for (doc, name), c in sorted(cells.items())
    ]  # fmt: skip
    (out_dir / "table.json").write_text(json.dumps(detail, indent=2))
    findings = [
        {"document": doc, "field": name, "value": c["value"]["value"], "fact_id": c["value_id"]}
        for (doc, name), c in sorted(cells.items())
        if "value" in c
    ]
    counts = await update_progress(coord, conn, task)
    report = {
        "findings": findings,
        "summary": out.summary.rstrip() + "\n\n" + _table_section(coord, counts, cells),
    }
    met = await goal_met(coord, conn)
    await coord.finish(conn, report, succeeded=met and not task.input["partial"])


async def _cells(coord: "Coordinator", conn: AsyncConnection) -> dict[tuple[str, str], dict]:
    q = select(facts).where(
        facts.c.session_id == coord.sid, facts.c.subject.like("cell:%"), facts.c.status != SUPERSEDED
    )
    out: dict[tuple[str, str], dict] = {}
    for r in (await conn.execute(q)).all():
        doc, name = r.subject.removeprefix("cell:").rsplit("|", 1)
        if r.key == F.VALUE and r.status != VERIFIED:
            continue  # a value from a stage that never read its document to the end
        cell = out.setdefault((doc, name), {})
        cell[r.key] = r.value
        if r.key == F.VALUE:
            cell["value_id"] = str(r.id)
    return out


def _table_section(coord: "Coordinator", counts: dict[str, int], cells: dict) -> str:
    sid = coord.sid
    lines = ["## The table", ""]
    lines.append(
        f"{counts['values']} value(s) from {counts['done']} of {counts['items']} stage(s) of "
        f"{counts['docs']} document(s) are in `runs/{sid}/table.csv`, and `runs/{sid}/table.json` "
        "has the sentence behind each of them."
    )
    missing = sorted(f"{d} ({f})" for (d, f), c in cells.items() if "missing" in c and "value" not in c)
    conflicts = sorted(
        f"{d} ({f}): {', '.join(c['conflict'])}" for (d, f), c in cells.items() if "conflict" in c
    )
    if missing:
        lines += ["", f"Not stated in the document, so left empty: {', '.join(missing[:50])}."]
    if conflicts:
        lines += ["", "Stated differently on different pages, where the first page's value is kept:"]
        lines += [f"- {c}" for c in conflicts[:50]]
    if counts["unreadable"] or counts["skipped"]:
        lines += [
            "",
            f"{counts['unreadable']} stage(s) could not read their document, and {counts['skipped']} "
            "were skipped because a stage they need could not.",
        ]
    return "\n".join(lines) + "\n"


# --- replanning, splitting, the goal and stalls -------------------------------------------


async def replan(
    coord: "Coordinator", conn: AsyncConnection, task: Row, kind: str, delay: float, probe: bool
) -> None:
    if task.type not in {s.name for s in domain_of(coord).spec.stages}:
        return await coord.decide(conn, task, "replan", "out of rounds; given up")
    inp = StageInput(**task.input)
    subject = F.item_subject(inp.doc, inp.stage)
    if kind == "upstream_failed":
        await crud.upsert_fact(conn, coord.sid, subject, F.SKIPPED, True, source_task_id=task.id)
        await update_progress(coord, conn, task)
        return await coord.decide(conn, task, "replan", f"{inp.stage} skipped for {inp.doc}")
    if kind not in ("not_found", "unreadable") and inp.round < MAX_ROUNDS:
        nxt = inp.model_copy(update={"round": inp.round + 1})
        await coord.create(conn, task.type, nxt, task.id, delay, probe=probe, replaces=task)
        return await coord.decide(conn, task, "replan", f"new round {nxt.round} later")
    await crud.upsert_fact(conn, coord.sid, subject, F.UNREADABLE, True, source_task_id=task.id)
    await update_progress(coord, conn, task)
    await coord.decide(conn, task, "replan", f"{inp.stage} of {inp.doc} given up ({kind.replace('_', ' ')})")


async def split(coord: "Coordinator", conn: AsyncConnection, task: Row, reason: str) -> bool:
    """A document too long for one attempt is read one page per task, where
    the number of pages comes from the failed attempt's own read of page 0,
    and every page task takes over the stages that wait on this one."""
    if task.type not in {s.name for s in domain_of(coord).spec.stages}:
        return False
    inp = StageInput(**task.input)
    if inp.page is not None:
        return False
    first = next(
        (
            p["response"]
            for p in (await coord.cited_responses(conn, task)).values()
            if p["tool"] == "read_document"
            and p["response"].get("doc") == inp.doc
            and p["response"].get("page") == 0
        ),
        None,
    )
    if first is None or first["pages"] < 2:
        return False
    for page in range(first["pages"]):
        await coord.create(conn, task.type, inp.model_copy(update={"page": page}), task.id)
    await coord.mark_split(conn, task, f"{reason}; split into {first['pages']} pages")
    return True


async def goal_met(coord: "Coordinator", conn: AsyncConnection) -> bool:
    """Every stage of every document is read to the end, given up or skipped,
    and at least one is read, because a run that gave up on everything met
    nothing, so it stalls and writes a partial report instead. Only the
    leader asks, and it keeps the counts up to date while it does."""
    c = await update_progress(coord, conn, None)
    return c["done"] > 0 and c["done"] + c["unreadable"] + c["skipped"] >= c["items"]


async def reopen(coord: "Coordinator", conn: AsyncConnection) -> int:
    """One more round for each stage of a document that is neither read to the
    end, given up nor skipped, and that nothing is working on or waiting for."""
    spec = domain_of(coord).spec
    active = {(t, i.get("doc")) for t, i in await coord.active_tasks(conn)}
    now = await coord.current_facts(conn)
    reads, unreadable, skipped = now.get(F.READ, {}), now.get(F.UNREADABLE, {}), now.get(F.SKIPPED, {})
    created = 0
    for doc in docs_of(coord):
        for stage in spec.stages:
            subject = F.item_subject(doc, stage.name)
            read = reads.get(subject)
            if (stage.name, doc) in active or subject in unreadable or subject in skipped:
                continue
            if read and len(read.value["done"]) == read.value["pages"]:
                continue
            inp = StageInput(
                stage=stage.name, doc=doc, fields=[f.name for f in stage.fields], round=MAX_ROUNDS + 1
            )
            if await coord.create(conn, stage.name, inp, None):
                created += 1
    return created
