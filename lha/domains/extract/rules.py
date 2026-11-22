"""The extraction rules, which are what the coordinator does with each kind of result.

A read must cover the document's pages, every value's quote must be in the
page it cites, word for word, and the value must be whole words of its
quote, so the table holds nothing a document doesn't say. A long document
read one page per task is put together under a lock per document, where
the lowest page that states a field gives its value, a page that states
something else records a conflict, and only once every page is read are
the values verified and a field no page states recorded as missing. The table is written by code from the
facts, as CSV and JSON next to the run's other files.
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
from lha.domains.extract import facts as F
from lha.domains.extract.tasks import ExtractInput, ExtractOutput, TableOutput
from lha.domains.research.facts import normal

if TYPE_CHECKING:
    from lha.core.coordinator import Coordinator


def docs_of(coord: "Coordinator") -> list[str]:
    return coord.session.params["docs"]


def fields_of(coord: "Coordinator") -> list[str]:
    return [f["name"] for f in coord.session.params["fields"]]


# --- reading a document -------------------------------------------------------------


async def accept_extract(coord: "Coordinator", conn: AsyncConnection, task: Row, out: ExtractOutput) -> None:
    inp = ExtractInput(**task.input)
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
        source = cited.get(v.event_id)
        if source is None or source["tool"] != "read_document" or source["response"].get("doc") != inp.doc:
            raise Rejected(
                f"{v.field} cites {v.event_id!r}, which is not a read of {inp.doc} in this attempt"
            )
        if not within(v.quote, source["response"].get("text", "")):
            raise Rejected(f"the document doesn't say {v.quote[:80]!r}")
        if not within(v.value, v.quote, whole_words=True):
            raise Rejected(f"the value {v.value!r} is not in its quote")
        stated.setdefault(v.field, (v.value, v.quote, source["response"]["page"]))

    # The pages of one document can be read by different tasks at once.
    await conn.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"), {"k": f"doc:{coord.sid}:{inp.doc}"}
    )
    read = await crud.current_fact(conn, coord.sid, F.doc_subject(inp.doc), F.READ)
    done = sorted(set(read.value["done"] if read else []) | wanted)
    await crud.upsert_fact(
        conn,
        coord.sid,
        F.doc_subject(inp.doc),
        F.READ,
        {"pages": total, "done": done},
        source_task_id=task.id,
    )
    for field, (value, quote, page) in stated.items():
        subject = F.cell_subject(inp.doc, field)
        current = await crud.current_fact(conn, coord.sid, subject, F.VALUE)
        if current is not None and normal(value) != normal(current.value["value"]):
            conflict = await crud.current_fact(conn, coord.sid, subject, F.CONFLICT)
            seen = sorted({*(conflict.value if conflict else [current.value["value"]]), value})
            await crud.upsert_fact(conn, coord.sid, subject, F.CONFLICT, seen, source_task_id=task.id)
        if current is None or page < current.value["page"]:  # the lowest page that states it wins
            cell, evidence = {"value": value, "quote": quote, "page": page}, [{"quote": quote, "page": page}]
            await crud.upsert_fact(
                conn, coord.sid, subject, F.VALUE, cell, evidence=evidence, source_task_id=task.id
            )
    if len(done) == total:
        # Every page is read, so each value is settled and verified, and a
        # field no page states is missing.
        for field in inp.fields:
            subject = F.cell_subject(inp.doc, field)
            cell = await crud.current_fact(conn, coord.sid, subject, F.VALUE)
            if cell is None:
                await crud.upsert_fact(conn, coord.sid, subject, F.MISSING, True, source_task_id=task.id)
            elif cell.status != VERIFIED:
                await crud.upsert_fact(
                    conn, coord.sid, subject, F.VALUE, cell.value, status=VERIFIED, evidence=cell.evidence,
                    source_task_id=task.id,
                )  # fmt: skip
    await update_progress(coord, conn, task)


async def update_progress(coord: "Coordinator", conn: AsyncConnection, task: Row | None) -> dict[str, int]:
    """The counts the writer works from, worked out again from the facts
    every time, in one query however many documents there are."""
    row = (
        await conn.execute(
            text(
                """
                SELECT
                  count(*) FILTER (
                    WHERE key = :read AND jsonb_array_length(value->'done') = (value->>'pages')::int
                  ),
                  count(*) FILTER (WHERE key = :value AND subject LIKE 'cell:%' AND status = :verified),
                  count(*) FILTER (WHERE key = :missing),
                  count(*) FILTER (WHERE key = :conflict),
                  count(*) FILTER (WHERE key = :unreadable)
                FROM facts WHERE session_id = :sid AND status != :superseded
                """
            ),
            {"read": F.READ, "value": F.VALUE, "missing": F.MISSING, "conflict": F.CONFLICT,
             "unreadable": F.UNREADABLE, "sid": coord.sid, "superseded": SUPERSEDED, "verified": VERIFIED},
        )
    ).one()  # fmt: skip
    keys = ("done", "values", "missing", "conflicts", "unreadable")
    counts = {"docs": len(docs_of(coord)), **dict(zip(keys, row, strict=True))}
    source = task.id if task is not None else None
    await crud.upsert_fact(conn, coord.sid, F.PROGRESS, F.COUNTS, counts, source_task_id=source)
    return counts


# --- the table ------------------------------------------------------------------------


async def accept_table(coord: "Coordinator", conn: AsyncConnection, task: Row, out: TableOutput) -> None:
    """The table, its findings and its files are all written from the facts,
    and only the summary on top comes from the writer."""
    cells = await _cells(coord, conn)
    rows = []
    for doc in docs_of(coord):
        row = {"document": doc}
        for field in fields_of(coord):
            cell = cells.get((doc, field), {})
            row[field] = cell["value"]["value"] if "value" in cell else ""
        rows.append(row)
    out_dir = Path("runs") / str(coord.sid)
    out_dir.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["document", *fields_of(coord)])
    writer.writeheader()
    writer.writerows(rows)
    (out_dir / "table.csv").write_text(buffer.getvalue())
    detail = [
        {"document": doc, "field": field, **({"value": c["value"]["value"], "quote": c["value"]["quote"]}
         if "value" in c else {"value": None, "missing": "missing" in c, "conflict": c.get("conflict")})}
        for (doc, field), c in sorted(cells.items())
    ]  # fmt: skip
    (out_dir / "table.json").write_text(json.dumps(detail, indent=2))
    findings = [
        {"document": doc, "field": field, "value": c["value"]["value"], "fact_id": c["value_id"]}
        for (doc, field), c in sorted(cells.items())
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
        doc, field = r.subject.removeprefix("cell:").rsplit("|", 1)
        if r.key == F.VALUE and r.status != VERIFIED:
            continue  # a value from a document that was never read to the end
        cell = out.setdefault((doc, field), {})
        cell[r.key] = r.value
        if r.key == F.VALUE:
            cell["value_id"] = str(r.id)
    return out


def _table_section(coord: "Coordinator", counts: dict[str, int], cells: dict) -> str:
    sid = coord.sid
    lines = ["## The table", ""]
    lines.append(
        f"{counts['values']} value(s) from {counts['done']} of {counts['docs']} document(s) are in "
        f"`runs/{sid}/table.csv`, and `runs/{sid}/table.json` has the sentence behind each of them."
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
    if counts["unreadable"]:
        lines += ["", f"{counts['unreadable']} document(s) could not be read."]
    return "\n".join(lines) + "\n"


# --- replanning, splitting, the goal and stalls -------------------------------------------


async def replan(
    coord: "Coordinator", conn: AsyncConnection, task: Row, kind: str, delay: float, probe: bool
) -> None:
    if task.type != "extract_doc":
        return await coord.decide(conn, task, "replan", "out of rounds; given up")
    inp = ExtractInput(**task.input)
    if kind not in ("not_found", "unreadable") and inp.round < MAX_ROUNDS:
        nxt = inp.model_copy(update={"round": inp.round + 1})
        await coord.create(conn, task.type, nxt, task.id, delay, probe=probe)
        return await coord.decide(conn, task, "replan", f"new round {nxt.round} later")
    await crud.upsert_fact(
        conn, coord.sid, F.doc_subject(inp.doc), F.UNREADABLE, True, source_task_id=task.id
    )
    await update_progress(coord, conn, task)
    await coord.decide(conn, task, "replan", f"{inp.doc} given up ({kind.replace('_', ' ')})")


async def split(coord: "Coordinator", conn: AsyncConnection, task: Row, reason: str) -> bool:
    """A document too long for one attempt is read one page per task, where
    the number of pages comes from the failed attempt's own read of page 0."""
    if task.type != "extract_doc":
        return False
    inp = ExtractInput(**task.input)
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
        nxt = ExtractInput(doc=inp.doc, fields=inp.fields, page=page, round=inp.round)
        await coord.create(conn, "extract_doc", nxt, task.id)
    await coord.mark_split(conn, task, f"{reason}; split into {first['pages']} pages")
    return True


async def goal_met(coord: "Coordinator", conn: AsyncConnection) -> bool:
    """Every document is read to its last page, or given up."""
    counts = await crud.current_fact(conn, coord.sid, F.PROGRESS, F.COUNTS)
    if counts is None:
        return False
    return counts.value["done"] + counts.value["unreadable"] >= counts.value["docs"]


async def reopen(coord: "Coordinator", conn: AsyncConnection) -> int:
    """One more round for each document that is neither read to the end nor
    given up and that nothing is working on."""
    active = {i.get("doc") for t, i in await coord.active_tasks(conn) if t == "extract_doc"}
    now = await coord.current_facts(conn)
    reads, unreadable = now.get(F.READ, {}), now.get(F.UNREADABLE, {})
    created = 0
    for doc in docs_of(coord):
        subject = F.doc_subject(doc)
        read = reads.get(subject)
        if doc in active or subject in unreadable or (read and len(read["done"]) == read["pages"]):
            continue
        inp = ExtractInput(doc=doc, fields=fields_of(coord), round=MAX_ROUNDS + 1)
        if await coord.create(conn, "extract_doc", inp, None):
            created += 1
    return created
