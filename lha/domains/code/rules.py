"""The code rules, which are what the coordinator does with each kind of result.

A whole check counts only as the cited run shows it, on the branch as it is
now, and every item it names must be in that run's output word for word. A
fix counts only when the cited run checked its item with exactly these
edits, by their fingerprint, and passed, and when the files it touches have
not changed on the branch since that run, and then the coordinator commits
it, one fix at a time under a lock, so two coordinators never commit at
once. A git commit and a Postgres commit can't be one transaction, so if
the coordinator dies between them the fix is on the branch with no fact,
and the next attempt at the item finds it passing, which heals it.
"""

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import func, select, text
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.core.domain import Rejected, cited_response
from lha.core.schemas import VERIFIED
from lha.db import crud
from lha.db.models import facts, tasks
from lha.domains.code import facts as F
from lha.domains.code import workspace as ws
from lha.domains.code.tasks import CheckInput, CheckOutput, FixInput, FixOutput, ReportOutput

if TYPE_CHECKING:
    from lha.core.coordinator import Coordinator

MAX_ROUNDS = 4  # whole checks, each followed by fixes for what it names
MAX_ITEMS_PER_ROUND = 20  # the rest come back in the next round's check
ACTIVE = ("ready", "leased", "submitted")


def _work(coord: "Coordinator") -> Path:
    return Path(coord.session.params["work"])


def _protected(coord: "Coordinator") -> tuple[str, ...]:
    return tuple(coord.session.params.get("protected", ()))


# --- the whole check --------------------------------------------------------------------


async def accept_check(coord: "Coordinator", conn: AsyncConnection, task: Row, out: CheckOutput) -> None:
    inp = CheckInput(**task.input)
    run = cited_response(await coord.cited_responses(conn, task), out.check_event_id, "run_check")
    if run["item"] or run["paths"]:
        raise Rejected("the whole check runs with no item and no edits")
    if "error" in run:
        raise Rejected(f"the check didn't run: {run['error']}")
    head = await asyncio.to_thread(ws.head, _work(coord))
    if run["commit"] != head:
        raise Rejected("the branch has moved since that run, so the check must run again")
    if out.passed != run["passed"]:
        raise Rejected(
            f"the answer says the check {'passed' if out.passed else 'failed'}, and the run says not"
        )
    if await crud.current_fact(conn, coord.sid, F.BRANCH, F.HEAD) is None:
        await crud.upsert_fact(
            conn, coord.sid, F.BRANCH, F.HEAD, {"commit": head, "fixes": 0}, source_task_id=task.id
        )
    if run["passed"]:
        result = {"commit": head, "passed": True, "round": inp.round, "failing": []}
        await crud.upsert_fact(
            conn, coord.sid, F.CHECK, F.RESULT, result, status=VERIFIED, source_task_id=task.id
        )
        return
    items = list(dict.fromkeys(i.strip() for i in out.items if i.strip()))
    if not items:
        raise Rejected("the check failed, so the answer must name what fails")
    for item in items:
        if not ws.valid_item(item):
            raise Rejected(f"{item!r} is not an item the check can take")
        if item not in run.get("output", ""):
            raise Rejected(f"the check's output doesn't name {item!r}")
    result = {"commit": head, "passed": False, "round": inp.round, "failing": items}
    await crud.upsert_fact(conn, coord.sid, F.CHECK, F.RESULT, result, source_task_id=task.id)
    for item in items[:MAX_ITEMS_PER_ROUND]:
        await coord.create(conn, "fix_item", FixInput(item=item, round=inp.round), task.id)


# --- fixing one item ------------------------------------------------------------------


async def accept_fix(coord: "Coordinator", conn: AsyncConnection, task: Row, out: FixOutput) -> None:
    inp = FixInput(**task.input)
    if out.item != inp.item:
        raise Rejected(f"fixed {out.item!r}, the task is {inp.item!r}")
    run = cited_response(await coord.cited_responses(conn, task), out.check_event_id, "run_check")
    edits = [e.model_dump() for e in out.edits]
    if run["item"] != inp.item:
        raise Rejected("the cited run checked another item")
    if not run["passed"]:
        raise Rejected("the cited run didn't pass")
    if run["edits_digest"] != ws.edits_digest(edits):
        raise Rejected("these edits are not the ones the cited run checked")
    if (out.status == "fixed") != bool(edits):
        raise Rejected("a fix comes with edits, and an item that already passes comes with none")

    # One commit at a time, whichever coordinator it is.
    await conn.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"), {"k": f"commit:{coord.sid}"}
    )
    work = _work(coord)
    head = await asyncio.to_thread(ws.head, work)
    commit = head
    if edits:
        if run["commit"] != head:
            changed = await asyncio.to_thread(ws.changed_since, work, run["commit"], run["paths"])
            if changed:
                raise Rejected(f"{', '.join(changed)} changed on the branch since the run, so check it again")
        try:
            commit = await asyncio.to_thread(
                ws.commit_edits, work, edits, f"lha: fix {inp.item}", _protected(coord)
            )
        except ws.WorkspaceError as e:
            raise Rejected(str(e)) from None
        branch = await crud.current_fact(conn, coord.sid, F.BRANCH, F.HEAD)
        fixes = (branch.value["fixes"] if branch else 0) + 1
        await crud.upsert_fact(
            conn, coord.sid, F.BRANCH, F.HEAD, {"commit": commit, "fixes": fixes}, source_task_id=task.id
        )
    status = {"status": out.status, "commit": commit, "round": inp.round}
    await crud.upsert_fact(
        conn, coord.sid, F.item_subject(inp.item), F.STATUS, status, status=VERIFIED, source_task_id=task.id
    )
    await next_round(coord, conn, inp.round, task)


async def next_round(coord: "Coordinator", conn: AsyncConnection, round_: int, task: Row) -> None:
    """Once no fix of a round is left, the whole check runs again, which
    starts the next round, until the rounds run out."""
    q = select(func.count()).where(
        tasks.c.session_id == coord.sid,
        tasks.c.type == "fix_item",
        tasks.c.status.in_(ACTIVE),
        tasks.c.id != task.id,
    )
    if (await conn.execute(q)).scalar_one() == 0 and round_ < MAX_ROUNDS:
        await coord.create(conn, "check_all", CheckInput(round=round_ + 1), task.id)


# --- the report -----------------------------------------------------------------------


async def accept_report(coord: "Coordinator", conn: AsyncConnection, task: Row, out: ReportOutput) -> None:
    """The report must list exactly the verified items, each matching its fact,
    and what was committed and what still fails are written here, from the facts."""
    verified = {str(r.id): r for r in await _items(coord, conn)}
    if sorted(f.fact_id for f in out.findings) != sorted(verified):
        raise Rejected(f"the report lists {len(out.findings)} item(s), {len(verified)} are verified")
    for f in out.findings:
        fact = verified[f.fact_id]
        if fact.subject != F.item_subject(f.item) or fact.value["status"] != f.status:
            raise Rejected(f"the report says {f.item} {f.status}, the fact says {fact.value['status']}")
    report = out.model_dump(mode="json")
    report["summary"] = out.summary.rstrip() + "\n\n" + await branch_section(coord, conn)
    met = await goal_met(coord, conn)
    await coord.finish(conn, report, succeeded=met and not task.input["partial"])


async def _items(coord: "Coordinator", conn: AsyncConnection) -> list[Row]:
    q = select(facts).where(
        facts.c.session_id == coord.sid,
        facts.c.subject.like("item:%"),
        facts.c.key == F.STATUS,
        facts.c.status == VERIFIED,
    )
    return list((await conn.execute(q)).all())


async def branch_section(coord: "Coordinator", conn: AsyncConnection) -> str:
    params = coord.session.params
    work = _work(coord)
    log = (await asyncio.to_thread(ws.git, work, "log", "--format=%h %s", f"{params['base']}..HEAD")).decode()
    lines = ["## The branch", ""]
    commits = log.splitlines()
    lines.append(
        f"The branch `{params['branch']}` of {params['repo']} has {len(commits)} commit(s) on top of "
        f"{params['base'][:10]}, which can be looked at with `git log {params['branch']}` and merged "
        "like any other branch."
    )
    lines += ["", *[f"- {c}" for c in commits]] if commits else []
    result = await crud.current_fact(conn, coord.sid, F.CHECK, F.RESULT)
    head = await asyncio.to_thread(ws.head, work)
    lines += ["", "## The check", ""]
    if result is None:
        lines.append("The whole check never ran to the end.")
    elif result.value["passed"] and result.value["commit"] == head:
        lines.append("The whole check passes on the branch.")
    else:
        failing = ", ".join(f"`{i}`" for i in result.value["failing"]) or "nothing it named"
        lines.append(f"The last whole check, in round {result.value['round']}, still failed on {failing}.")
    return "\n".join(lines) + "\n"


# --- replanning, the goal and stalls ----------------------------------------------------


async def replan(
    coord: "Coordinator", conn: AsyncConnection, task: Row, kind: str, delay: float, probe: bool
) -> None:
    if task.type == "fix_item":
        # Given up for this round, and the next whole check names it again if it still fails.
        inp = FixInput(**task.input)
        await crud.upsert_fact(
            conn, coord.sid, F.item_subject(inp.item), F.UNFIXABLE, True, source_task_id=task.id
        )
        await coord.decide(conn, task, "replan", "given up for this round")
        return await next_round(coord, conn, inp.round, task)
    await coord.decide(conn, task, "replan", "given up; a stall runs the whole check again")


async def goal_met(coord: "Coordinator", conn: AsyncConnection) -> bool:
    """The whole check passed on the branch as it is now."""
    result = await crud.current_fact(conn, coord.sid, F.CHECK, F.RESULT)
    if result is None or result.status != VERIFIED or not result.value["passed"]:
        return False
    return result.value["commit"] == await asyncio.to_thread(ws.head, _work(coord))


async def reopen(coord: "Coordinator", conn: AsyncConnection) -> int:
    """When nothing is left to do and the goal isn't met, the whole check runs
    again as the next round, which also covers a check that failed for good
    and a round whose last fix was accepted while another was still open."""
    active = await coord.active_tasks(conn)
    if any(t == "check_all" for t, _ in active):
        return 0
    q = select(func.max(tasks.c.input["round"].as_integer())).where(
        tasks.c.session_id == coord.sid, tasks.c.type == "check_all"
    )
    last = (await conn.execute(q)).scalar_one() or 0
    if last >= MAX_ROUNDS:
        return 0
    return 1 if await coord.create(conn, "check_all", CheckInput(round=last + 1), None) else 0
