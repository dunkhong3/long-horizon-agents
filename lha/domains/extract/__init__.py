"""Batch extraction, where agents read every document in a folder and fill in a table of fields.

Each document gets a task of its own, and a reader reports the value of
every field it states, with the sentence it comes from, which the
coordinator finds in the page it was read from before the value counts,
so the table holds nothing its documents don't say. A field no page states
is left empty and marked as missing, and the table is written by code as
CSV and JSON next to the run's other files.

It runs on mock documents generated from the seed, which are invoices with
known values, with the fake model or a real one, and is scored against
those values. With --docs and --fields it runs on a real folder instead,
with a real model, where the table is the result.
"""

import shutil
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.core.domain import Domain, TaskType
from lha.core.schemas import SUPERSEDED
from lha.db import models
from lha.domains.extract import agents, rules
from lha.domains.extract import facts as F
from lha.domains.extract.documents import list_documents
from lha.domains.extract.tasks import (
    ExtractInput,
    ExtractOutput,
    TableInput,
    TableOutput,
    task_key,
    task_scope,
)
from lha.domains.extract.tools import ExtractTools
from lha.domains.extract.world import DEFAULT_DOCS, FIELDS, MockDocs, generate_docs
from lha.domains.research.facts import normal

GOAL = "Extract {field_list} from each of the {n_docs} document(s) in {folder}, quoting each value."


@lru_cache(maxsize=16)
def _docs(seed: int, size: int, goal_kind: str) -> MockDocs:
    return generate_docs(seed, size, goal_kind)


def parse_fields(spec: str) -> list[dict[str, str]]:
    """'invoice number; total: the amount to pay' as fields, each with a name and the form of its value."""
    out: list[dict[str, str]] = []
    for part in spec.split(";"):
        name, _, form = part.partition(":")
        name = " ".join(name.split())
        if name and name.casefold() not in {f["name"].casefold() for f in out} and "|" not in name:
            out.append({"name": name, "form": " ".join(form.split())})
    return out


class ExtractDomain(Domain):
    name = "extract"
    roles = ("reader", "writer")
    task_types = {
        "extract_doc": TaskType(
            ExtractInput,
            ExtractOutput,
            "reader",
            agents.run_extract,
            agents.EXTRACT_INSTRUCTIONS,
            network=True,
        ),
        "write_table": TaskType(
            TableInput, TableOutput, "writer", agents.run_table, agents.TABLE_INSTRUCTIONS
        ),
    }
    report_type = "write_table"
    default_size = DEFAULT_DOCS  # documents in the mock folder
    real_docs = True
    goals = {"one": GOAL, "all": GOAL}
    done = {kind: "goal met: every document is read to its last page" for kind in ("one", "all")}

    # --- the world ----------------------------------------------------------

    def make_world(self, seed: int, size: int, goal_kind: str) -> MockDocs:
        return _docs(seed, size, goal_kind)

    def world_process(self, session: Row) -> bool:
        return False  # the tool reads the folder itself

    def prepare(self, session_id: UUID, world: MockDocs | None, args: Any) -> dict[str, Any]:
        """The mock documents written out to a folder of the run's own, or the
        real folder as it is, with the documents it holds and the fields."""
        if world is not None:
            folder = (Path("runs") / str(session_id) / "docs").resolve()
            if folder.exists():
                shutil.rmtree(folder)
            for name, text in world.files.items():
                (folder / name).parent.mkdir(parents=True, exist_ok=True)
                (folder / name).write_text(text)
            fields = [{"name": f, "form": FIELDS[f]} for f in world.fields]
        else:
            folder = Path(args.docs).expanduser().resolve()
            fields = parse_fields(args.fields)
        docs = list_documents(folder)
        names = [f["name"] for f in fields]
        field_list = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
        return {
            "folder": str(folder),
            "docs": docs,
            "fields": fields,
            "field_list": field_list,
            "n_docs": len(docs),
        }

    def tools(self, http, task_key, attempt, log, load, session) -> ExtractTools:
        return ExtractTools(http, task_key, attempt, log, load, session=session)

    # --- tasks ----------------------------------------------------------------

    def start(self, world: MockDocs | None) -> list[str]:
        return []

    def start_tasks(self, world: MockDocs | None, goal: str, params: dict) -> list[tuple[str, BaseModel]]:
        names = [f["name"] for f in params["fields"]]
        return [("extract_doc", ExtractInput(doc=doc, fields=names)) for doc in params["docs"]]

    def task_key(self, task_type: str, inp: BaseModel) -> str:
        return task_key(task_type, inp)

    def scope(self, task_type: str, inp: BaseModel) -> list[str]:
        return task_scope(inp)

    def resource(self, task_type: str, inp: BaseModel) -> str | None:
        return inp.doc if isinstance(inp, ExtractInput) else None

    def report_input(self, partial: bool) -> TableInput:
        return TableInput(partial=partial)

    def gone(self, task: Row, path: str) -> bool:
        return True

    def pointer_wanted(self, task: Row, path: str) -> bool:
        """A read may reuse earlier reads of its own document, or of the one page it reads."""
        doc, page = task.input.get("doc"), task.input.get("page")
        if task.type != "extract_doc" or doc is None:
            return False
        return path.startswith(f"read_document:{doc}#") and (
            page is None or path == f"read_document:{doc}#{page}"
        )

    async def accept(self, coord, conn, task, out) -> None:
        accept = {"extract_doc": rules.accept_extract, "write_table": rules.accept_table}
        await accept[task.type](coord, conn, task, out)

    async def replan(self, coord, conn, task, kind, delay, probe) -> None:
        await rules.replan(coord, conn, task, kind, delay, probe)

    async def split(self, coord, conn, task, reason) -> bool:
        return await rules.split(coord, conn, task, reason)

    async def goal_met(self, coord, conn, session) -> bool:
        return await rules.goal_met(coord, conn)

    async def reopen(self, coord, conn) -> int:
        return await rules.reopen(coord, conn)

    # --- scoring ---------------------------------------------------------------

    async def score(
        self, conn: AsyncConnection, session: Row, world: MockDocs | None
    ) -> list[tuple[str, bool]]:
        if world is None:
            return []
        cells = await _cells(conn, session.id)
        stated = [(k, v) for k, v in world.truth.items() if v is not None]
        unstated = [k for k, v in world.truth.items() if v is None]
        return [
            (
                f"every value is right ({len(stated)} stated)",
                all(
                    normal(cells.get(k, {}).get(F.VALUE, {}).get("value", "")) == normal(v) for k, v in stated
                ),
            ),
            (
                f"every unstated value is marked missing ({len(unstated)})",
                all(F.MISSING in cells.get(k, {}) and F.VALUE not in cells.get(k, {}) for k in unstated),
            ),
        ]

    async def stats(self, conn: AsyncConnection, sid: UUID, world: MockDocs | None) -> dict[str, Any]:
        f = models.facts.c
        q = select(f.value).where(
            f.session_id == sid, f.subject == F.PROGRESS, f.key == F.COUNTS, f.status != SUPERSEDED
        )
        counts = (await conn.execute(q)).scalar_one_or_none() or {}
        t = models.tasks.c
        pages = select(func.count()).where(
            t.session_id == sid, t.type == "extract_doc", t.input["page"].astext.isnot(None)
        )
        return {
            "docs_read": counts.get("done", 0),
            "values_found": counts.get("values", 0),
            "missing": counts.get("missing", 0),
            "page_tasks": (await conn.execute(pages)).scalar_one(),
        }

    async def progress(self, conn: AsyncConnection, sid: UUID) -> str:
        f = models.facts.c
        q = select(f.value).where(
            f.session_id == sid, f.subject == F.PROGRESS, f.key == F.COUNTS, f.status != SUPERSEDED
        )
        c = (await conn.execute(q)).scalar_one_or_none() or {}
        return f"documents read: {c.get('done', 0)}/{c.get('docs', '?')} | values: {c.get('values', 0)}"


async def _cells(conn: AsyncConnection, sid: UUID) -> dict[tuple[str, str], dict[str, Any]]:
    f = models.facts.c
    q = select(f.subject, f.key, f.value).where(
        f.session_id == sid, f.subject.like("cell:%"), f.status != SUPERSEDED
    )
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for subject, key, value in (await conn.execute(q)).all():
        doc, field = subject.removeprefix("cell:").rsplit("|", 1)
        out.setdefault((doc, field), {})[key] = value
    return out


EXTRACT = ExtractDomain()
