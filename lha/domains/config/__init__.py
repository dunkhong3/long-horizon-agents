"""Domains loaded from configuration, which are pipelines of stages over documents.

A new domain can be a TOML file instead of a folder of Python, when what it
does is read documents in stages and fill in a table (lha/domains/config/
spec.py describes the file). Every stage is a task type, with one task per
document, and a stage that waits on earlier ones is planned with them and
waits in task_deps until they are done. The values are checked by code as
in batch extraction, every quote against its page, every value against its
quote, its options and its pattern, and the table is written by code.

A run keeps its domain's spec with its session in Postgres, so every process
that works on it, a joined machine's workers included, builds the same
domain from the session alone (get_domain in lha/core/domain.py). It can't
express a search, sources that have to agree or edits to files, which stay
domains written in Python.

    python -m lha.run --domain tickets                     # the example's own documents, fake model
    python -m lha.run --domain-file my.toml --docs ~/inbox  # a folder, with a real model
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from lha.core.domain import Domain, Planned, TaskType
from lha.core.schemas import SUPERSEDED
from lha.db import models
from lha.domains.config import agents, rules
from lha.domains.config import facts as F
from lha.domains.config.spec import REPORT_TYPE, DomainSpec, SpecError, load_spec, parse_spec
from lha.domains.config.tasks import ReportInput, ReportOutput, StageInput, StageOutput, task_key, task_scope
from lha.domains.config.tools import ConfigTools
from lha.domains.extract.documents import list_documents
from lha.domains.research.facts import normal

EXAMPLES = Path(__file__).parent / "examples"
GOAL = (
    "Work through each of the {n_docs} document(s) in {where}, stage by stage ({stages}), "
    "and quote every value from its document."
)


@dataclass
class ConfigDocs:
    """The configuration's own documents, which are its mock world, with the
    values each one states and None for a field it doesn't state."""

    files: dict[str, str] = field(default_factory=dict)  # name -> text
    truth: dict[tuple[str, str], str | None] = field(default_factory=dict)  # (doc, field) -> value


class ConfigDomain(Domain):
    real_docs = True  # a folder of documents with --docs, which needs a real model

    def __init__(self, spec: DomainSpec, world: str = "mock"):
        self.spec = spec
        self.name = spec.name
        self.description = spec.description
        self.roles = spec.roles()
        local = world == "docs"  # a folder only the home machine has, while its own documents are in Postgres
        self.task_types = {
            s.name: TaskType(
                StageInput,
                StageOutput,
                s.worker_role,
                agents.run_stage,
                agents.stage_instructions(spec, s),
                network=True,
                rereads=True,  # every stage reads its document itself, on purpose
                local=local,
            )
            for s in spec.stages
        }
        self.task_types[REPORT_TYPE] = TaskType(
            ReportInput, ReportOutput, spec.report.role, agents.run_report, agents.report_instructions(spec)
        )
        self.report_type = REPORT_TYPE
        self.default_size = len(spec.documents)
        self.mock_world = bool(spec.documents)
        goal = spec.goal or GOAL
        self.goals = {"one": goal, "all": goal}
        self.done = {kind: "goal met: every stage of every document is settled" for kind in ("one", "all")}

    # --- the world ----------------------------------------------------------

    def make_world(self, seed: int, size: int, goal_kind: str) -> ConfigDocs:
        world = ConfigDocs()
        names = [f.name for f in self.spec.fields()]
        for doc in self.spec.documents:
            world.files[doc.name] = doc.text
            expect = {k.casefold(): v for k, v in doc.expect.items()}
            missing = {k.casefold() for k in doc.missing}
            for name in names:
                if name.casefold() in expect:
                    world.truth[(doc.name, name)] = expect[name.casefold()]
                elif name.casefold() in missing:
                    world.truth[(doc.name, name)] = None
        return world

    def world_process(self, session: Row) -> bool:
        return False  # the tool reads the documents itself

    def prepare(self, session_id: UUID, world: ConfigDocs | None, args: Any) -> dict[str, Any]:
        """The spec, kept with the session, and the documents, which are its own
        or the folder given with --docs."""
        if world is not None:
            folder, docs, where = "", list(world.files), "its configuration"
        else:
            path = Path(args.docs).expanduser().resolve()
            folder, docs, where = str(path), list_documents(path), str(path)
        return {
            "spec": self.spec.dump(),
            "folder": folder,
            "docs": docs,
            "n_docs": len(docs),
            "where": where,
            "stages": " then ".join(s.name for s in self.spec.stages),
        }

    def tools(self, http, task_key, attempt, log, load, session) -> ConfigTools:
        return ConfigTools(http, task_key, attempt, log, load, session=session)

    # --- tasks ----------------------------------------------------------------

    def start(self, world: ConfigDocs | None) -> list[str]:
        return []

    def start_tasks(self, world: ConfigDocs | None, goal: str, params: dict) -> list[Planned]:
        """Every stage of every document, planned at once, where a stage waits
        on the stages it names for the same document."""
        planned = []
        for doc in params["docs"]:
            for s in self.spec.stages:
                inp = StageInput(stage=s.name, doc=doc, fields=[f.name for f in s.fields])
                planned.append(
                    Planned(
                        s.name,
                        inp,
                        after=tuple(f"{a}:{doc}" for a in s.after),
                        needs=tuple(f"{n}:{doc}" for n in s.needs),
                    )
                )
        return planned

    def task_key(self, task_type: str, inp: BaseModel) -> str:
        return task_key(task_type, inp)

    def scope(self, task_type: str, inp: BaseModel) -> list[str]:
        return task_scope(inp)

    def resource(self, task_type: str, inp: BaseModel) -> str | None:
        """The document, so every stage of a document has one coordinator, which
        is also the one that releases the stages waiting on the others."""
        return inp.doc if isinstance(inp, StageInput) else None

    def report_input(self, partial: bool) -> ReportInput:
        return ReportInput(partial=partial)

    def gone(self, task: Row, path: str) -> bool:
        return True

    def pointer_wanted(self, task: Row, path: str) -> bool:
        """A read may reuse earlier reads of its own document, or of the one page it reads."""
        doc, page = task.input.get("doc"), task.input.get("page")
        if task.type == REPORT_TYPE or doc is None:
            return False
        return path.startswith(f"read_document:{doc}#") and (
            page is None or path == f"read_document:{doc}#{page}"
        )

    async def accept(self, coord, conn, task, out) -> None:
        if task.type == REPORT_TYPE:
            return await rules.accept_report(coord, conn, task, out)
        await rules.accept_stage(coord, conn, task, out)

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
        self, conn: AsyncConnection, session: Row, world: ConfigDocs | None
    ) -> list[tuple[str, bool]]:
        """Against the values its own documents say they state, when it ran on those."""
        if world is None or not world.truth:
            return []
        cells = await _cells(conn, session.id)
        stated = [(k, v) for k, v in world.truth.items() if v is not None]
        unstated = [k for k, v in world.truth.items() if v is None]
        checks = []
        if stated:
            right = all(
                normal(cells.get(k, {}).get(F.VALUE, {}).get("value", "")) == normal(v) for k, v in stated
            )
            checks.append((f"every value is right ({len(stated)} stated)", right))
        if unstated:
            marked = all(F.MISSING in cells.get(k, {}) and F.VALUE not in cells.get(k, {}) for k in unstated)
            checks.append((f"every unstated value is marked missing ({len(unstated)})", marked))
        return checks

    async def stats(self, conn: AsyncConnection, sid: UUID, world: ConfigDocs | None) -> dict[str, Any]:
        counts = await _counts(conn, sid)
        t = models.tasks.c
        pages = select(func.count()).where(t.session_id == sid, t.input["page"].astext.isnot(None))
        return {
            "stages_read": counts.get("done", 0),
            "values_found": counts.get("values", 0),
            "missing": counts.get("missing", 0),
            "skipped": counts.get("skipped", 0),
            "page_tasks": (await conn.execute(pages)).scalar_one(),
        }

    async def progress(self, conn: AsyncConnection, sid: UUID) -> str:
        c = await _counts(conn, sid)
        return f"stages read: {c.get('done', 0)}/{c.get('items', '?')} | values: {c.get('values', 0)}"


async def _counts(conn: AsyncConnection, sid: UUID) -> dict[str, Any]:
    f = models.facts.c
    q = select(f.value).where(
        f.session_id == sid, f.subject == F.PROGRESS, f.key == F.COUNTS, f.status != SUPERSEDED
    )
    return (await conn.execute(q)).scalar_one_or_none() or {}


async def _cells(conn: AsyncConnection, sid: UUID) -> dict[tuple[str, str], dict[str, Any]]:
    f = models.facts.c
    q = select(f.subject, f.key, f.value).where(
        f.session_id == sid, f.subject.like("cell:%"), f.status != SUPERSEDED
    )
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for subject, key, value in (await conn.execute(q)).all():
        doc, name = subject.removeprefix("cell:").rsplit("|", 1)
        out.setdefault((doc, name), {})[key] = value
    return out


# --- finding a domain's configuration --------------------------------------------------

_registered: dict[str, DomainSpec] = {}  # loaded with --domain-file in this process
_by_session: dict[tuple[UUID, str], ConfigDomain] = {}


def search_path() -> list[Path]:
    """Where a domain's file is looked for by its name, which is every folder
    in LHA_DOMAIN_PATH and then the examples that come with the code."""
    dirs = [Path(p).expanduser() for p in os.environ.get("LHA_DOMAIN_PATH", "").split(os.pathsep) if p]
    return [*dirs, EXAMPLES]


def find_spec(name: str) -> DomainSpec | None:
    """The configuration of the domain with this name, from a file named
    <name>.toml on the search path, or one loaded with --domain-file, or None."""
    if name in _registered:
        return _registered[name]
    for folder in search_path():
        path = folder / f"{name}.toml"
        if path.is_file():
            spec = load_spec(path)
            if spec.name != name:
                raise SpecError(f"{path}: it names its domain {spec.name!r}, while its file says {name!r}")
            return spec
    return None


def register(spec: DomainSpec) -> None:
    _registered[spec.name] = spec


def list_specs() -> list[tuple[Path, DomainSpec | None, str | None]]:
    """Every file on the search path, with its domain or what is wrong with it,
    where a name found twice is the first one's."""
    out, seen = [], set()
    for folder in search_path():
        for path in sorted(folder.glob("*.toml")) if folder.is_dir() else []:
            if path.stem in seen:
                continue
            seen.add(path.stem)
            try:
                out.append((path, load_spec(path), None))
            except SpecError as e:
                out.append((path, None, str(e)))
    return out


def config_domain(name: str, world: str = "mock") -> ConfigDomain:
    """The domain a new run of this configuration works in, for its world."""
    spec = find_spec(name)
    if spec is None:
        raise KeyError(name)
    return ConfigDomain(spec, world)


def session_domain(session: Row) -> ConfigDomain:
    """The domain of a run, built from the spec its session keeps, once per process."""
    key = (session.id, session.world)
    if key not in _by_session:
        if len(_by_session) >= 64:  # a long-lived process, such as the dashboard, sees many runs
            _by_session.clear()
        _by_session[key] = ConfigDomain(
            parse_spec(session.params["spec"], f"session {session.id}"), session.world
        )
    return _by_session[key]
