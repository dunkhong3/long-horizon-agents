"""The audit's task types, with their inputs, outputs, keys and scopes.

These Pydantic models are the contract between processes. A worker's result
must validate against the output model of its task type before the
coordinator even looks at it. Which role each type belongs to is in
lha/domains/audit/__init__.py.
"""

from pydantic import BaseModel, Field

from lha.domains.audit.facts import DRIFT, VERIFIED, host_subject, registry_subject, service_subject

# --- inputs -----------------------------------------------------------------


class DiscoverInput(BaseModel):
    host: str
    round: int = 1
    # Set when a host was too big for one attempt and its discovery was split
    # into batches, as part `part` of `parts`. A batch reads exactly the
    # services and documents it lists (None means everything the host lists),
    # and a document too big for one attempt is split again into single pages.
    services: list[str] | None = None
    documents: list[str] | None = None
    page: int | None = None
    part: int = 0
    parts: int = 0


class CompareInput(BaseModel):
    service: str
    host: str
    round: int = 1


class VerifyInput(BaseModel):
    service: str
    host: str
    round: int = 1


class ReportInput(BaseModel):
    partial: bool = False  # True when the run ended without meeting the goal


# --- outputs ----------------------------------------------------------------
# Every fact a worker reports cites its source, which is the id of the
# tool-call event it was read from or the ids of the facts it compared. The
# coordinator checks those citations before committing anything.


class ServiceRead(BaseModel):
    service: str
    replicas: int = Field(ge=0)
    event_id: str


class DocumentRead(BaseModel):
    """One page of a document, which for most documents is the whole of it."""

    name: str
    page: int = Field(ge=0)
    pages: int = Field(ge=1)
    mentions: list[str]  # hosts named on this page
    registry: dict[str, int] | None = None  # only for registry.json
    event_id: str


class DiscoverOutput(BaseModel):
    host: str
    host_event_id: str
    services: list[ServiceRead]
    documents: list[DocumentRead]


class CompareOutput(BaseModel):
    service: str
    host: str
    expected: int
    actual: int
    drift: bool
    read_fact_id: str
    registry_fact_id: str


class VerifyOutput(BaseModel):
    service: str
    host: str
    replicas: int = Field(ge=0)
    event_id: str


class Finding(BaseModel):
    service: str
    host: str
    expected: int
    actual: int
    fact_id: str  # the verified drift fact this finding rests on


class ReportOutput(BaseModel):
    findings: list[Finding]  # one per verified drift, possibly none
    summary: str  # markdown


# --- keys and scopes ----------------------------------------------------------


def task_key(task_type: str, inp: BaseModel) -> str:
    """A stable, readable name for a task, in the form `<type>:<what it's about>[#round]`.

    It is the same in every run, so it stops the same task being created
    twice and it seeds the faults. Round 1 has no suffix, and a deliberate new
    round of the same work gets '#n'. A batch of a split discovery adds
    '/<part>of<parts>' before the round, as in 'discover_host:host-7/2of4',
    and a single page of a document adds '/p<page>' after that.
    """
    if isinstance(inp, DiscoverInput):
        base = f"discover_host:{inp.host}" + (f"/{inp.part}of{inp.parts}" if inp.parts else "")
        base += f"/p{inp.page}" if inp.page is not None else ""
    elif isinstance(inp, CompareInput):
        base = f"compare_service:{inp.service}@{inp.host}"
    elif isinstance(inp, VerifyInput):
        # Verify rounds always carry their number (#1, #2, ...).
        return f"verify_drift:{inp.service}@{inp.host}#{inp.round}"
    elif isinstance(inp, ReportInput):
        base = "write_report"
    else:
        raise TypeError(f"unknown input {inp!r}")
    rnd = getattr(inp, "round", 1)
    return base if rnd == 1 else f"{base}#{rnd}"


def task_scope(inp: BaseModel) -> list[str]:
    """Which facts the context builder selects for this task.

    Entries are exact subjects, '*@<host>' for everything on a host, or for
    the reporter every verified drift (see lha/core/context.py).
    """
    if isinstance(inp, DiscoverInput) and inp.services is not None:
        # A batch only needs its own services, which leaves room in its
        # packet for pointers to what the too-big attempt already read.
        return [host_subject(inp.host), *(service_subject(s, inp.host) for s in inp.services)]
    if isinstance(inp, DiscoverInput):
        return [host_subject(inp.host), f"*@{inp.host}"]
    if isinstance(inp, (CompareInput, VerifyInput)):
        return [service_subject(inp.service, inp.host), registry_subject(inp.service)]
    if isinstance(inp, ReportInput):
        return [f"key:{DRIFT}={VERIFIED}"]
    raise TypeError(f"unknown input {inp!r}")
