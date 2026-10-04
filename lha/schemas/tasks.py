"""Task types: their inputs, outputs, roles, keys and scopes.

These Pydantic models are the contract between processes. A worker's result
must validate against the output model of its task type before the
coordinator even looks at it.
"""

from typing import Literal

from pydantic import BaseModel, Field

from lha.schemas.facts import host_subject, registry_subject, service_subject

TaskType = Literal["discover_host", "compare_service", "verify_drift", "write_report"]

# Each task type belongs to exactly one role; workers only claim their role.
ROLE_OF: dict[str, str] = {
    "discover_host": "discovery",
    "compare_service": "analysis",
    "verify_drift": "analysis",
    "write_report": "reporter",
}

# --- inputs -----------------------------------------------------------------


class DiscoverInput(BaseModel):
    host: str
    round: int = 1


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


INPUT_MODELS: dict[str, type[BaseModel]] = {
    "discover_host": DiscoverInput,
    "compare_service": CompareInput,
    "verify_drift": VerifyInput,
    "write_report": ReportInput,
}

# --- outputs ----------------------------------------------------------------
# Every fact a worker reports cites its source: the id of the tool-call event
# it was read from, or the ids of the facts it compared. The coordinator
# checks those citations before committing anything.


class ServiceRead(BaseModel):
    service: str
    replicas: int = Field(ge=0)
    event_id: str


class DocumentRead(BaseModel):
    name: str
    mentions: list[str]  # hosts named in the document
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


class ReportOutput(BaseModel):
    service: str | None
    host: str | None
    expected: int | None
    actual: int | None
    drift_fact_id: str | None
    summary: str  # markdown


OUTPUT_MODELS: dict[str, type[BaseModel]] = {
    "discover_host": DiscoverOutput,
    "compare_service": CompareOutput,
    "verify_drift": VerifyOutput,
    "write_report": ReportOutput,
}

# --- keys and scopes ----------------------------------------------------------


def task_key(task_type: str, inp: BaseModel) -> str:
    """A stable, readable name for a task: `<type>:<what it's about>[#round]`.

    It's the same in every run, so it dedups task creation and seeds faults.
    Round 1 has no suffix; a deliberate new round of the same work gets "#n".
    """
    if isinstance(inp, DiscoverInput):
        base = f"discover_host:{inp.host}"
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

    Entries are exact subjects, "*@<host>" for everything on a host, or
    "verified_drifts" for the reporter.
    """
    if isinstance(inp, DiscoverInput):
        return [host_subject(inp.host), f"*@{inp.host}"]
    if isinstance(inp, (CompareInput, VerifyInput)):
        return [service_subject(inp.service, inp.host), registry_subject(inp.service)]
    if isinstance(inp, ReportInput):
        return ["verified_drifts"]
    raise TypeError(f"unknown input {inp!r}")
