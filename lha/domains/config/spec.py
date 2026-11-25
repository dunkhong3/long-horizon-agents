"""The configuration a domain is loaded from, which is a TOML file checked here.

A domain loaded from configuration is a pipeline over documents, where
every stage reads each document and reports the fields it asks for, each
with the sentence it comes from, and a stage can wait on the stages before
it, where `needs` says the earlier stage must have read the document and
`after` only says it comes later. The documents are a folder of text files
given with --docs, and the file can also hold documents of its own with the
values they state, which are its mock world, so it runs on the fake model
and is scored like the other domains' mock worlds.

    name = "tickets"
    description = "Sort support tickets and route each one to a team."

    [[stage]]
    name = "classify"
    instructions = "Say what the ticket is about."
      [[stage.field]]
      name = "category"
      one_of = ["bug", "billing", "feature request"]

    [[stage]]
    name = "route"
    needs = ["classify"]
      [[stage.field]]
      name = "team"
      one_of = ["engineering", "finance", "product"]

    [[document]]
    name = "ticket-001.txt"
    text = "Category: billing\\nTeam: finance\\nI was charged twice."
    expect = { category = "billing", team = "finance" }

Everything code can check before a run starts is checked here, such as
names that are unique, stages that wait only on stages before them,
patterns that compile and documents that only state fields that exist, so
a mistake in the file is one message and not a run that goes wrong.
"""

import re
import tomllib
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from lha.domains.research.facts import normal

BUILT_IN = ("audit", "research", "code", "extract")
REPORT_TYPE = "write_report"  # the report's task type, which no stage may be called
NAME = r"^[a-z][a-z0-9-]{0,39}$"  # a domain's name, as in --domain tickets
STAGE = r"^[a-z][a-z0-9_]{0,29}$"  # a stage's or a role's name
MAX_STAGES = 10
MAX_FIELDS = 20  # per stage
MAX_OPTIONS = 50  # per field with one_of
MAX_DOCUMENTS = 100  # of its own, kept with the session in Postgres
MAX_TEXT = 1_000_000  # characters in all the documents of its own
MAX_INSTRUCTIONS = 2_000
GOAL_NAMES = {"n_docs": 1, "where": "a folder", "stages": "a then b"}  # what a goal may name


class SpecError(ValueError):
    """A domain's configuration that can't be loaded, with what is wrong in it."""


class FieldSpec(BaseModel):
    """One value a stage asks for. A value must be whole words of its quote,
    unless the field has `one_of`, which makes it a choice between options
    that the quote only has to support, and a `pattern` is a regular
    expression the whole value must match."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=60)
    form: str = Field("", max_length=300)  # what its value looks like, which the model is shown
    pattern: str | None = Field(None, max_length=300)
    one_of: list[str] | None = Field(None, min_length=1, max_length=MAX_OPTIONS)

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        v = " ".join(v.split())
        if not v or "|" in v:
            raise ValueError("a field's name can't be empty or hold '|'")
        return v

    @field_validator("pattern")
    @classmethod
    def _pattern(cls, v: str | None) -> str | None:
        if v is not None:
            try:
                re.compile(v)
            except re.error as e:
                raise ValueError(f"the pattern {v!r} doesn't compile ({e})") from None
        return v

    @field_validator("one_of")
    @classmethod
    def _options(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        v = [" ".join(o.split()) for o in v]
        if any(not o or len(o) > 100 for o in v) or len({normal(o) for o in v}) != len(v):
            raise ValueError("the options must be distinct, and each 1 to 100 characters")
        return v

    def option(self, value: str) -> str | None:
        """The option a value names, in the option's own spelling, or None."""
        return next((o for o in self.one_of or () if normal(o) == normal(value)), None)

    def problem(self, value: str) -> str | None:
        """Why a value can't be this field's, or None when it can be."""
        if self.one_of is not None and self.option(value) is None:
            return f"{value!r} is not one of the options for {self.name} ({', '.join(self.one_of)})"
        if self.pattern is not None and re.fullmatch(self.pattern, value) is None:
            return f"{value!r} doesn't match the pattern for {self.name} ({self.pattern})"
        return None


class StageSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str = Field(pattern=STAGE)
    role: str | None = Field(None, pattern=STAGE)  # the workers it runs on, by default its name
    instructions: str = Field("", max_length=MAX_INSTRUCTIONS)
    needs: list[str] = []  # stages before it that must have read the document
    after: list[str] = []  # stages before it that only have to be finished
    fields: list[FieldSpec] = Field(alias="field", min_length=1, max_length=MAX_FIELDS)

    @property
    def worker_role(self) -> str:
        return self.role or self.name


class DocumentSpec(BaseModel):
    """A document of the configuration's own, with the values it states
    (`expect`) and the fields it doesn't state (`missing`), which are what a
    run on these documents is scored against."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    text: str = Field(min_length=1)
    expect: dict[str, str] = {}
    missing: list[str] = []

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        if "|" in v or v.startswith("/") or ".." in v.split("/") or v != v.strip():
            raise ValueError(f"{v!r} can't name a document, which needs a plain relative name")
        return v


class ReportSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: str = Field("writer", pattern=STAGE)
    instructions: str = Field("", max_length=MAX_INSTRUCTIONS)


class DomainSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str = Field(pattern=NAME)
    description: str = Field("", max_length=300)
    goal: str = Field("", max_length=500)  # which may name {n_docs}, {where} and {stages}
    stages: list[StageSpec] = Field(alias="stage", min_length=1, max_length=MAX_STAGES)
    report: ReportSpec = ReportSpec()
    documents: list[DocumentSpec] = Field([], alias="document", max_length=MAX_DOCUMENTS)

    @model_validator(mode="after")
    def _check(self) -> "DomainSpec":
        if self.name in BUILT_IN:
            raise ValueError(f"{self.name!r} is a built-in domain's name")
        seen: list[str] = []
        fields: dict[str, str] = {}  # field name, folded -> its stage
        for stage in self.stages:
            if stage.name in seen or stage.name == REPORT_TYPE:
                raise ValueError(f"the stage name {stage.name!r} is used twice or is reserved")
            for dep in [*stage.needs, *stage.after]:
                if dep not in seen:
                    raise ValueError(f"stage {stage.name!r} waits on {dep!r}, which is not a stage before it")
            for f in stage.fields:
                if f.name.casefold() in fields:
                    raise ValueError(f"the field {f.name!r} is asked for twice, and a field has one stage")
                fields[f.name.casefold()] = stage.name
            seen.append(stage.name)
        if self.report.role in {s.worker_role for s in self.stages}:
            raise ValueError(f"the report's role {self.report.role!r} is a stage's role, so give it its own")
        try:
            self.goal.format(**GOAL_NAMES)
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"the goal may only name {{n_docs}}, {{where}} and {{stages}} ({e})") from None
        if sum(len(d.text) for d in self.documents) > MAX_TEXT:
            raise ValueError(f"its own documents hold more than {MAX_TEXT} characters, so use --docs")
        names = [d.name for d in self.documents]
        if len(set(names)) != len(names):
            raise ValueError("two of its documents have the same name")
        for doc in self.documents:
            for name in [*doc.expect, *doc.missing]:
                if name.casefold() not in fields:
                    raise ValueError(f"{doc.name} names the field {name!r}, which no stage asks for")
            if set(doc.expect) & set(doc.missing):
                raise ValueError(
                    f"{doc.name} both states and doesn't state {sorted(set(doc.expect) & set(doc.missing))}"
                )
            for name, value in doc.expect.items():
                if problem := self.field(name).problem(value):
                    raise ValueError(f"{doc.name} expects {problem}")
        return self

    def fields(self) -> list[FieldSpec]:
        return [f for s in self.stages for f in s.fields]

    def field(self, name: str) -> FieldSpec:
        return next(f for f in self.fields() if f.name.casefold() == name.casefold())

    def stage(self, name: str) -> StageSpec:
        return next(s for s in self.stages if s.name == name)

    def roles(self) -> tuple[str, ...]:
        """The stages' roles in order, and the report's last, which gets one worker."""
        return (*dict.fromkeys(s.worker_role for s in self.stages), self.report.role)

    def dump(self) -> dict:
        """The spec as it is kept with a session, which loads back as it was."""
        return self.model_dump(mode="json", by_alias=True)


def load_spec(path: Path) -> DomainSpec:
    """Read and check a domain's configuration, or raise SpecError with what is wrong."""
    try:
        data = tomllib.loads(path.read_text())
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as e:
        raise SpecError(f"{path}: {e}") from None
    return parse_spec(data, str(path))


def parse_spec(data: dict, where: str = "the configuration") -> DomainSpec:
    try:
        return DomainSpec.model_validate(data)
    except ValidationError as e:
        problems = "; ".join(
            (".".join(str(p) for p in err["loc"]) + " " if err["loc"] else "")
            + err["msg"].removeprefix("Value error, ")
            for err in e.errors()
        )
        raise SpecError(f"{where}: {problems}") from None
