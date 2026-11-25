"""The agents of a domain loaded from configuration, with instructions built from its spec.

Every stage has one reader, whose instructions are the same core rules for
reading and quoting, then the stage's own instructions from the file, then
a line for each of its fields. The fake reader finds lines of the form
'Label: value', where the label is the field's name, which is how the
configuration's own documents state their values, so it only ever runs on
those, like the other domains' fake models only run on their mock worlds.
The writer writes a short summary from the counts, and the table is added
by code.
"""

import re
from typing import Any

from lha.core.agent import AgentContext, Call, Final, run_agent
from lha.core.schemas import ContextPacket
from lha.domains.config.facts import COUNTS, PROGRESS
from lha.domains.config.spec import DomainSpec, FieldSpec, StageSpec
from lha.domains.config.tasks import ReportOutput, StageOutput

# --- the readers ----------------------------------------------------------------------

READ_INSTRUCTIONS = """\
Read the document in your input with read_document, every page of it from
page 0 unless your input names one page, and fetch by pointer anything an
earlier attempt already read. For each field in your input, report its
value with the sentence it comes from copied word for word and the event_id
of the read it is in, or a null value when the document, or that page of
it, doesn't state it. Report nothing the document doesn't say. What earlier
stages found in this document is in your prompt, among the facts.
"""


def stage_instructions(spec: DomainSpec, stage: StageSpec) -> str:
    parts = [READ_INSTRUCTIONS.rstrip()]
    if stage.instructions.strip():
        parts.append(" ".join(stage.instructions.split()))
    parts.append("The fields are these.\n" + "\n".join(_describe(f) for f in stage.fields))
    return "\n\n".join(parts) + "\n"


def _describe(f: FieldSpec) -> str:
    line = f"- {f.name}" + (f", {f.form}" if f.form else "")
    if f.one_of:
        line += (
            f". It is one of {', '.join(repr(o) for o in f.one_of)}, which may be your judgement, "
            "with the sentence it rests on as its quote"
        )
    else:
        line += ", in the words of its quote"
    if f.pattern:
        line += f", and the whole value matches the regular expression {f.pattern}"
    return line + "."


def stage_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
    inp = packet.pinned.input
    doc, only = inp["doc"], inp.get("page")
    earlier = {p.path: p.id for p in packet.pointers}
    fetched = {
        r["page"]: r for tool, r in results if tool in ("read_document", "fetch_pointer") and "text" in r
    }
    wanted = [only] if only is not None else [0]
    if only is None and 0 in fetched:
        wanted = list(range(fetched[0]["pages"]))
    for page in wanted:
        if page not in fetched:
            path = f"read_document:{doc}#{page}"
            if path in earlier:  # an earlier attempt already read it
                return Call("fetch_pointer", {"event_id": earlier[path]})
            return Call("read_document", {"doc": doc, "page": page})
    values = []
    for name in inp["fields"]:
        label = re.compile(rf"\s*{re.escape(name)}\s*:\s*(?P<v>.+?)\s*\.?\s*", re.IGNORECASE)
        found = next(
            (
                {"field": name, "value": m.group("v"), "quote": line, "event_id": fetched[page]["event_id"]}
                for page in wanted
                for line in (x.strip() for x in fetched[page]["text"].split("\n"))
                if (m := label.fullmatch(line))
            ),
            None,
        )
        values.append(found or {"field": name, "value": None})
    return Final({"doc": doc, "values": values})


def stage_fabricate(output: dict[str, Any]) -> dict[str, Any]:
    """A believable lie, which is a value its quote doesn't hold, or a sentence the document doesn't have."""
    values = [dict(v) for v in output["values"]]
    stated = [v for v in values if v["value"] is not None]
    if stated:
        stated[0]["value"] = f"{stated[0]['value']} and more"
    elif values:
        values[0].update(value="unknown", quote=f"{values[0]['field']}: unknown", event_id="")
    return {**output, "values": values}


async def run_stage(ctx: AgentContext) -> StageOutput:
    async def read_document(doc: str, page: int = 0) -> dict:
        """Read one page of a document, where page 0 also says how many pages it has."""
        return await ctx.tools.read_document(doc, page)

    async def fetch_pointer(event_id: str) -> dict:
        """Fetch an earlier attempt's raw tool output by its pointer."""
        return await ctx.tools.fetch_pointer(event_id)

    return await run_agent(ctx, StageOutput, stage_policy, stage_fabricate, [read_document, fetch_pointer])


# --- the writer -----------------------------------------------------------------------

REPORT_INSTRUCTIONS = """\
Write a short markdown summary of the run from the counts in your prompt.
The table itself is added under it by code. Make no tool calls.
"""


def report_instructions(spec: DomainSpec) -> str:
    extra = " ".join(spec.report.instructions.split())
    return REPORT_INSTRUCTIONS + (f"\n{extra}\n" if extra else "")


def report_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    counts = next((f.value for f in packet.facts if f.subject == PROGRESS and f.key == COUNTS), None) or {}
    summary = (
        "# Report\n\n"
        f"Read {counts.get('done', 0)} of {counts.get('items', 0)} stage(s) of "
        f"{counts.get('docs', 0)} document(s) and found {counts.get('values', 0)} value(s), "
        "each quoted from its document.\n"
    )
    return Final({"summary": summary})


async def run_report(ctx: AgentContext) -> ReportOutput:
    return await run_agent(ctx, ReportOutput, report_policy, lambda output: None)
