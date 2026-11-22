"""The extraction agents, each with its instructions, its fake policy, its tools and a fabricated variant.

The reader reads one document, every page of it unless its input names one
page, and reports each field's value with the sentence it comes from, or
that the document doesn't state it. The writer writes a short summary, and
the table is added by code. The fake reader only ever runs on the mock
documents, whose fields are stated in sentences it can parse.
"""

import re
from typing import Any

from lha.core.agent import AgentContext, Call, Final, run_agent
from lha.core.schemas import ContextPacket
from lha.domains.extract.facts import COUNTS, PROGRESS
from lha.domains.extract.tasks import ExtractOutput, TableOutput
from lha.domains.extract.world import TEMPLATES

# --- the reader -----------------------------------------------------------------------

EXTRACT_INSTRUCTIONS = """\
Read the document in your input with read_document, every page of it from
page 0 unless your input names one page, and fetch by pointer anything an
earlier attempt already read. For each field in your input,
report its value as the document states it, with the sentence it comes from
copied word for word and the event_id of the read it is in, or a null value
when the document, or that page of it, doesn't state it. A value must be the
field itself, and not something that only looks like it, such as the number
of another document it mentions. Report nothing the document doesn't say.
"""

PATTERNS = {
    f: [re.compile(re.escape(t).replace(r"\{v\}", "(?P<v>.+?)")) for t in templates]
    for f, templates in TEMPLATES.items()
}


def extract_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
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
    for f in inp["fields"]:
        found = next(
            (
                {"field": f, "value": m.group("v"), "quote": line, "event_id": fetched[page]["event_id"]}
                for page in wanted
                for line in (x.strip() for x in fetched[page]["text"].split("\n"))
                for pattern in PATTERNS.get(f, ())
                if (m := pattern.fullmatch(line))
            ),
            None,
        )
        values.append(found or {"field": f, "value": None})
    return Final({"doc": doc, "values": values})


def extract_fabricate(output: dict[str, Any]) -> dict[str, Any]:
    """A believable lie, which is a value its quote doesn't hold, or a sentence the document doesn't have."""
    values = [dict(v) for v in output["values"]]
    stated = [v for v in values if v["value"] is not None]
    if stated:
        stated[0]["value"] = "INV-1999-0001" if stated[0]["value"] != "INV-1999-0001" else "INV-1999-0002"
    elif values:
        values[0].update(value="INV-1999-0001", quote="Invoice number: INV-1999-0001.", event_id="")
    return {**output, "values": values}


async def run_extract(ctx: AgentContext) -> ExtractOutput:
    async def read_document(doc: str, page: int = 0) -> dict:
        """Read one page of a document, where page 0 also says how many pages it has."""
        return await ctx.tools.read_document(doc, page)

    async def fetch_pointer(event_id: str) -> dict:
        """Fetch an earlier attempt's raw tool output by its pointer."""
        return await ctx.tools.fetch_pointer(event_id)

    return await run_agent(
        ctx, ExtractOutput, extract_policy, extract_fabricate, [read_document, fetch_pointer]
    )


# --- the writer -----------------------------------------------------------------------

TABLE_INSTRUCTIONS = """\
Write a short markdown summary of the extraction from the counts in your
prompt. The table itself is added under it by code. Make no tool calls.
"""


def table_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    counts = next((f.value for f in packet.facts if f.subject == PROGRESS and f.key == COUNTS), None) or {}
    summary = (
        "# Extraction report\n\n"
        f"Read {counts.get('done', 0)} of {counts.get('docs', 0)} document(s) and found "
        f"{counts.get('values', 0)} value(s), each quoted from its document.\n"
    )
    return Final({"summary": summary})


async def run_table(ctx: AgentContext) -> TableOutput:
    return await run_agent(ctx, TableOutput, table_policy, lambda output: None)
