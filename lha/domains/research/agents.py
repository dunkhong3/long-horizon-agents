"""The research agents, each with its instructions, its fake policy, its tools and a fabricated variant.

The planner turns the question into cells, which are the attributes to find
for each subject, and the first searches. The searcher runs one search and
picks the results worth reading. The reader reads one page, or one page of a
long document, and reports what it states about the cells, quoting each
sentence word for word, and the links worth following. The analyst works out
a cell's value from the claims gathered so far, and the writer writes the
report from the verified answers only.

On the real web a real model searches with its provider's own web search,
and the fake policies here only ever run on the mock web, whose pages state
their facts in sentences the policies can parse.
"""

import re
from collections import Counter
from typing import Any

from pydantic_ai.capabilities.web_search import WebSearch

from lha.core.agent import AgentContext, Call, Final, run_agent
from lha.core.schemas import VERIFIED, ContextPacket
from lha.domains.research.facts import CELLS, PLAN, VALUE, normal, split_cell
from lha.domains.research.tasks import (
    PlanOutput,
    ReadOutput,
    ReconcileOutput,
    ReportOutput,
    SearchOutput,
    page_path,
)
from lha.domains.research.world import claims_in, links_in

# --- the planner ----------------------------------------------------------------------

PLAN_INSTRUCTIONS = """\
Turn the research question into a plan. Name the subjects it is about and
the attributes to find for each of them, where each attribute has a short
name and the form its value takes (such as 'a year' or 'an SPDX license
id'), so values from different pages can be compared. Then write up to a
dozen web searches that would find pages stating those values. Keep the plan
to what the question asks. Make no tool calls.
"""

QUESTION = re.compile(r"Find the (?P<attributes>.+) of each of these projects: (?P<projects>.+)\.")


def plan_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    m = QUESTION.fullmatch(packet.pinned.input["question"].strip())
    subjects = m.group("projects").split(", ") if m else []
    attributes = m.group("attributes").split(" and the ") if m else []
    queries = [f"{s} {a}" for s in subjects for a in attributes]
    return Final(
        {
            "subjects": subjects,
            "attributes": [{"name": a, "form": ""} for a in attributes],
            "queries": queries,
        }
    )


async def run_plan(ctx: AgentContext) -> PlanOutput:
    # A plan can't be checked against a source, so a fabricated one is never
    # made, and a broken answer is a malformed one instead.
    return await run_agent(ctx, PlanOutput, plan_policy, lambda output: None)


# --- the searcher ---------------------------------------------------------------------

SEARCH_INSTRUCTIONS = """\
Run the web search in your input, and answer with the results worth reading
for the plan in your prompt, each with its URL exactly as the search returned
it and its title. Leave out results that can't hold any of the planned
values.
"""


def search_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
    query = packet.pinned.input["query"]
    if not results:
        return Call("web_search", {"query": query})
    hits = results[0][1]["results"]
    return Final({"query": query, "results": [{"url": h["url"], "title": h["title"]} for h in hits]})


def search_fabricate(output: dict[str, Any]) -> dict[str, Any]:
    """A believable lie, which is a page the search never returned."""
    made_up = {"url": "https://made-up.example/the-answer", "title": "The answer"}
    return {**output, "results": [*output["results"], made_up]}


async def run_search(ctx: AgentContext) -> SearchOutput:
    async def web_search(query: str) -> dict:
        """Search the web, and get back the matching pages' URLs and titles."""
        return await ctx.tools.web_search(query)

    if ctx.world == "web":
        # A real model uses its provider's own search, whose results the
        # call wrapper logs, so the coordinator can check every URL.
        return await run_agent(ctx, SearchOutput, search_policy, search_fabricate, capabilities=[WebSearch()])
    return await run_agent(ctx, SearchOutput, search_policy, search_fabricate, [web_search])


# --- the reader -----------------------------------------------------------------------

READ_INSTRUCTIONS = """\
Read the page in your input with fetch_page, every page of it from page 0
unless your input names one page, and fetch by pointer anything an earlier
attempt already read. Report every value it states for the planned cells in
your prompt, each with the subject and attribute exactly as the plan names
them, the value, the sentence it comes from copied word for word, and the
event_id of the fetch it was read in. Add the links on the page that look
likely to state planned values. Report nothing the page doesn't say.
"""


def read_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
    inp = packet.pinned.input
    url, only = inp["url"], inp.get("page")
    earlier = {p.path: p.id for p in packet.pointers}
    fetched = {r["page"]: r for tool, r in results if tool == "fetch_page"}
    wanted = [only] if only is not None else [0]
    if only is None and 0 in fetched:
        wanted = list(range(fetched[0]["pages"]))
    for page in wanted:
        if page not in fetched:
            path = page_path(url, page)
            if path in earlier:
                return Call("fetch_pointer", {"event_id": earlier[path]})
            return Call("fetch_page", {"url": url, "page": page})

    cells = _cells(packet)
    claims, links = [], []
    for page in wanted:
        r = fetched[page]
        for subject, attribute, value, sentence in claims_in(r["text"]):
            if (subject, attribute) in cells:
                claims.append(
                    {"subject": subject, "attribute": attribute, "value": value, "quote": sentence,
                     "event_id": r["event_id"]}
                )  # fmt: skip
        links += [link for link in links_in(r["text"]) if link not in links]
    return Final({"url": url, "claims": claims, "links": links})


def read_fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    """A believable lie, either a value the quote doesn't hold or a link the page doesn't have."""
    if output["claims"]:
        claims = [dict(c) for c in output["claims"]]
        claims[0]["value"] = "1999" if claims[0]["value"] != "1999" else "2001"
        return {**output, "claims": claims}
    return {**output, "links": [*output["links"], "https://made-up.example/more"]}


async def run_read(ctx: AgentContext) -> ReadOutput:
    async def fetch_page(url: str, page: int = 0) -> dict:
        """Read one page of a web page's text, where page 0 also says how many pages there are."""
        return await ctx.tools.fetch_page(url, page)

    async def fetch_pointer(event_id: str) -> dict:
        """Fetch an earlier attempt's raw tool output by its pointer."""
        return await ctx.tools.fetch_pointer(event_id)

    return await run_agent(ctx, ReadOutput, read_policy, read_fabricate, [fetch_page, fetch_pointer])


def _cells(packet: ContextPacket) -> set[tuple[str, str]]:
    plan = next((f.value for f in packet.facts if f.subject == PLAN and f.key == CELLS), None)
    if plan is None:
        return set()
    return {(s, a["name"]) for s in plan["subjects"] for a in plan["attributes"]}


# --- the analyst ----------------------------------------------------------------------

RECONCILE_INSTRUCTIONS = """\
Work out one cell's value from the claim facts in your prompt. Each claim
names the page's source, and two pages from one source count once. A value
counts only when two or more sources agree on it and outnumber every other
value. Answer with that value, or null when the claims don't settle it yet,
and the ids of the claim facts that agree on it. Make no tool calls.
"""


def reconcile_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    inp = packet.pinned.input
    claims = [f for f in packet.facts if f.subject.startswith("claim:") and f.key == VALUE]
    sources: dict[str, set[str]] = {}
    shown: dict[str, str] = {}
    for f in claims:
        key = normal(f.value["value"])
        sources.setdefault(key, set()).add(f.value["source"])
        shown.setdefault(key, f.value["value"])
    ranked = Counter({k: len(v) for k, v in sources.items()}).most_common()
    settled = ranked and ranked[0][1] >= 2 and (len(ranked) == 1 or ranked[0][1] > ranked[1][1])
    out = {"subject": inp["subject"], "attribute": inp["attribute"], "value": None, "claim_fact_ids": []}
    if settled:
        winner = ranked[0][0]
        out["value"] = shown[winner]
        out["claim_fact_ids"] = [f.id for f in claims if normal(f.value["value"]) == winner]
    return Final(out)


def reconcile_fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    if output["value"] is None:
        return None
    return {**output, "value": f"{output['value']}0"}


async def run_reconcile(ctx: AgentContext) -> ReconcileOutput:
    return await run_agent(ctx, ReconcileOutput, reconcile_policy, reconcile_fabricate)


# --- the writer -----------------------------------------------------------------------

REPORT_INSTRUCTIONS = """\
Write the report from the verified answer facts in your prompt. List every
verified answer with its subject, attribute, value and the id of its fact,
and write a short markdown summary that answers the question from those
values only. The sources are added under it by code. Make no tool calls.
"""


def report_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    answers = sorted(
        (
            f
            for f in packet.facts
            if f.key == VALUE and f.status == VERIFIED and f.subject.startswith("answer:")
        ),
        key=lambda f: f.subject,
    )
    out = []
    for f in answers:
        subject, attribute = split_cell(f.subject.removeprefix("answer:"))
        out.append({"subject": subject, "attribute": attribute, "value": f.value, "fact_id": f.id})
    if out:
        lines = ["# Research report", ""]
        lines += [f"- The {a['attribute']} of **{a['subject']}** is **{a['value']}**." for a in out]
    else:
        lines = ["# Research report", "", "No value could be confirmed by agreeing sources in this run."]
    return Final({"findings": out, "summary": "\n".join(lines) + "\n"})


def report_fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    if not output["findings"]:
        return None
    findings = [dict(a) for a in output["findings"]]
    findings[0]["value"] = f"{findings[0]['value']}0"
    return {**output, "findings": findings}


async def run_report(ctx: AgentContext) -> ReportOutput:
    return await run_agent(ctx, ReportOutput, report_policy, report_fabricate)
