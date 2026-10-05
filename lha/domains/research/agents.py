"""The research brief's three agents, each a policy, its tools and a fabricated variant.

The reader reads one source and reports every launch year it states and
every source it cites. The analyst reconciles one project's claims, from
the facts in its packet, and makes no call at all, and it settles on a year
only when two or more sources agree and outnumber every other year. The
writer runs once, at the end, and writes the brief from the verified
answers only.
"""

from collections import Counter
from typing import Any

from lha.core.agent import AgentContext, Call, Final, run_agent
from lha.core.schemas import VERIFIED, ContextPacket
from lha.domains.research.facts import YEAR
from lha.domains.research.tasks import BriefOutput, ReadOutput, ReconcileOutput
from lha.domains.research.world import cites_in, claims_in

# --- the reader -----------------------------------------------------------------------


def read_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
    source = packet.pinned.input["source"]
    if not results:
        earlier = {p.path: p.id for p in packet.pointers}
        path = f"/sources/{source}"
        if path in earlier:
            return Call("fetch_pointer", {"event_id": earlier[path]})
        return Call("get_source", {"source": source})
    r = results[0][1]
    claims = [{"project": p, "year": y, "quote": q} for p, y, q in claims_in(r["text"])]
    return Final(
        {"source": source, "event_id": r["event_id"], "claims": claims, "cites": cites_in(r["text"], source)}
    )


def read_fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    """A believable lie, either a year off by one or a source the text doesn't cite."""
    if output["claims"]:
        claims = [dict(c) for c in output["claims"]]
        claims[0]["year"] += 1
        return {**output, "claims": claims}
    return {**output, "cites": [*output["cites"], "src-999"]}


async def run_read(ctx: AgentContext) -> ReadOutput:
    async def get_source(source: str) -> dict:
        """Read a source from the library."""
        return await ctx.tools.get_source(source)

    async def fetch_pointer(event_id: str) -> dict:
        """Fetch an earlier attempt's raw tool output by its pointer."""
        return await ctx.tools.fetch_pointer(event_id)

    return await run_agent(ctx, ReadOutput, read_policy, read_fabricate, [get_source, fetch_pointer])


# --- the analyst ----------------------------------------------------------------------


def reconcile_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    project = packet.pinned.input["project"]
    claims = [f for f in packet.facts if f.subject.startswith(f"claim:{project}@") and f.key == YEAR]
    ranked = Counter(f.value for f in claims).most_common()
    settled = len(ranked) > 0 and ranked[0][1] >= 2 and (len(ranked) == 1 or ranked[0][1] > ranked[1][1])
    if not settled:
        return Final({"project": project, "year": None, "claim_fact_ids": []})
    year = ranked[0][0]
    return Final(
        {"project": project, "year": year, "claim_fact_ids": [f.id for f in claims if f.value == year]}
    )


def reconcile_fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    if output["year"] is None:
        return None
    return {**output, "year": output["year"] + 1}


async def run_reconcile(ctx: AgentContext) -> ReconcileOutput:
    return await run_agent(ctx, ReconcileOutput, reconcile_policy, reconcile_fabricate)


# --- the writer -----------------------------------------------------------------------


def brief_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    answers = sorted(
        (
            f
            for f in packet.facts
            if f.key == YEAR and f.status == VERIFIED and f.subject.startswith("answer:")
        ),
        key=lambda f: f.subject,
    )
    findings = [
        {"project": f.subject.removeprefix("answer:"), "year": f.value, "fact_id": f.id} for f in answers
    ]
    if findings:
        lines = ["# Brief: when the projects launched", ""]
        lines += [
            f"- **{a['project']}** launched in **{a['year']}** (verified answer `{a['fact_id']}`)."
            for a in findings
        ]
        lines += ["", "Each year is stated by at least two sources that agree and outnumber any other year."]
    else:
        lines = ["# Brief", "", "No launch year could be confirmed by agreeing sources in this run."]
    return Final({"findings": findings, "summary": "\n".join(lines) + "\n"})


def brief_fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    if not output["findings"]:
        return None
    findings = [dict(a) for a in output["findings"]]
    findings[0]["year"] += 1
    return {**output, "findings": findings}


async def run_brief(ctx: AgentContext) -> BriefOutput:
    return await run_agent(ctx, BriefOutput, brief_policy, brief_fabricate)
