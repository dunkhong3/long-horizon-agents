"""The audit's three agents, each a policy, its tools and a fabricated variant.

Discovery is the 'breadth' role. It reads one host, its services and its
documents (page by page), reports what exists and which other hosts the
documents mention, and decides nothing about drift.

Analysis is the 'depth' role, and it handles two task types. The first is
compare_service, which makes no network call at all and compares the
discovered replica count with the registry entry, both taken from the facts
in the ContextPacket. The second is verify_drift, which is a fresh read of a
service we suspect has drifted, and it only reports the number it saw,
because deciding what that number means is the coordinator's job.

The reporter runs once, at the end. Its ContextPacket contains only verified
drift facts plus the registry entry and latest read behind each one, and it
reports every verified drift, which is one for the default goal and all of
them for the 'find all drifts' goal.
"""

import re
from typing import Any

from lha.core.agent import AgentContext, Call, Final, run_agent
from lha.core.schemas import ContextPacket
from lha.domains.audit.facts import (
    DRIFT,
    EXPECTED,
    REPLICAS,
    VERIFIED,
    registry_subject,
    service_subject,
    split_service_subject,
)
from lha.domains.audit.tasks import CompareOutput, DiscoverOutput, ReportOutput, VerifyOutput
from lha.domains.audit.world import REGISTRY_DOC, registry_entries

HOST_NAME = re.compile(r"\bhost-\d+\b")


# --- discovery ---------------------------------------------------------------------

DISCOVER_INSTRUCTIONS = """\
You explore one host of a cloud deployment. Read it with get_host, then read
every service it lists with get_service and every page of every document it
lists with fetch_document, where page 0 says how many pages there are. If
your input names services, documents or a page, read only those. When a
pointer in your prompt already holds a read, fetch it with fetch_pointer
instead of calling the network again. Report exactly what you read, each
value with the event_id of the call it came from, and for each page the
hosts it mentions and, for registry.json, its entries. Never guess a value.
"""


def discover_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
    inp = packet.pinned.input
    host = inp["host"]
    # Raw outputs of earlier attempts, by path, which are fetched from
    # Postgres by pointer instead of being read from the network again.
    earlier = {p.path: p.id for p in packet.pointers}

    def call(tool: str, path: str, args: dict[str, Any]) -> Call:
        if path in earlier:
            return Call("fetch_pointer", {"event_id": earlier[path]})
        return Call(tool, args)

    if not results:
        return call("get_host", f"/hosts/{host}", {"host": host})

    info = results[0][1]
    reads = {r["service"]: r for name, r in results if name == "get_service"}
    pages = {(r["name"], r["page"]): r for name, r in results if name == "fetch_document"}
    # A batch of a split discovery reads only what it lists.
    services = inp["services"] if inp.get("services") is not None else info["services"]
    documents = inp["documents"] if inp.get("documents") is not None else info["documents"]

    # One call at a time, first every service, then every page of every document.
    for service in services:
        if service not in reads:
            path = f"/hosts/{host}/services/{service}"
            return call("get_service", path, {"host": host, "service": service})
    for doc in documents:
        if inp.get("page") is not None:
            wanted = [inp["page"]]
        elif (doc, 0) in pages:
            wanted = range(pages[(doc, 0)]["pages"])
        else:
            wanted = [0]  # the first page says how many there are
        for page in wanted:
            if (doc, page) not in pages:
                path = f"/hosts/{host}/documents/{doc}?page={page}"
                return call("fetch_document", path, {"host": host, "name": doc, "page": page})

    found = []
    for (name, page), d in pages.items():
        mentions = sorted(set(HOST_NAME.findall(d["content"])) - {host})
        registry = None
        if name == REGISTRY_DOC:
            registry = [{"service": k, "replicas": n} for k, n in registry_entries(d["content"]).items()]
        found.append({"name": name, "page": page, "pages": d["pages"], "mentions": mentions,
                      "registry": registry, "event_id": d["event_id"]})  # fmt: skip
    return Final(
        {
            "host": host,
            "host_event_id": info["event_id"],
            "services": [
                {"service": s, "replicas": r["replicas"], "event_id": r["event_id"]} for s, r in reads.items()
            ],
            "documents": found,
        }
    )


def discover_fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    """A believable lie, either a wrong replica count or a host no document names."""
    if output["services"]:
        services = [dict(s) for s in output["services"]]
        services[0]["replicas"] += 1
        return {**output, "services": services}
    if output["documents"]:
        documents = [dict(d) for d in output["documents"]]
        documents[0]["mentions"] = [*documents[0]["mentions"], "host-999"]
        return {**output, "documents": documents}
    return None


async def run_discover(ctx: AgentContext) -> DiscoverOutput:
    async def get_host(host: str) -> dict:
        """Read a host, meaning its services and documents."""
        return await ctx.tools.get_host(host)

    async def get_service(host: str, service: str) -> dict:
        """Read a service's current replica count."""
        return await ctx.tools.get_service(host, service)

    async def fetch_document(host: str, name: str, page: int = 0) -> dict:
        """Read one page of a document stored on a host."""
        return await ctx.tools.fetch_document(host, name, page)

    async def fetch_pointer(event_id: str) -> dict:
        """Fetch an earlier attempt's raw tool output by its pointer."""
        return await ctx.tools.fetch_pointer(event_id)

    tools = [get_host, get_service, fetch_document, fetch_pointer]
    return await run_agent(ctx, DiscoverOutput, discover_policy, discover_fabricate, tools)


# --- analysis ----------------------------------------------------------------------

COMPARE_INSTRUCTIONS = """\
Compare one service's replica count with the registry, using only the facts
in your prompt, which are the service's config.replicas fact and its
registry entry. Report both numbers, whether they differ, and the ids of the
two facts you used. Make no tool calls.
"""

VERIFY_INSTRUCTIONS = """\
Read one service again with get_service, which is a fresh read, and report
the replica count you saw with the event_id of that call. Do not use
pointers, and decide nothing about drift.
"""


def compare_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    service, host = packet.pinned.input["service"], packet.pinned.input["host"]
    read = _find(packet, service_subject(service, host), REPLICAS)
    expected = _find(packet, registry_subject(service), EXPECTED)
    if read is None or expected is None:
        # The context is missing a fact, so we answer honestly with what we
        # have, and the coordinator's checks reject it and retry the task.
        return Final({"service": service, "host": host, "expected": -1, "actual": -1,
                      "drift": False, "read_fact_id": "", "registry_fact_id": ""})  # fmt: skip
    return Final(
        {
            "service": service,
            "host": host,
            "expected": expected.value,
            "actual": read.value,
            "drift": read.value != expected.value,
            "read_fact_id": read.id,
            "registry_fact_id": expected.id,
        }
    )


def compare_fabricate(output: dict[str, Any]) -> dict[str, Any]:
    return {**output, "actual": output["actual"] + 1}


def verify_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
    service, host = packet.pinned.input["service"], packet.pinned.input["host"]
    if not results:
        return Call("get_service", {"host": host, "service": service})
    read = results[-1][1]
    return Final(
        {"service": service, "host": host, "replicas": read["replicas"], "event_id": read["event_id"]}
    )


def verify_fabricate(output: dict[str, Any]) -> dict[str, Any]:
    return {**output, "replicas": output["replicas"] + 1}


def _find(packet: ContextPacket, subject: str, key: str):
    return next((f for f in packet.facts if f.subject == subject and f.key == key), None)


async def run_compare(ctx: AgentContext) -> CompareOutput:
    return await run_agent(ctx, CompareOutput, compare_policy, compare_fabricate)


async def run_verify(ctx: AgentContext) -> VerifyOutput:
    async def get_service(host: str, service: str) -> dict:
        """Read a service's current replica count."""
        return await ctx.tools.get_service(host, service)

    return await run_agent(ctx, VerifyOutput, verify_policy, verify_fabricate, [get_service])


# --- the reporter --------------------------------------------------------------------

REPORT_INSTRUCTIONS = """\
Write the final finding from the verified drift facts in your prompt. List
every verified drift with its service, host, expected and actual counts and
the id of its fact, and add a short markdown summary. Use verified facts
only. Make no tool calls.
"""


def report_policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    verified = [f for f in packet.facts if f.key == DRIFT and f.status == VERIFIED]
    drifts = sorted(verified, key=lambda f: f.subject)
    findings = []
    for drift in drifts:
        service, host = split_service_subject(drift.subject)
        findings.append(
            {
                "service": service,
                "host": host,
                "expected": drift.value["expected"],
                "actual": drift.value["actual"],
                "fact_id": drift.id,
            }
        )
    return Final({"findings": findings, "summary": _summary(findings)})


def _summary(findings: list[dict[str, Any]]) -> str:
    if not findings:
        return "# Finding\n\nNo verified replica drift was found in this run.\n"
    n = len(findings)
    title = "# Finding: replica drift" if n == 1 else f"# Findings: {n} replica drifts"
    lines = [title, ""]
    for f in findings:
        lines.append(
            f"- **{f['service']}** on **{f['host']}** runs **{f['actual']}** replica(s); "
            f"the registry expects **{f['expected']}** (verified drift fact `{f['fact_id']}`)."
        )
    lines += [
        "",
        "Each drift was confirmed by independent reads that agreed with each other (see each "
        "fact's evidence list), and each expected count comes from `registry.json`.",
    ]
    return "\n".join(lines) + "\n"


def report_fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    if not output["findings"]:
        return None
    findings = [dict(f) for f in output["findings"]]
    findings[0]["expected"] += 1
    return {**output, "findings": findings}


async def run_report(ctx: AgentContext) -> ReportOutput:
    return await run_agent(ctx, ReportOutput, report_policy, report_fabricate)
