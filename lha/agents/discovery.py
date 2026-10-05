"""The discovery agent, which reads one host, its services and its documents (page by page).

This is the 'breadth' role. It reports what exists and which other hosts
the documents mention, and it decides nothing about drift.
"""

import re
from typing import Any

from lha.agents.base import AgentContext, Call, Final, run_agent
from lha.schemas.context import ContextPacket
from lha.schemas.tasks import DiscoverOutput
from lha.world.model import REGISTRY_DOC, registry_entries

HOST_NAME = re.compile(r"\bhost-\d+\b")


def policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Call | Final:
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
        registry = registry_entries(d["content"]) if name == REGISTRY_DOC else None
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


def fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
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


async def run(ctx: AgentContext) -> DiscoverOutput:
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
    return await run_agent(ctx, DiscoverOutput, policy, fabricate, tools)
