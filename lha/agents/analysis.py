"""The analysis agent, which compares services with the registry and re-reads drifts.

This is the 'depth' role, and it handles two task types. The first is
compare_service, which makes no network call at all and compares the
discovered replica count with the registry entry, both taken from the facts
in the ContextPacket. The second is verify_drift, which is a fresh read of a
service we suspect has drifted, and it only reports the number it saw,
because deciding what that number means is the coordinator's job.
"""

from typing import Any

from lha.agents.base import AgentContext, Call, Final, run_agent
from lha.schemas.context import ContextPacket
from lha.schemas.facts import EXPECTED, REPLICAS, registry_subject, service_subject
from lha.schemas.tasks import CompareOutput, VerifyOutput


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
