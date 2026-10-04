"""The reporter agent, which writes the finding from verified facts only.

It runs once, at the end. Its ContextPacket contains only verified drift
facts plus the registry entry and latest read behind each one.
"""

from typing import Any

from lha.agents.base import AgentContext, Final, run_agent
from lha.schemas.context import ContextPacket
from lha.schemas.facts import DRIFT, VERIFIED
from lha.schemas.tasks import ReportOutput


def policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
    drifts = [f for f in packet.facts if f.key == DRIFT and f.status == VERIFIED]
    if not drifts:
        return Final(
            {
                "service": None,
                "host": None,
                "expected": None,
                "actual": None,
                "drift_fact_id": None,
                "summary": "# Finding\n\nNo verified replica drift was found in this run.\n",
            }
        )
    drift = drifts[0]
    service, host = drift.subject.removeprefix("service:").split("@")
    expected, actual = drift.value["expected"], drift.value["actual"]
    summary = "\n".join(
        [
            "# Finding: replica drift",
            "",
            f"**{service}** on **{host}** runs **{actual}** replica(s); the registry expects **{expected}**.",
            "",
            "## Evidence",
            "",
            f"- Drift claim: fact `{drift.id}` (status: verified).",
            "- Confirmed by independent reads that agreed with each other (see the fact's evidence list).",
            "- Expected count from `registry.json`.",
        ]
    )
    return Final(
        {
            "service": service,
            "host": host,
            "expected": expected,
            "actual": actual,
            "drift_fact_id": drift.id,
            "summary": summary + "\n",
        }
    )


def fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    if output["expected"] is None:
        return None
    return {**output, "expected": output["expected"] + 1}


async def run(ctx: AgentContext) -> ReportOutput:
    return await run_agent(ctx, ReportOutput, policy, fabricate)
