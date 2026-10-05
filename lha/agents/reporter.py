"""The reporter agent, which writes the finding from verified facts only.

It runs once, at the end. Its ContextPacket contains only verified drift
facts plus the registry entry and latest read behind each one, and it
reports every verified drift, which is one for the default goal and all of
them for the 'find all drifts' goal.
"""

from typing import Any

from lha.agents.base import AgentContext, Final, run_agent
from lha.schemas.context import ContextPacket
from lha.schemas.facts import DRIFT, VERIFIED, split_service_subject
from lha.schemas.tasks import ReportOutput


def policy(packet: ContextPacket, results: list[tuple[str, Any]]) -> Final:
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
                "drift_fact_id": drift.id,
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
            f"the registry expects **{f['expected']}** (verified drift fact `{f['drift_fact_id']}`)."
        )
    lines += [
        "",
        "Each drift was confirmed by independent reads that agreed with each other (see each "
        "fact's evidence list), and each expected count comes from `registry.json`.",
    ]
    return "\n".join(lines) + "\n"


def fabricate(output: dict[str, Any]) -> dict[str, Any] | None:
    if not output["findings"]:
        return None
    findings = [dict(f) for f in output["findings"]]
    findings[0]["expected"] += 1
    return {**output, "findings": findings}


async def run(ctx: AgentContext) -> ReportOutput:
    return await run_agent(ctx, ReportOutput, policy, fabricate)
