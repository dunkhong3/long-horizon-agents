"""The scale benchmark, which runs one big world with more and more workers and coordinators.

    python -m lha.scale                       # 200 hosts, 'find all drifts', seed 7
    python -m lha.scale --hosts 100 --seed 3

Each run uses the same world, the same faults and the same goal, and only
the number of processes and the way they wait for work change, so the
differences come from the coordination alone (see 'Limits and scaling' in
docs/design.md for what we measured).
"""

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path
from statistics import quantiles
from uuid import UUID

from sqlalchemy import select

from lha.db import make_engine
from lha.db.models import events

# (discovery workers, analysis workers, coordinators, wake-ups)
CONFIGS = [
    (2, 2, 1, "notify"),
    (8, 8, 1, "notify"),
    (8, 8, 1, "poll"),
    (8, 8, 4, "notify"),
    (16, 16, 1, "notify"),
    (16, 16, 4, "notify"),
]


async def run_one(
    args: argparse.Namespace, discovery: int, analysis: int, coordinators: int, wakeups: str
) -> dict:
    cmd = [
        "--quiet", "--seed", str(args.seed), "--hosts", str(args.hosts), "--goal", "all",
        "--step-budget", "100000", "--discovery", str(discovery), "--analysis", str(analysis),
        "--coordinators", str(coordinators), *(["--poll"] if wakeups == "poll" else []),
    ]  # fmt: skip
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "lha.run",
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out = (await proc.communicate())[0].decode()
    sid = re.search(r"session ([0-9a-f-]{36})", out).group(1)
    elapsed = float(re.search(r"elapsed=([0-9.]+)s", out).group(1))
    steps = int(re.search(r"steps=(\d+)", out).group(1))
    return {
        "discovery": discovery,
        "analysis": analysis,
        "coordinators": coordinators,
        "wakeups": wakeups,
        "passed": "verdict=PASS" in out,
        "steps": steps,
        "elapsed": elapsed,
        **await _measure(UUID(sid), elapsed),
    }


async def _measure(sid: UUID, elapsed: float) -> dict:
    """Decision latency and idle claims, worked out from the session's events."""
    engine = make_engine()
    try:
        async with engine.connect() as conn:
            q = select(events.c.payload["latency_ms"].as_integer()).where(
                events.c.session_id == sid, events.c.kind == "decision"
            )
            latencies = sorted(x for x in (await conn.execute(q)).scalars() if x is not None)
            q = select(events.c.payload).where(events.c.session_id == sid, events.c.kind == "worker_stats")
            stats = list((await conn.execute(q)).scalars())
    finally:
        await engine.dispose()
    cuts = quantiles(latencies, n=20) if len(latencies) >= 2 else [0] * 19
    workers = max(len(stats), 1)
    return {
        "latency_p50_ms": round(cuts[9]),
        "latency_p95_ms": round(cuts[18]),
        "empty_claims_per_worker_s": round(sum(s["empty_claims"] for s in stats) / workers / elapsed, 1),
    }


def table(runs: list[dict]) -> str:
    lines = [
        "| workers | coordinators | wake-ups | pass | steps | elapsed | steps/s | decision p50 "
        "| decision p95 | empty claims per worker-second |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in runs:
        lines.append(
            f"| {r['discovery']}+{r['analysis']} | {r['coordinators']} | {r['wakeups']} "
            f"| {'yes' if r['passed'] else 'no'} | {r['steps']:,} | {r['elapsed']:.1f} s "
            f"| {r['steps'] / r['elapsed']:.0f} | {r['latency_p50_ms']} ms | {r['latency_p95_ms']} ms "
            f"| {r['empty_claims_per_worker_s']} |"
        )
    return "\n".join(lines)


async def main(args: argparse.Namespace) -> int:
    runs = []
    for config in CONFIGS:
        run = await run_one(args, *config)
        print(f"[scale] {config}: {'PASS' if run['passed'] else 'FAIL'} in {run['elapsed']:.1f}s", flush=True)
        runs.append(run)
    text = table(runs)
    print()
    print(text)
    out_dir = Path("runs") / "scale"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    (out_dir / f"{stamp}.json").write_text(json.dumps(runs, indent=2))
    (out_dir / f"{stamp}.md").write_text(text + "\n")
    print(f"\nsaved in {out_dir}/{stamp}.*")
    return 0 if all(r["passed"] for r in runs) else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--hosts", type=int, default=200)
    p.add_argument("--seed", type=int, default=7)
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
