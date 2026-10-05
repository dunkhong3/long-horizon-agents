"""The benchmark, which runs the system and the naive baseline on the same worlds.

    python -m lha.bench                          # 5 seeds, 20 and 60 hosts
    python -m lha.bench --seeds 10 --hosts 20 40 60 --chaos 0.3

Every run is its own set of processes, so runs don't share anything, and a
few run at the same time. The results are printed as a table and saved in
runs/bench/ (see 'The benchmark' in docs/design.md for what the columns mean
and what we measured).
"""

import argparse
import asyncio
import json
import re
import sys
import tempfile
import time
from pathlib import Path
from statistics import mean
from typing import Any

from lha.config import DEFAULT_FAULT_RATE

# name -> extra arguments for lha.baseline, or None for the system itself.
VARIANTS: dict[str, list[str] | None] = {
    "system": None,
    "naive": [],
    "naive + re-read": ["--reread"],
    "naive + re-read, 2000-token window": ["--reread", "--window", "2000"],
}


async def run_one(variant: str, seed: int, hosts: int, chaos: float) -> dict[str, Any]:
    common = ["--seed", str(seed), "--hosts", str(hosts), "--chaos", str(chaos)]
    extra = VARIANTS[variant]
    if extra is None:
        out = await _exec("lha.run", "--quiet", *common)
        match = re.search(r"session ([0-9a-f-]{36})", out)
        if match is None:
            raise RuntimeError(f"system run failed:\n{out}")
        score = _read_json(Path("runs") / match.group(1) / "score.json")
    else:
        with tempfile.NamedTemporaryFile(suffix=".json") as f:
            await _exec("lha.baseline", *common, *extra, "--out", f.name)
            score = _read_json(Path(f.name))
    return {"variant": variant, "seed": seed, "hosts": hosts, "chaos": chaos, **score}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


async def _exec(module: str, *args: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", module, *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    out, _ = await proc.communicate()
    return out.decode()


def outcome(run: dict[str, Any]) -> str:
    """PASS, or why not, which is a wrong answer or no answer at all."""
    if run["passed"]:
        return "pass"
    report = run.get("report") or {}
    return "wrong" if report.get("service") else "none"


def table(runs: list[dict[str, Any]]) -> str:
    lines = [
        "| hosts | variant | pass | wrong | none | steps | prompt mean | prompt max "
        "| prompt total | repeated reads |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for hosts in sorted({r["hosts"] for r in runs}):
        for variant in VARIANTS:
            rs = [r for r in runs if r["hosts"] == hosts and r["variant"] == variant]
            if not rs:
                continue
            outcomes = [outcome(r) for r in rs]

            def avg(key: str, rs=rs) -> str:
                return f"{round(mean(r['stats'][key] for r in rs)):,}"

            lines.append(
                f"| {hosts} | {variant} | {outcomes.count('pass')}/{len(rs)} | {outcomes.count('wrong')} "
                f"| {outcomes.count('none')} | {avg('steps')} | {avg('prompt_tokens_mean')} "
                f"| {max(r['stats']['prompt_tokens_max'] for r in rs):,} | {avg('prompt_tokens_total')} "
                f"| {avg('repeated_reads')} |"
            )
    return "\n".join(lines)


async def main(args: argparse.Namespace) -> int:
    jobs = [
        (variant, seed, hosts, args.chaos)
        for hosts in args.hosts
        for seed in range(1, args.seeds + 1)
        for variant in VARIANTS
    ]
    limit = asyncio.Semaphore(args.parallel)
    started = time.monotonic()

    async def guarded(job):
        async with limit:
            run = await run_one(*job)
            print(f"[bench] {job[2]} hosts, seed {job[1]}, {job[0]}: {outcome(run)}", flush=True)
            return run

    runs = await asyncio.gather(*(guarded(job) for job in jobs))
    text = table(runs)
    print()
    print(text)
    out_dir = Path("runs") / "bench"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    (out_dir / f"{stamp}.json").write_text(json.dumps(runs, indent=2))
    (out_dir / f"{stamp}.md").write_text(text + "\n")
    print(f"\n{len(runs)} runs in {time.monotonic() - started:.0f}s, saved in {out_dir}/{stamp}.*")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seeds", type=int, default=5, help="seeds 1..N for every world size")
    p.add_argument("--hosts", type=int, nargs="+", default=[20, 60], help="world sizes")
    p.add_argument("--chaos", type=float, default=DEFAULT_FAULT_RATE)
    p.add_argument("--parallel", type=int, default=4, help="runs at the same time")
    return p.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(main(parse_args())))
