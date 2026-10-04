"""Full runs: real processes, real Postgres, faults on. Each must PASS."""

import re
import subprocess
import sys


def run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "lha.run", "--quiet", *args],
        capture_output=True, text=True, timeout=300,
    )  # fmt: skip


def test_full_run_passes():
    result = run("--seed", "101")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "verdict=PASS" in result.stdout


def test_full_run_passes_under_heavy_faults():
    result = run("--seed", "102", "--chaos", "0.35")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "verdict=PASS" in result.stdout


def test_crash_then_resume_passes():
    crashed = run("--seed", "103", "--kill-at", "100")
    assert crashed.returncode == 137, crashed.stdout + crashed.stderr
    session = re.search(r"--resume ([0-9a-f-]+)", crashed.stdout).group(1)

    resumed = run("--resume", session)
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    assert "verdict=PASS" in resumed.stdout
