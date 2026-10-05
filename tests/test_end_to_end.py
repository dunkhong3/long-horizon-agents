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


def test_find_all_drifts_passes():
    result = run("--seed", "104", "--goal", "all")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "found all 3 planted drifts" in result.stdout


def test_injected_crashes_pass():
    """Workers die or hang mid-task and the coordinator crashes mid-commit."""
    result = run("--seed", "105", "--crashes", "0.05")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "verdict=PASS" in result.stdout
    assert "crashes=0 " not in result.stdout


def test_partitioned_coordinators_with_more_workers_pass():
    result = run("--seed", "106", "--goal", "all", "--coordinators", "3", "--workers", "6,6")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "found all 3 planted drifts" in result.stdout


def test_polling_instead_of_notify_passes():
    result = run("--seed", "107", "--poll")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "verdict=PASS" in result.stdout


def test_research_brief_passes():
    """The second domain, on the same core."""
    result = run("--domain", "research", "--seed", "108", "--goal", "all", "--chaos", "0.3")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "answered all 6 project(s) asked about" in result.stdout


def test_crash_then_resume_passes():
    crashed = run("--seed", "103", "--kill-at", "100")
    assert crashed.returncode == 137, crashed.stdout + crashed.stderr
    session = re.search(r"--resume ([0-9a-f-]+)", crashed.stdout).group(1)

    resumed = run("--resume", session)
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    assert "verdict=PASS" in resumed.stdout
