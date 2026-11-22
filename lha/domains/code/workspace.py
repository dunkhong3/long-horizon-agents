"""A session's git workspace, and the checks that run in it.

Every session works in a git worktree of its own, which is a second working
folder of the same repository checked out on a new branch, `lha/<session>`,
under runs/<session>/work, so the repository's own folder and branches are
never touched and the result can be reviewed and merged like any branch.
Only the coordinator commits to it, one accepted fix at a time.

A check never runs in the worktree. Each run gets a scratch copy of one
commit, made with `git archive`, with the run's edits applied, so runs in
parallel can't disturb each other or the branch. The command is always the
session's check template, split like a shell would split it, with the item
put in place of `{item}`, and it runs without a shell, so a model never
chooses what runs, only which item, and an item is checked against a strict
pattern first.
"""

import fnmatch
import hashlib
import io
import json
import os
import re
import shlex
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path

CHECK_TIMEOUT_SECONDS = 300.0
OUTPUT_CHARS = 3000  # the end of a check's output, which is where failures are summed up
ITEM = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./:\[\]=,+-]*")
GIT_ENV = {
    "GIT_AUTHOR_NAME": "lha",
    "GIT_AUTHOR_EMAIL": "lha@localhost",
    "GIT_COMMITTER_NAME": "lha",
    "GIT_COMMITTER_EMAIL": "lha@localhost",
}


class WorkspaceError(Exception):
    """Something about the workspace or an edit is wrong, which a retry won't fix."""


@dataclass
class CheckRun:
    passed: bool
    exit_code: int
    output: str  # the end of what it printed


def git(cwd: Path, *args: str, stdin: bytes | None = None) -> bytes:
    done = subprocess.run(
        ["git", "-c", "commit.gpgsign=false", *args],
        cwd=cwd, input=stdin, capture_output=True, env={**os.environ, **GIT_ENV},
    )  # fmt: skip
    if done.returncode != 0:
        raise WorkspaceError(f"git {' '.join(args)}: {done.stderr.decode(errors='replace').strip()}")
    return done.stdout


def head(work: Path) -> str:
    return git(work, "rev-parse", "HEAD").decode().strip()


def init_repo(path: Path, files: dict[str, str]) -> None:
    """A new repository with these files as its first commit, for the mock repository."""
    for name, text in files.items():
        (path / name).parent.mkdir(parents=True, exist_ok=True)
        (path / name).write_text(text)
    git(path, "init", "-q", "-b", "main")
    git(path, "add", "-A")
    git(path, "commit", "-q", "-m", "the code as it was given")


def open_worktree(repo: Path, work: Path, branch: str) -> str:
    """The session's worktree on a new branch from the repository's HEAD, and the commit it starts at."""
    git(repo, "rev-parse", "--is-inside-work-tree")
    git(repo, "worktree", "add", "-q", "-b", branch, str(work), "HEAD")
    return head(work)


def check_argv(template: str, item: str | None) -> list[str]:
    """The command for one item, or for the whole check when item is None."""
    words = shlex.split(template)
    if "{item}" not in words:
        raise WorkspaceError("the check needs {item} as one of its words, where one failing item goes")
    if item is None:
        return [w for w in words if w != "{item}"]
    if not valid_item(item):
        raise WorkspaceError(f"{item!r} is not an item a check can take")
    return [item if w == "{item}" else w for w in words]


def valid_item(item: str) -> bool:
    return ITEM.fullmatch(item) is not None and ".." not in item and len(item) <= 300


def snapshot(work: Path, commit: str, dest: Path) -> None:
    """The files of one commit, without git, in an empty folder."""
    data = git(work, "archive", "--format=tar", commit)
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        tar.extractall(dest, filter="data")


def apply_edits(root: Path, edits: list[dict[str, str]], protected: tuple[str, ...] = ()) -> list[str]:
    """Each edit replaces `old`, which must be in its file exactly once, with
    `new`, or with an empty `old` creates a file that doesn't exist yet. It
    returns the paths it changed, and raises WorkspaceError if any edit can't
    be made, before anything is written."""
    changed: dict[str, str] = {}
    for e in edits:
        path = safe_path(e["path"], protected)
        target = root / path
        text = changed.get(path)
        if text is None:
            text = target.read_text() if target.is_file() else None
        if e["old"] == "":
            if text is not None:
                raise WorkspaceError(f"{path} already exists, so an edit needs the text it replaces")
            changed[path] = e["new"]
            continue
        if text is None:
            raise WorkspaceError(f"{path} doesn't exist")
        count = text.count(e["old"])
        if count != 1:
            raise WorkspaceError(f"the text to replace is in {path} {count} times, and must be there once")
        changed[path] = text.replace(e["old"], e["new"])
    for path, text in changed.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text(text)
    return sorted(changed)


def safe_path(path: str, protected: tuple[str, ...] = ()) -> str:
    """A repository-relative path an edit may change, or WorkspaceError."""
    p = Path(path)
    if p.is_absolute() or ".." in p.parts or not p.parts or p.parts[0] == ".git":
        raise WorkspaceError(f"{path!r} is not a path inside the repository")
    rel = p.as_posix()
    if any(fnmatch.fnmatch(rel, pattern) for pattern in protected):
        raise WorkspaceError(f"{rel} is protected, so a fix may not change it")
    return rel


def edits_digest(edits: list[dict[str, str]]) -> str:
    """A fingerprint of a list of edits, so a fix can be matched with the run that checked it."""
    canonical = json.dumps([[e["path"], e["old"], e["new"]] for e in edits], separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def run_check(
    work: Path, commit: str, template: str, item: str | None, edits: list[dict[str, str]],
    protected: tuple[str, ...] = (), timeout: float = CHECK_TIMEOUT_SECONDS,
) -> CheckRun:  # fmt: skip
    """The check for one item (or all of it) on a scratch copy of a commit with these edits."""
    argv = check_argv(template, item)
    scratch = Path(tempfile.mkdtemp(prefix="check-", dir=work.parent))
    try:
        snapshot(work, commit, scratch)
        apply_edits(scratch, edits, protected)
        try:
            done = subprocess.run(
                argv, cwd=scratch, capture_output=True, timeout=timeout, env=_check_env(scratch)
            )
        except subprocess.TimeoutExpired:
            return CheckRun(False, -1, f"the check took longer than {timeout:.0f} seconds and was stopped")
        except FileNotFoundError as e:
            raise WorkspaceError(f"the check can't start: {e}") from None
        output = (done.stdout + done.stderr).decode(errors="replace")
        return CheckRun(done.returncode == 0, done.returncode, _tail(output.replace(str(scratch) + "/", "")))
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def commit_edits(
    work: Path, edits: list[dict[str, str]], message: str, protected: tuple[str, ...] = ()
) -> str:
    """Apply edits to the worktree and commit them, returning the new commit.
    The worktree is first reset to its last commit, which drops whatever a
    coordinator that died halfway through applying a fix left behind."""
    git(work, "reset", "-q", "--hard", "HEAD")
    git(work, "clean", "-q", "-f", "-d")
    paths = apply_edits(work, edits, protected)
    git(work, "add", "--", *paths)
    git(work, "commit", "-q", "--no-verify", "-m", message)
    return head(work)


def changed_since(work: Path, base: str, paths: list[str]) -> list[str]:
    """Which of these paths changed on the branch after `base`."""
    if not paths:
        return []
    out = git(work, "diff", "--name-only", base, "HEAD", "--", *paths)
    return out.decode().split()


def list_files(work: Path) -> list[str]:
    return git(work, "ls-files").decode().splitlines()


def _check_env(scratch: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.pop("VIRTUAL_ENV", None)  # a check finds its own environment, as it would in a fresh checkout
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["COLUMNS"] = "200"  # wide, so a failure's message is never cut short (_tail drops the padding)
    return env


def _tail(text: str) -> str:
    """The end of an output, without the padding and the rules that fill a
    terminal, which only cost tokens."""
    text = re.sub(r"([=\-_*~#])\1{3,}", r"\1\1\1", re.sub(r"[ \t]{4,}", " ", text))
    return text if len(text) <= OUTPUT_CHARS else "..." + text[-OUTPUT_CHARS:]
