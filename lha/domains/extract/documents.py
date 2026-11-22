"""The documents of an extraction, which are the text files in one folder.

A document is a file under the folder with one of TEXT_SUFFIXES, named by
its path inside the folder. Its text is read as it is, except for HTML,
which is turned into plain text the way research reads a web page
(lha/domains/research/web.py), and long text comes in pages of about
PAGE_CHARS characters, cut at line breaks, so one page fits in one model
call and the quote check compares every quote with exactly that text.
"""

from pathlib import Path

from lha.domains.research.web import html_to_text, paginate

TEXT_SUFFIXES = (".txt", ".md", ".text", ".html", ".htm")
MAX_DOCS = 5000
PAGE_CHARS = {"fake": 1500, "real": 6000}  # by the size of the model's window


class DocumentError(Exception):
    """A document that can't be read as text, which another try won't change."""


def list_documents(folder: Path) -> list[str]:
    """Every document under the folder, by its path inside it, in order."""
    docs = []
    for path in sorted(folder.rglob("*")):
        rel = path.relative_to(folder)
        if (
            path.is_file()
            and path.suffix.lower() in TEXT_SUFFIXES
            and not any(p.startswith(".") for p in rel.parts)
        ):
            docs.append(rel.as_posix())
    return docs[:MAX_DOCS]


def read_text(folder: Path, doc: str) -> str:
    path = (folder / doc).resolve()
    if folder.resolve() not in path.parents or not path.is_file():
        raise DocumentError(f"{doc} is not a document in the folder")
    text = path.read_bytes().decode("utf-8", errors="replace")
    if path.suffix.lower() in (".html", ".htm"):
        text = html_to_text(text)[1].split("\nLinks on this page:\n")[0]
    if not text.strip():
        raise DocumentError(f"{doc} has no text")
    return text


def pages_of(folder: Path, doc: str, size: int) -> list[str]:
    return paginate(read_text(folder, doc), size)
