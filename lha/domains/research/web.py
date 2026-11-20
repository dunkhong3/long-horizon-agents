"""Reading real web pages, for research on the real web with a real model.

A page is fetched with httpx, and its HTML is turned into plain text with
the standard library's HTMLParser, which skips scripts, styles and other
tags that hold no reading text, and turns block tags such as paragraphs
and list items into line breaks. The page's links come last, as full URLs,
because in HTML they hide in attributes the text doesn't show. Long text is
served in pages of about PAGE_CHARS characters, cut at line breaks, so one
page fits in one model call, and the quote check later compares every quote
with exactly this text.
"""

from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

PAGE_CHARS = 4000  # about 1000 tokens of text per page
MAX_BYTES = 2_000_000  # the most of one response that is read
FETCH_TIMEOUT_SECONDS = 15.0
USER_AGENT = "long-horizon-agents (research; +https://github.com/dunkhong3/long-horizon-agents)"
TEXT_TYPES = ("text/html", "application/xhtml+xml", "text/plain")
MAX_LINKS = 40  # links listed at the end of a page's text

SKIPPED = {"script", "style", "noscript", "svg", "template", "iframe", "nav", "footer", "form"}
BLOCKS = {
    "p", "div", "br", "li", "ul", "ol", "dl", "dt", "dd", "h1", "h2", "h3", "h4", "h5", "h6",
    "tr", "table", "section", "article", "header", "main", "aside", "pre", "blockquote", "hr",
}  # fmt: skip


class _Text(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.parts: list[str] = []
        self.links: list[str] = []
        self.title = ""
        self.skipping = 0
        self.in_title = False

    def handle_starttag(self, tag: str, attrs: list) -> None:
        href = dict(attrs).get("href") if tag == "a" and not self.skipping else None
        if href:
            link = urljoin(self.base_url, href).split("#")[0]
            if link.startswith(("http://", "https://")) and link not in self.links:
                self.links.append(link)
        if tag == "title":
            self.in_title = True
        elif tag in SKIPPED:
            self.skipping += 1
        elif tag in BLOCKS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self.in_title = False
        elif tag in SKIPPED and self.skipping:
            self.skipping -= 1
        elif tag in BLOCKS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.title += data
        elif not self.skipping:
            self.parts.append(data)


def html_to_text(html: str, base_url: str = "") -> tuple[str, str]:
    """The page's title, and its reading text one paragraph per line with its links at the end."""
    parser = _Text(base_url)
    parser.feed(html)
    parser.close()
    lines = [" ".join(line.split()) for line in "".join(parser.parts).split("\n")]
    text = "\n".join(line for line in lines if line)
    if parser.links:
        text += "\nLinks on this page:\n" + "\n".join(parser.links[:MAX_LINKS])
    return " ".join(parser.title.split()), text


def paginate(text: str, size: int = PAGE_CHARS) -> list[str]:
    """Text cut into pages of about `size` characters at line breaks, where a
    line longer than a page is cut at a space. There is always one page."""
    pages: list[str] = []
    current = ""
    for line in text.split("\n"):
        while len(line) > size:
            cut = line.rfind(" ", 0, size)
            cut = size if cut <= 0 else cut
            pages, current = _add(pages, current, line[:cut], size)
            line = line[cut:].lstrip()
        pages, current = _add(pages, current, line, size)
    return [*pages, current] if current or not pages else pages


def _add(pages: list[str], current: str, line: str, size: int) -> tuple[list[str], str]:
    if current and len(current) + 1 + len(line) > size:
        return [*pages, current], line
    return pages, f"{current}\n{line}" if current else line


def site_of(url: str) -> str:
    """The site a URL belongs to, which is what counts as one source, so two
    pages on the same site are never two independent sources."""
    host = urlsplit(url).hostname or ""
    return host.removeprefix("www.")
