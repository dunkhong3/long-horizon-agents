"""Finding a quote, or a value, in the text it is said to come from.

Domains that read documents check every quote a model reports against the
raw text a tool fetched, and every value against its quote, and both checks
ignore case, spacing and the difference between curly and straight quotes
and dashes, which a model often changes without meaning anything by it.
"""

import re
import unicodedata


def within(needle: str, haystack: str, whole_words: bool = False) -> bool:
    """Whether a quote or value appears in a text, where a value must be
    whole words, so '19' is not in 'launched in 1997', and a token joined by
    hyphens, dots or slashes counts as one word, so 'INV-2024' is not in
    'INV-2024-0042' and '1.5' is not in '1.5.2', while the full stop that
    ends a sentence still ends the word before it."""
    needle, haystack = flat(needle), flat(haystack)
    if not needle:
        return False
    if whole_words:
        pattern = rf"(?<!\w)(?<!\w[-./]){re.escape(needle)}(?!\w)(?![-./]\w)"
        return re.search(pattern, haystack) is not None
    return needle in haystack


def flat(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    for a, b in (("‘", "'"), ("’", "'"), ("“", '"'), ("”", '"'), ("–", "-"), ("—", "-")):
        s = s.replace(a, b)
    return " ".join(s.casefold().split())
