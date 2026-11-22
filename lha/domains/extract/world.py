"""The mock documents the extraction agents read, generated from a seed.

They are invoices, as plain text, Markdown or HTML files, each stating its
fields in sentences such as 'Invoice number: INV-2024-0042.' or 'The amount
to pay is 1234.50 EUR.', among lines that state nothing asked about. The
goal 'one' asks for the invoice number, the date and the total of every
invoice, and 'all' also asks for the supplier.

Some invoices don't state their date, so the right answer for that field
is that it isn't stated, and some mention a second invoice number, the one
they replace, or a draft's total, in a sentence of their own, so the reader
has to tell the field from a value that only looks like it. A few invoices
list so many items that they come in several pages, with the total on the
last one.
"""

import random
from dataclasses import dataclass, field

DEFAULT_DOCS = 40
LONG_DOCS = 3
LONG_ITEMS = 200  # item lines on a long invoice, more than one attempt can read
MISSING_DATE_SHARE = 0.15
FIELDS = {
    "invoice number": "an id such as INV-2024-0042",
    "supplier": "the company that sent the invoice",
    "date": "the day it was issued, such as 14 March 2024",
    "total": "the amount to pay, such as 1234.50 EUR",
}
ASKED = {"one": ("invoice number", "date", "total"), "all": ("invoice number", "supplier", "date", "total")}
# How each field is stated, where {v} is the value.
TEMPLATES = {
    "invoice number": ("Invoice number: {v}.", "This is invoice {v}."),
    "supplier": ("Issued by {v}.", "From: {v}."),
    "date": ("Date of issue: {v}.", "This invoice was issued on {v}."),
    "total": ("Total due: {v}.", "The amount to pay is {v}."),
}
SUPPLIERS = (
    "Acme Supplies", "Borealis Trading", "Cobalt Parts", "Dynamo Tools", "Ember Foods",
    "Fathom Logistics", "Granite Works", "Harbor Freight", "Iris Printing", "Juniper Office",
)  # fmt: skip
MONTHS = ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
          "November", "December")  # fmt: skip
FILLER = (
    "Thank you for your business.",
    "Please pay by bank transfer to the account below.",
    "Goods remain our property until paid in full.",
    "For questions about this invoice, write to our billing team.",
    "Prices include delivery.",
)


@dataclass
class MockDocs:
    seed: int
    size: int
    fields: tuple[str, ...]
    files: dict[str, str] = field(default_factory=dict)  # name -> text
    truth: dict[tuple[str, str], str | None] = field(
        default_factory=dict
    )  # (doc, field) -> value or not stated
    long_docs: list[str] = field(default_factory=list)


def generate_docs(seed: int, size: int = DEFAULT_DOCS, goal: str = "one") -> MockDocs:
    rng = random.Random(f"docs|{seed}|{size}")
    docs = MockDocs(seed=seed, size=size, fields=ASKED[goal])
    long_ones = set(rng.sample(range(size), min(LONG_DOCS, size)))
    for i in range(size):
        number = f"INV-{rng.choice((2023, 2024))}-{rng.randint(1, 9999):04d}"
        values = {
            "invoice number": number,
            "supplier": rng.choice(SUPPLIERS),
            "date": f"{rng.randint(1, 28)} {rng.choice(MONTHS)} {rng.choice((2023, 2024))}",
            "total": f"{rng.randint(20, 9999)}.{rng.randint(0, 99):02d} {rng.choice(('EUR', 'USD', 'GBP'))}",
        }
        if rng.random() < MISSING_DATE_SHARE:
            values["date"] = None
        lines = [
            rng.choice(TEMPLATES[f]).format(v=v) for f, v in values.items() if v is not None and f != "total"
        ]
        lines += rng.sample(FILLER, 2)
        if rng.random() < 0.3:
            lines.append(f"This invoice replaces invoice INV-2022-{rng.randint(1, 9999):04d}.")
        if rng.random() < 0.2:
            lines.append(f"A draft of this invoice came to {rng.randint(20, 9999)}.00 EUR.")
        rng.shuffle(lines)
        if i in long_ones:
            for n in range(LONG_ITEMS):
                part, count, price = rng.randint(100, 999), rng.randint(1, 9), rng.randint(1, 99)
                lines.append(f"Item {n + 1}: part {part}, {count} x {price}.00.")
        lines.append(rng.choice(TEMPLATES["total"]).format(v=values["total"]))
        name, text = _file(i, values["supplier"], lines)
        docs.files[name] = text
        if i in long_ones:
            docs.long_docs.append(name)
        for f in docs.fields:
            docs.truth[(name, f)] = values[f]
    return docs


def _file(i: int, supplier: str, lines: list[str]) -> tuple[str, str]:
    """Every fifth invoice is HTML and every seventh is Markdown, and the rest are plain text."""
    if i % 5 == 4:
        body = "".join(f"<p>{line}</p>" for line in lines)
        return (
            f"invoice-{i + 1:03d}.html",
            f"<html><head><title>Invoice</title></head><body>{body}</body></html>",
        )
    if i % 7 == 6:
        return f"invoice-{i + 1:03d}.md", f"# Invoice from {supplier}\n\n" + "\n\n".join(lines) + "\n"
    return f"invoice-{i + 1:03d}.txt", "INVOICE\n\n" + "\n".join(lines) + "\n"
