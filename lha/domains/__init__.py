"""The domains the core can run, by name."""

from lha.domains.audit import AUDIT
from lha.domains.code import CODE
from lha.domains.research import RESEARCH

DOMAINS = {"audit": AUDIT, "research": RESEARCH, "code": CODE}
