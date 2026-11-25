"""The domains built into the code, by name, while a domain loaded from
configuration is found by lha/domains/config and built from its file."""

from lha.domains.audit import AUDIT
from lha.domains.code import CODE
from lha.domains.extract import EXTRACT
from lha.domains.research import RESEARCH

DOMAINS = {"audit": AUDIT, "research": RESEARCH, "code": CODE, "extract": EXTRACT}
