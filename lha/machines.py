"""Which machine a process runs on, for workers on more than one machine.

A machine is named by LHA_MACHINE, or by its hostname when that isn't set,
and the name has to differ between the machines that work on one run. Each
supervisor names its workers after a crew of its own, which is the
machine's name with four random hex digits, so two supervisors, even on one
machine, never hand out the same worker name, and a worker's lease is only
ever its own (see Supervisor in lha/run.py).
"""

import os
import secrets
import socket
from urllib.parse import urlsplit

LISTEN_ALL = ("0.0.0.0", "::")


def machine_name() -> str:
    return os.environ.get("LHA_MACHINE") or socket.gethostname()


def new_crew(machine: str) -> str:
    return f"{machine}-{secrets.token_hex(2)}"


def machine_of(worker: str) -> str | None:
    """The machine a worker name belongs to, or None for a name from before
    workers were named by crew, such as 'discovery-1'."""
    crew, slash, _ = worker.partition("/")
    return crew.rsplit("-", 1)[0] if slash else None


def advertised_host(listen: str) -> str:
    """The host other machines reach a server on that listens on `listen`,
    which is LHA_ADVERTISE_HOST when it is set, and otherwise the address it
    listens on, or this machine's hostname when it listens on every address."""
    if advertise := os.environ.get("LHA_ADVERTISE_HOST"):
        return advertise
    return socket.gethostname() if listen in LISTEN_ALL else listen


def local_address(listen: str) -> str:
    """Where this machine itself reaches a server that listens on `listen`."""
    return "127.0.0.1" if listen in LISTEN_ALL else listen


def is_loopback(url: str) -> bool:
    host = urlsplit(url).hostname or ""
    return host == "localhost" or host.startswith("127.") or host == "::1"
