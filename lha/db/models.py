"""The four tables, where Postgres is the only state shared between processes.

These are internal storage definitions (SQLAlchemy Core). The contracts
between processes live in lha/schemas.
"""

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

# clock_timestamp() rather than now(), because now() is fixed for a whole
# transaction and the coordinator creates several rows per transaction.
CREATED_AT = {"server_default": text("clock_timestamp()"), "nullable": False}

sessions = Table(
    "sessions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("seed", Integer, nullable=False),
    Column("n_hosts", Integer, nullable=False),
    Column("fault_rate", Float, nullable=False),
    Column("step_budget", Integer, nullable=False),
    Column("goal_kind", Text, nullable=False),  # 'one' drift or 'all' drifts
    Column("n_drifts", Integer, nullable=False),  # how many the world plants
    Column("crash_rate", Float, nullable=False),  # injected crashes per attempt
    Column("partitions", Integer, nullable=False),  # coordinators splitting the plan
    Column("wakeups", Text, nullable=False),  # 'notify' (LISTEN/NOTIFY) or 'poll'
    Column("goal", Text, nullable=False),
    Column("start_hosts", JSONB, nullable=False),
    Column("status", Text, nullable=False),  # running | succeeded | failed
    Column("report", JSONB),  # the accepted reporter output
    Column("created_at", DateTime(timezone=True), **CREATED_AT),
    Column("finished_at", DateTime(timezone=True)),
)

# Append-only log of everything that happened. Workers, the coordinator and
# the supervisor write here, and nothing is ever updated or deleted.
events = Table(
    "events",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("session_id", UUID(as_uuid=True), ForeignKey("sessions.id"), nullable=False),
    Column("task_id", UUID(as_uuid=True)),
    Column("attempt", Integer),
    Column("actor", Text, nullable=False),  # 'coordinator' or a worker name
    Column("kind", Text, nullable=False),  # model_call, tool_call, decision, ...
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), **CREATED_AT),
    Index("events_by_kind", "session_id", "kind"),
    Index("events_by_task", "session_id", "task_id"),
)

tasks = Table(
    "tasks",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("session_id", UUID(as_uuid=True), ForeignKey("sessions.id"), nullable=False),
    Column("task_key", Text, nullable=False),  # e.g. discover_host:host-4
    Column("parent_task_id", UUID(as_uuid=True)),  # whose result created this task
    Column("type", Text, nullable=False),
    Column("role", Text, nullable=False),
    Column("partition", Integer, nullable=False),  # which coordinator decides its results
    Column("input", JSONB, nullable=False),
    Column("scope", JSONB, nullable=False),  # which facts the context builder selects
    Column("status", Text, nullable=False),  # ready|leased|submitted|succeeded|failed|split|cancelled
    Column("attempt", Integer, nullable=False, server_default="1"),
    Column("max_attempts", Integer, nullable=False),
    Column("leased_by", Text),
    Column("lease_expires_at", DateTime(timezone=True)),
    Column("not_before", DateTime(timezone=True)),  # backoff
    Column("result", JSONB),  # submitted output, before the coordinator accepts it
    Column("submitted_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), **CREATED_AT),
    # The same task is never created twice (two documents can mention the
    # same host; a resumed coordinator can repeat a decision).
    UniqueConstraint("session_id", "task_key", name="tasks_one_per_key"),
    Index("tasks_claim", "session_id", "status", "role", "created_at"),
    Index("tasks_by_partition", "session_id", "partition", "status"),
)

facts = Table(
    "facts",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("session_id", UUID(as_uuid=True), ForeignKey("sessions.id"), nullable=False),
    Column("subject", Text, nullable=False),  # e.g. service:cache@host-4
    Column("key", Text, nullable=False),  # e.g. config.replicas
    Column("value", JSONB, nullable=False),
    Column("status", Text, nullable=False),  # observed|inferred|verified|refuted|superseded
    Column("source_task_id", UUID(as_uuid=True)),
    Column("source_event_id", UUID(as_uuid=True)),
    Column("evidence", JSONB),  # for drift claims, the reads backing it
    Column("created_at", DateTime(timezone=True), **CREATED_AT),
    # At most one *current* fact per (session, subject, key). Superseded rows
    # are kept as history and don't count.
    Index(
        "facts_one_current",
        "session_id",
        "subject",
        "key",
        unique=True,
        postgresql_where=text("status <> 'superseded'"),
    ),
)
