"""create the four tables and the notify trigger

Revision ID: 0001
Revises: none

The schema as it was before there were migrations. Postgres tells the
processes waiting on a channel when a task becomes ready for a role, or
when a result is submitted for a coordinator's partition, so nobody has
to poll (see lha/db/notify.py).
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

NOTIFY_FUNCTION = """
CREATE FUNCTION lha_notify_tasks() RETURNS trigger AS $$
BEGIN
  IF NEW.status = 'ready' AND (TG_OP = 'INSERT' OR OLD.status <> 'ready'
                               OR OLD.not_before IS DISTINCT FROM NEW.not_before) THEN
    PERFORM pg_notify('lha_ready', NEW.session_id::text || ':' || NEW.role);
  ELSIF NEW.status = 'submitted' AND (TG_OP = 'INSERT' OR OLD.status <> 'submitted') THEN
    PERFORM pg_notify('lha_submitted', NEW.session_id::text || ':' || NEW.partition);
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql
"""
NOTIFY_TRIGGER = """
CREATE TRIGGER tasks_notify AFTER INSERT OR UPDATE OF status, not_before ON tasks
FOR EACH ROW EXECUTE FUNCTION lha_notify_tasks()
"""


def upgrade() -> None:
    op.create_table(
        "sessions",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("domain", sa.Text(), nullable=False),
        sa.Column("seed", sa.Integer(), nullable=False),
        sa.Column("n_hosts", sa.Integer(), nullable=False),
        sa.Column("fault_rate", sa.Float(), nullable=False),
        sa.Column("step_budget", sa.Integer(), nullable=False),
        sa.Column("goal_kind", sa.Text(), nullable=False),
        sa.Column("crash_rate", sa.Float(), nullable=False),
        sa.Column("partitions", sa.Integer(), nullable=False),
        sa.Column("wakeups", sa.Text(), nullable=False),
        sa.Column("goal", sa.Text(), nullable=False),
        sa.Column("start_points", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("report", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "events",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=True),
        sa.Column("attempt", sa.Integer(), nullable=True),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["sessions.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("events_by_kind", "events", ["session_id", "kind"], unique=False)
    op.create_index("events_by_task", "events", ["session_id", "task_id"], unique=False)
    op.create_table(
        "facts",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("value", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("source_task_id", sa.UUID(), nullable=True),
        sa.Column("source_event_id", sa.UUID(), nullable=True),
        sa.Column("evidence", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["sessions.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "facts_one_current",
        "facts",
        ["session_id", "subject", "key"],
        unique=True,
        postgresql_where=sa.text("status <> 'superseded'"),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("task_key", sa.Text(), nullable=False),
        sa.Column("parent_task_id", sa.UUID(), nullable=True),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("resource", sa.Text(), nullable=True),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("input", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("scope", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("attempt", sa.Integer(), server_default="1", nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("leased_by", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("not_before", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["sessions.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", "task_key", name="tasks_one_per_key"),
    )
    op.create_index("tasks_by_partition", "tasks", ["session_id", "partition", "status"], unique=False)
    op.create_index("tasks_claim", "tasks", ["session_id", "status", "role", "created_at"], unique=False)
    op.execute(NOTIFY_FUNCTION)
    op.execute(NOTIFY_TRIGGER)


def downgrade() -> None:
    op.execute("DROP TRIGGER tasks_notify ON tasks")
    op.execute("DROP FUNCTION lha_notify_tasks()")
    op.drop_index("tasks_claim", table_name="tasks")
    op.drop_index("tasks_by_partition", table_name="tasks")
    op.drop_table("tasks")
    op.drop_index("facts_one_current", table_name="facts", postgresql_where=sa.text("status <> 'superseded'"))
    op.drop_table("facts")
    op.drop_index("events_by_task", table_name="events")
    op.drop_index("events_by_kind", table_name="events")
    op.drop_table("events")
    op.drop_table("sessions")
