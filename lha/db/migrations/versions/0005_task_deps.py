"""add the dependencies of tasks planned ahead of their inputs

Revision ID: 0005
Revises: 0004

A task may now be created before the work it builds on is done, in a new
status 'waiting', with one row here for each task it waits on. A required
edge needs the task it points at to succeed, while an edge that isn't
required only needs it to be finished in any way. Every task that already
exists waits on nothing.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_deps",
        sa.Column("session_id", UUID(as_uuid=True), sa.ForeignKey("sessions.id"), nullable=False),
        sa.Column("task_id", UUID(as_uuid=True), nullable=False),
        sa.Column("depends_on", UUID(as_uuid=True), nullable=False),
        sa.Column("required", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("task_id", "depends_on", name="task_deps_pkey"),
    )
    op.create_index("task_deps_by_dependency", "task_deps", ["depends_on"])


def downgrade() -> None:
    op.drop_index("task_deps_by_dependency", table_name="task_deps")
    op.drop_table("task_deps")
