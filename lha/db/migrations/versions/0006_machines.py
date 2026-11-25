"""add where tasks may run, for workers on more than one machine

Revision ID: 0006
Revises: 0005

A task that needs files only its session's home machine has, such as a git
workspace or a folder of documents, is marked local, and a session keeps
which machine is its home, where its mock world can be reached, and the
address the world listens on, which a resume keeps. Tasks that already
exist are marked local by their type, and a session without a home lets
any machine take its local tasks, as every machine could before.
"""

import sqlalchemy as sa
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("local", sa.Boolean(), server_default=sa.false(), nullable=False))
    op.add_column("sessions", sa.Column("home_machine", sa.Text(), nullable=True))
    op.add_column("sessions", sa.Column("world_url", sa.Text(), nullable=True))
    op.add_column("sessions", sa.Column("world_listen", sa.Text(), nullable=True))
    op.execute("UPDATE tasks SET local = true WHERE type IN ('extract_doc', 'check_all', 'fix_item')")


def downgrade() -> None:
    op.drop_column("sessions", "world_listen")
    op.drop_column("sessions", "world_url")
    op.drop_column("sessions", "home_machine")
    op.drop_column("tasks", "local")
