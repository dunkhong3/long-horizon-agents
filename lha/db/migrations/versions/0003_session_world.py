"""add the world each session runs in

Revision ID: 0003
Revises: 0002

A session runs in a mock world generated from its seed, or with 'web' on the
real web, and every session that already exists ran in a mock world.
"""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sessions", sa.Column("world", sa.Text(), server_default="mock", nullable=False))


def downgrade() -> None:
    op.drop_column("sessions", "world")
