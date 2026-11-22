"""add the parameters a domain keeps for each session

Revision ID: 0004
Revises: 0003

Some domains need more than the seed and the size, such as the repository
and the check command of a session on a real repository, which go in this
column, and every session that already exists has none.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sessions", sa.Column("params", JSONB(), server_default="{}", nullable=False))


def downgrade() -> None:
    op.drop_column("sessions", "params")
