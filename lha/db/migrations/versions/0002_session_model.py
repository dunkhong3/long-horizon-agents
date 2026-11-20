"""add the model, context window and budget of each session

Revision ID: 0002
Revises: 0001

A session runs on the fake model unless it names a real one, with the fake
model's 2000-token window and no budget in money, so every session that
already exists gets those.
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sessions", sa.Column("model", sa.Text(), server_default="fake", nullable=False))
    op.add_column(
        "sessions", sa.Column("context_window", sa.Integer(), server_default="2000", nullable=False)
    )
    op.add_column("sessions", sa.Column("budget_usd", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("sessions", "budget_usd")
    op.drop_column("sessions", "context_window")
    op.drop_column("sessions", "model")
