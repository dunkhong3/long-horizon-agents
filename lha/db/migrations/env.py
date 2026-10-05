"""Alembic's entry point, which runs the migrations on a connection that
lha/db/migrate.py hands over, so there is no alembic.ini and no second
place where the database URL is configured."""

from alembic import context

from lha.db.models import metadata

connection = context.config.attributes["connection"]
context.configure(connection=connection, target_metadata=metadata, compare_server_default=True)
with context.begin_transaction():
    context.run_migrations()
