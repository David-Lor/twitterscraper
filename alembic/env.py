from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlmodel import SQLModel

from alembic import context

import os
import sys

try:
    import twitterscraper.entrypoint
except ModuleNotFoundError:
    sys.path.append(os.getcwd())
    import twitterscraper.entrypoint
from twitterscraper.settings import load_settings
# noinspection PyUnresolvedReferences
from twitterscraper.models.domain import *


config = context.config
fileConfig(config.config_file_name)

target_metadata = SQLModel.metadata

# Set sqlalchemy.url programatically from env settings
# noinspection PyProtectedMember
config.set_main_option("sqlalchemy.url", load_settings().persistence.uri)


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
