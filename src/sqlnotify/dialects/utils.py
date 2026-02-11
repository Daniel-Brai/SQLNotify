import logging

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from ..exceptions import SQLNotifyUnSupportedDatabaseProviderError
from .base import BaseDialect
from .postgresql import PostgreSQLDialect
from .sqlite import SQLiteDialect


def detect_dialect_name(engine: AsyncEngine | Engine) -> str:
    """
    Detect the database dialect from an SQLAlchemy engine

    Args:
        engine (Union[AsyncEngine, Engine]): SQLAlchemy Engine or AsyncEngine

    Returns:
        str: The dialect name (e.g., 'postgresql', 'mysql', 'sqlite')
    """

    if isinstance(engine, AsyncEngine):
        dialect_name = engine.dialect.name
    else:
        dialect_name = engine.dialect.name

    return dialect_name


def get_dialect_for_engine(
    engine: AsyncEngine | Engine,
    async_engine: AsyncEngine | None = None,
    sync_engine: Engine | None = None,
    logger: logging.Logger | None = None,
    revoke_on_model_change: bool = True,
) -> BaseDialect:
    """
    Get the appropriate dialect implementation for the given engine

    Args:
        engine (Union[AsyncEngine, Engine]): SQLAlchemy Engine or AsyncEngine
        async_engine (Optional[AsyncEngine]): Async engine if available
        sync_engine (Optional[Engine]): Sync engine if available
        logger (Optional[logging.Logger]): Logger instance
        revoke_on_model_change (bool): Whether to revoke triggers on model change

    Returns:
        BaseDialect: The appropriate dialect implementation

    Raises:
        SQLNotifyUnSupportedDatabaseProviderError: If the dialect is not supported
    """

    dialect_name = detect_dialect_name(engine)

    if dialect_name == "postgresql":
        return PostgreSQLDialect(
            async_engine=async_engine,
            sync_engine=sync_engine,
            logger=logger,
            revoke_on_model_change=revoke_on_model_change,
        )
    elif dialect_name == "sqlite":
        return SQLiteDialect(
            async_engine=async_engine,
            sync_engine=sync_engine,
            logger=logger,
            revoke_on_model_change=revoke_on_model_change,
        )

    raise SQLNotifyUnSupportedDatabaseProviderError(
        f"Database dialect '{dialect_name}' is not supported. " f"Currently supported dialects: PostgreSQL, SQLite."
    )
