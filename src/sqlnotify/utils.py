import inspect
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from .constants import MAX_SQLNOTIFY_IDENTIFER_BYTES, MAX_SQLNOTIFY_PAYLOAD_BYTES
from .exceptions import SQLNotifyException, SQLNotifyIdentifierSizeError, SQLNotifyPayloadSizeError


def extract_database_url(engine: AsyncEngine | Engine) -> str:
    """
    Extract the database URL from an engine for use with asyncpg LISTEN.

    Converts SQLAlchemy URL format to raw database DSN.

    Args:
        engine (Union[AsyncEngine, Engine]): SQLAlchemy Engine or AsyncEngine

    Returns:
        str: Database connection URL
    """

    url = engine.url.render_as_string(hide_password=False)

    sqlite_drivers = ["+aiosqlite"]
    postgres_drivers = ["+asyncpg", "+aiopg", "+psycopg2", "+psycopg", "+pg8000"]

    if "sqlite" in url:
        for driver in sorted(sqlite_drivers, key=len, reverse=True):
            url = url.replace(f"sqlite{driver}", "sqlite")
    elif "postgresql" in url:
        for driver in sorted(postgres_drivers, key=len, reverse=True):
            url = url.replace(f"postgresql{driver}", "postgresql")

    return url


def strip_database_query_params(db_url: str) -> str:
    """
    Strip query parameters from a PostgreSQL connection URL.

    Args:
        db_url (str): PostgreSQL connection URL

    Returns:
        str: Connection URL without query parameters

    """

    if "?" in db_url:
        db_url = db_url.split("?", 1)[0]

    return db_url


def replace_spaces_with_underscores(name: str) -> str:
    """
    Replace spaces in a string with underscores.

    Args:
        name (str): Input string

    Returns:
        str: String with spaces replaced by underscores
    """

    return name.strip().replace(" ", "_")


def validate_payload_size(payload_str: str, allow_overflow: bool = False) -> bool:
    """
    Validate payload size doesn't exceed PostgreSQL limit.

    Args:
        payload_str (str): JSON payload string
        allow_overflow (bool): If True, returns False instead of raising exception

    Returns:
        bool: True if valid, False if overflow and allow_overflow=True

    Raises:
        SQLNotifyPayloadSizeError: If payload too large and allow_overflow=False
    """

    payload_bytes = len(payload_str.encode("utf-8"))

    if payload_bytes > MAX_SQLNOTIFY_PAYLOAD_BYTES:
        if allow_overflow:
            return False

        raise SQLNotifyPayloadSizeError(
            f"Payload size {payload_bytes} bytes exceeds SQLNotify "
            f"limit of {MAX_SQLNOTIFY_PAYLOAD_BYTES} bytes. "
            f"Consider: (1) reducing extra_columns, (2) using use_overflow_table=True, "
            f"or (3) sending only ID and fetching full data from database."
        )

    return True


def hash_identifier(s: str, max_length: int = 20) -> str:
    """
    Hash an identifier string to a shorter unique string using SHA-256.

    Uses cryptographic hashing to minimize collision probability while keeping
    the output short enough for PostgreSQL identifier limits (63 bytes).

    Args:
        s (str): Input string to hash
        max_length (int): Maximum length of output string (default: 20 chars = ~50 bytes)

    Returns:
        str: Hashed string value, significantly shorter than input with very low collision probability

    Note:
        While collisions are theoretically possible with any hash, SHA-256 truncated to 20 characters
        (80 bits) provides ~1.2e24 possible values, making collisions extremely unlikely in practice.
    """
    import hashlib

    hash_bytes = hashlib.sha256(s.encode("utf-8")).digest()

    hex_hash = hash_bytes.hex()[:max_length]

    return hex_hash


def validate_identifier_size(
    identifiers: list[str],
    stop_on_first_error: bool = True,
) -> bool:
    """
    Validate that PostgreSQL identifiers (e.g. channel, function, trigger names) do not exceed the byte limit.

    Args:
        identifiers (list[str]): The identifiers to validate
        stop_on_first_error (bool): If True, raise on the first offending identifier. If False, collect
            all offending identifiers and raise a combined error message listing them.

    Returns:
        bool: True if all identifiers are within limits

    Raises:
        SQLNotifyIdentifierSizeError: If one or more identifiers exceed the PostgreSQL byte limit
    """

    errors: list[str] = []

    for identifier in identifiers:
        identifier_bytes = len(identifier.encode("utf-8"))

        if identifier_bytes > MAX_SQLNOTIFY_IDENTIFER_BYTES:
            msg = (
                f"Identifier '{identifier}' is {identifier_bytes} bytes, "
                f"and exceeds limit of {MAX_SQLNOTIFY_IDENTIFER_BYTES} bytes."
            )

            if stop_on_first_error:
                raise SQLNotifyIdentifierSizeError(msg)

            errors.append(msg)

    if errors:
        combined = "; ".join(errors)
        raise SQLNotifyIdentifierSizeError(combined)

    return True


def wrap_unhandled_error(
    logger_getter: Callable[..., logging.Logger] | None = None,
    reraise=True,
):
    """
    Decorator factory that wraps unhandled exceptions and converts them to SQLNotifyException
    """

    def _get_logger_from_call(*args, **kwargs):
        if callable(logger_getter):
            try:
                return logger_getter(*args, **kwargs)
            except Exception:
                return None

        return logger_getter

    def decorator(func: Callable[..., Any]):

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except SQLNotifyException:
                    raise
                except Exception as e:
                    logger = _get_logger_from_call(*args, **kwargs)
                    if logger:
                        logger.exception(f"Unhandled exception in {func.__name__}")

                    if reraise:
                        raise SQLNotifyException(f"SQLNotify unhandled exception caught. Error: {str(e)}") from e
                    else:
                        return None

            return async_wrapper

        else:

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except SQLNotifyException:
                    raise
                except Exception as e:
                    logger = _get_logger_from_call(*args, **kwargs)
                    if logger:
                        logger.exception(f"Unhandled exception in {func.__name__}")

                    if reraise:
                        raise SQLNotifyException(f"SQLNotify unhandled exception caught. Error: {str(e)}") from e
                    else:
                        return None

            return sync_wrapper

    return decorator
