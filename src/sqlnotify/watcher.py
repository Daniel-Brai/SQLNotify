import logging
from typing import Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Mapper

from .constants import PACKAGE_NAME
from .exceptions import SQLNotifyConfigurationError
from .types import Operation
from .utils import hash_identifier, replace_spaces_with_underscores, validate_identifier_size


class Watcher:
    """
    Configuration class for SQLNotify Watcher
    """

    def __init__(
        self,
        model: type,
        operation: Operation,
        extra_columns: list[str] | None,
        trigger_columns: list[str] | None,
        primary_keys: list[str],
        channel_label: str | None = None,
        use_overflow_table: bool = False,
        logger: logging.Logger | None = None,
    ):
        self.model = model
        self.operation = operation
        self.extra_columns = extra_columns or []
        self.trigger_columns = trigger_columns
        self.primary_keys = primary_keys
        self.channel_label = channel_label
        self.use_overflow_table = use_overflow_table
        self._logger = logger

        try:
            sa_mapper: Mapper[Any] = sa_inspect(model)
            self.table_name = (
                sa_mapper.local_table.name  # type: ignore[attr-defined]
                or model.__table__.name  # type: ignore[attr-defined]
                or model.__tablename__  # type: ignore[attr-defined]
                or model.__name__.lower()
            )
            self.schema_name = sa_mapper.local_table.schema or getattr(model, "__table_args__", {}).get(
                "schema", "public"
            )
        except Exception as e:
            if self._logger:
                self._logger.error(
                    f"Error inspecting model '{model.__name__}': {str(e)}",
                    exc_info=True,
                )

            raise SQLNotifyConfigurationError(
                f"Failed to inspect model '{model.__name__}'. Ensure it's a valid SQLAlchemy or SQLModel model"
            ) from e

        self.channel_name = f"{PACKAGE_NAME}_{self.schema_name}_{self.table_name}_{self.operation.value}"
        if self.channel_label:
            self.channel_name = f"{PACKAGE_NAME}_{replace_spaces_with_underscores(self.channel_label)}"

        hashed_channel_name = hash_identifier(self.channel_name)

        self.function_name = f"notify_{hashed_channel_name}"
        self.trigger_name = f"trigger_{hashed_channel_name}"

        validate_identifier_size([self.channel_name, self.function_name, self.trigger_name])

        if self.use_overflow_table:
            self.overflow_table_name = f"{PACKAGE_NAME}_overflow"
