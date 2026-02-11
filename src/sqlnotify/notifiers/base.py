import asyncio
import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Mapper
from typing_extensions import Self

from ..constants import MAX_SQLNOTIFY_EXTRA_COLUMNS, MAX_SQLNOTIFY_PAYLOAD_BYTES
from ..exceptions import SQLNotifyConfigurationError
from ..types import FilterOnParams, Operation
from ..watcher import Watcher


class BaseNotifier:
    """
    Base class for SQLNotify notifier

    It basically acts as a constructor class for registration and validation of watchers and subscribers of the notifier, but does not implement the actual listening logic.

    Raises:
        SQLNotifyConfigurationError: If there are configuration issues with watchers or subscribers
    """

    def __init__(self):
        self.watchers: list[Watcher] = []
        self.subscribers: dict[str, list[Callable]] = {}
        self._listening_task: asyncio.Task | None = None
        self._running = False
        self._logger = None
        self._listener_ready: asyncio.Event | None = None

    def watch(
        self,
        model: type,
        operation: Operation,
        extra_columns: list[str] | None = None,
        trigger_columns: list[str] | None = None,
        primary_keys: list[str] = ["id"],  # noqa: B006
        channel_label: str | None = None,
        use_overflow_table: bool = False,
    ) -> Self:
        """
        Register a watcher for a model and operation

        Args:
            model (Type): SQLModel or SQLAlchemy model class
            operation (Operation | str): Operation to watch (insert, update, delete)
            extra_columns (Optional[List[str]]): Additional columns to include in notifications. If None (the default),
                only the primary key(s) will be returned in the notification event.
            trigger_columns (Optional[List[str]]): For UPDATE, only trigger on these columns
            primary_keys (List[str]): List of primary key column names. Must be actual primary keys on the model. Defaults to ["id"]
            channel_label (Optional[str]): Custom label for the watcher
            use_overflow_table (bool): If True, large payloads are stored in overflow table

        Returns:
            Notifier: The Notifier instance

        Raises:
            SQLNotifyConfigurationError: If there are configuration issues with the watcher (e.g. invalid column names, primary keys not actually being primary keys, etc.)
        """

        model_columns = self._get_model_columns(model)
        model_primary_keys = self._get_model_primary_keys(model)

        if not primary_keys:
            raise SQLNotifyConfigurationError(f"primary_keys cannot be empty for model '{model.__name__}'")

        invalid_primary_keys = [pk for pk in primary_keys if pk not in model_columns]
        if invalid_primary_keys:
            raise SQLNotifyConfigurationError(
                f"Primary key column(s) {invalid_primary_keys} not found on model '{model.__name__}'. "
                f"Available columns: {sorted(model_columns)}"
            )

        not_actual_pks = [pk for pk in primary_keys if pk not in model_primary_keys]
        if not_actual_pks:
            raise SQLNotifyConfigurationError(
                f"Column(s) {not_actual_pks} exist on model '{model.__name__}' but are not primary keys. "
                f"Actual primary keys: {sorted(model_primary_keys) if model_primary_keys else 'None defined'}"
            )

        if extra_columns:
            invalid_extra_cols = [col for col in extra_columns if col not in model_columns]
            if invalid_extra_cols:
                raise SQLNotifyConfigurationError(
                    f"Extra column(s) {invalid_extra_cols} not found on model '{model.__name__}'. "
                    f"Available columns: {sorted(model_columns)}"
                )

        if trigger_columns:
            invalid_trigger_cols = [col for col in trigger_columns if col not in model_columns]
            if invalid_trigger_cols:
                raise SQLNotifyConfigurationError(
                    f"Trigger column(s) {invalid_trigger_cols} not found on model '{model.__name__}'. "
                    f"Available columns: {sorted(model_columns)}"
                )

        if extra_columns and len(extra_columns) > MAX_SQLNOTIFY_EXTRA_COLUMNS:
            if self._logger:
                self._logger.warning(
                    f"SQLNotify Watcher for {model.__name__}.{operation} has {len(extra_columns)} extra_columns. "
                    f"This may exceed SQLNotify {MAX_SQLNOTIFY_PAYLOAD_BYTES} byte limit. "
                    f"Consider setting use_overflow_table=True or reducing extra_columns."
                )

        watcher = Watcher(
            model=model,
            operation=operation,
            extra_columns=extra_columns,
            trigger_columns=trigger_columns,
            primary_keys=primary_keys,
            channel_label=channel_label,
            use_overflow_table=use_overflow_table,
            logger=self._logger,
        )

        self.watchers.append(watcher)

        return self

    def subscribe(
        self,
        model: type | str,
        operation: Operation,
        filters: list[FilterOnParams] | None = None,
    ):
        """
        Decorator to subscribe to change events

        Args:
            model (Union[Type, str]): Model class or class name as string to watch
            operation (Operation | str): Operation to watch
            filters (Optional[List[FilterOnParams]]): Optional list of column filters to watch specific records

        Returns:
            Callable: Decorator for subscriber function

        Raises:
            SQLNotifyConfigurationError: If model is not registered as a watcher for the specified operation,
                                         or if filter column names don't exist on the model
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            if inspect.iscoroutinefunction(func):

                @wraps(func)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    return await func(*args, **kwargs)

                registered_func = async_wrapper
            else:

                @wraps(func)
                def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                    return func(*args, **kwargs)

                registered_func = sync_wrapper

            matching_watcher = None

            for watcher in self.watchers:
                watcher_matches = False

                if isinstance(model, str):
                    watcher_matches = watcher.model.__name__ == model and watcher.operation == operation
                else:
                    watcher_matches = watcher.model == model and watcher.operation == operation

                if watcher_matches:
                    matching_watcher = watcher
                    break

            if not matching_watcher:
                raise SQLNotifyConfigurationError(
                    f"Model {model if isinstance(model, str) else model.__name__} is not registered as a watcher for operation {operation}"
                )

            if filters:
                model_columns = self._get_model_columns(matching_watcher.model)
                for filter_param in filters:
                    column_name = filter_param["column"]

                    if column_name not in model_columns:
                        raise SQLNotifyConfigurationError(
                            f"Column '{column_name}' does not exist on model '{matching_watcher.model.__name__}'. "
                            f"Available columns: {', '.join(model_columns)}"
                        )

            channel = matching_watcher.channel_name

            if filters:
                sorted_filters = sorted(filters, key=lambda f: f["column"])
                filter_parts = [f"{f['column']}:{f['value']}" for f in sorted_filters]
                channel = f"{channel}:{'|'.join(filter_parts)}"

            if channel not in self.subscribers:
                self.subscribers[channel] = []

            self.subscribers[channel].append(registered_func)

            return registered_func

        return decorator

    def _get_model_columns(self, model: type) -> set[str]:
        """
        Get all column names from a model.

        Args:
            model (Type): SQLModel or SQLAlchemy model class

        Returns:
            set[str]: Set of column names
        """

        try:
            mapper: Mapper[Any] = sa_inspect(model)
            return {c.key for c in mapper.column_attrs}
        except Exception:
            return set()

    def _get_model_primary_keys(self, model: type) -> set[str]:
        """
        Get actual primary key column names from a model.

        Args:
            model (Type): SQLModel or SQLAlchemy model class

        Returns:
            set[str]: Set of primary key column names
        """

        try:
            mapper: Mapper[Any] = sa_inspect(model)
            return {column.name for column in mapper.primary_key}
        except Exception:
            return set()
