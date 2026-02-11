from enum import Enum
from typing import Any, TypeAlias, TypedDict
from uuid import UUID

from typing_extensions import ReadOnly

EventID: TypeAlias = str | int | UUID | tuple[Any, ...] | Any


class Operation(str, Enum):
    """
    Enumeration of database operations that can be watched for changes

    Attributes:
        INSERT (str, "insert"): Represents an insert operation in the database.
        UPDATE (str, "update"): Represents an update operation in the database.
        DELETE (str, "delete"): Represents a delete operation in the database.
    """

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class ChangeEvent(TypedDict):
    """
    Schema to represent a database change event

    Attributes:
        schema (str): The name of the database schema where the change occurred.
        table (str): The name of the database table where the change occurred.
        operation (Operation): The type of database operation that occurred (insert, update, delete).
        id (EventID): The primary key value of the affected row. For single primary keys, this is the value directly.
                  For composite primary keys, this is a tuple of values in the order specified in primary_keys.
        extra_columns (dict[str, Any]): A dictionary containing any additional columns and their values that were affected by the change.
        timestamp (str): The timestamp when the change event occurred.
    """

    schema: str
    table: str
    operation: Operation
    id: ReadOnly[EventID]
    extra_columns: dict[str, Any]
    timestamp: str


class FilterOnParams(TypedDict):
    """
    Schema for parameters used to filter notifications when subscribing to changes.

    Attributes:
        column (str): The name of the column to filter on.
        value (Any): The value that the specified column should match for the notification to be triggered.
    """

    column: str
    value: Any
