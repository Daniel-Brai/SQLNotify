import logging
from unittest.mock import Mock

import pytest

from sqlnotify.constants import PACKAGE_NAME
from sqlnotify.exceptions import SQLNotifyConfigurationError
from sqlnotify.types import Operation
from sqlnotify.utils import hash_identifier
from sqlnotify.watcher import Watcher
from tests.models import (
    InvalidModel, 
    Post,
    SessionLog, 
    User,
    UserPostLink
)


class TestWatcher:

    def test_initialization(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email", "name"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.model == User
        assert watcher.operation == Operation.INSERT
        assert watcher.extra_columns == ["email", "name"]
        assert watcher.trigger_columns is None
        assert watcher.primary_keys == ["id"]
        assert watcher.channel_label is None
        assert watcher.use_overflow_table is False

    def test_initialization_with_all_parameters(self):

        logger = logging.getLogger("test")

        watcher = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email"],
            trigger_columns=["email", "name"],
            primary_keys=["id"],
            channel_label="user_updates",
            use_overflow_table=True,
            logger=logger,
        )

        assert watcher.model == User
        assert watcher.operation == Operation.UPDATE
        assert watcher.extra_columns == ["email"]
        assert watcher.trigger_columns == ["email", "name"]
        assert watcher.primary_keys == ["id"]
        assert watcher.channel_label == "user_updates"
        assert watcher.use_overflow_table is True
        assert watcher._logger == logger

    def test_initialization_with_empty_extra_columns(self):

        watcher = Watcher(
            model=User,
            operation=Operation.DELETE,
            extra_columns=None,
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.extra_columns == []

    def test_initialization_with_composite_primary_keys(self):

        watcher = Watcher(
            model=UserPostLink,
            operation=Operation.INSERT,
            extra_columns=["name"],
            trigger_columns=None,
            primary_keys=["user_id", "post_id"],
        )

        assert watcher.primary_keys == ["user_id", "post_id"]

    def test_table_name_extraction(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.table_name == "users"

    def test_schema_name_default(self):
        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.schema_name == "public"

    def test_custom_schema_extraction(self):

        watcher = Watcher(
            model=SessionLog,
            operation=Operation.INSERT,
            extra_columns=["ip_address"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.schema_name == "analytics"

    def test_table_name_from_post_model(self):

        watcher = Watcher(
            model=Post,
            operation=Operation.INSERT,
            extra_columns=["title"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.table_name == "posts"

    def test_default_channel_name_format(self):
        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        expected_channel = f"{PACKAGE_NAME}_public_users_insert"
        assert watcher.channel_name == expected_channel

    def test_channel_name_with_update_operation(self):

        watcher = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        expected_channel = f"{PACKAGE_NAME}_public_users_update"
        assert watcher.channel_name == expected_channel

    def test_channel_name_with_delete_operation(self):

        watcher = Watcher(
            model=User,
            operation=Operation.DELETE,
            extra_columns=None,
            trigger_columns=None,
            primary_keys=["id"],
        )

        expected_channel = f"{PACKAGE_NAME}_public_users_delete"
        assert watcher.channel_name == expected_channel

    def test_channel_name_with_custom_schema(self):

        watcher = Watcher(
            model=SessionLog,
            operation=Operation.INSERT,
            extra_columns=["ip_address"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        expected_channel = f"{PACKAGE_NAME}_analytics_session_logs_insert"
        assert watcher.channel_name == expected_channel

    def test_custom_channel_label(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
            channel_label="my_custom_channel",
        )

        expected_channel = f"{PACKAGE_NAME}_my_custom_channel"
        assert watcher.channel_name == expected_channel

    def test_custom_channel_label_with_spaces(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
            channel_label="my custom channel",
        )

        expected_channel = f"{PACKAGE_NAME}_my_custom_channel"
        assert watcher.channel_name == expected_channel

    def test_custom_channel_label_with_leading_trailing_spaces(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
            channel_label="  my channel  ",
        )

        expected_channel = f"{PACKAGE_NAME}_my_channel"
        assert watcher.channel_name == expected_channel

    def test_function_name_generation(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        expected_channel = f"{PACKAGE_NAME}_public_users_insert"
        expected_hash = hash_identifier(expected_channel)
        expected_function = f"notify_{expected_hash}"

        assert watcher.function_name == expected_function

    def test_trigger_name_generation(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        expected_channel = f"{PACKAGE_NAME}_public_users_insert"
        expected_hash = hash_identifier(expected_channel)
        expected_trigger = f"trigger_{expected_hash}"

        assert watcher.trigger_name == expected_trigger

    def test_function_and_trigger_names_are_unique(self):

        watcher1 = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        watcher2 = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher1.function_name != watcher2.function_name
        assert watcher1.trigger_name != watcher2.trigger_name

    def test_function_and_trigger_names_with_custom_label(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
            channel_label="custom_label",
        )

        expected_channel = f"{PACKAGE_NAME}_custom_label"
        expected_hash = hash_identifier(expected_channel)
        expected_function = f"notify_{expected_hash}"
        expected_trigger = f"trigger_{expected_hash}"

        assert watcher.function_name == expected_function
        assert watcher.trigger_name == expected_trigger

    def test_hashed_names_are_consistent(self):

        watcher1 = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        watcher2 = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher1.function_name == watcher2.function_name
        assert watcher1.trigger_name == watcher2.trigger_name
        assert watcher1.channel_name == watcher2.channel_name

    def test_overflow_table_disabled_by_default(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.use_overflow_table is False
        assert not hasattr(watcher, "overflow_table_name")

    def test_overflow_table_enabled(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
            use_overflow_table=True,
        )

        assert watcher.use_overflow_table is True
        assert hasattr(watcher, "overflow_table_name")
        assert watcher.overflow_table_name == f"{PACKAGE_NAME}_overflow"

    def test_overflow_table_name_format(self):

        watcher = Watcher(
            model=Post,
            operation=Operation.UPDATE,
            extra_columns=["title", "content"],
            trigger_columns=None,
            primary_keys=["id"],
            use_overflow_table=True,
        )

        expected_overflow_table = f"{PACKAGE_NAME}_overflow"
        assert watcher.overflow_table_name == expected_overflow_table

    def test_invalid_model_raises_configuration_error(self):

        with pytest.raises(SQLNotifyConfigurationError) as exc_info:

            Watcher(
                model=InvalidModel, 
                operation=Operation.INSERT,
                extra_columns=None,
                trigger_columns=None,
                primary_keys=["id"],
            )

        assert "Failed to inspect model 'InvalidModel'" in str(exc_info.value)

    def test_error_with_logger(self):

        logger = Mock(spec=logging.Logger)

        with pytest.raises(SQLNotifyConfigurationError):
            Watcher(
                model=InvalidModel, 
                operation=Operation.INSERT,
                extra_columns=None,
                trigger_columns=None,
                primary_keys=["id"],
                logger=logger,
            )

        logger.error.assert_called_once()

        call_args = logger.error.call_args

        assert "Error inspecting model 'InvalidModel'" in call_args[0][0]

    def test_none_model_raises_error(self):
        with pytest.raises((SQLNotifyConfigurationError, TypeError, AttributeError)):

            Watcher(
                model=None, # type: ignore[arg-type]
                operation=Operation.INSERT,
                extra_columns=None,
                trigger_columns=None,
                primary_keys=["id"],
            )

    def test_insert_operation(self):
        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.operation == Operation.INSERT
        assert "insert" in watcher.channel_name

    def test_update_operation(self):

        watcher = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.operation == Operation.UPDATE
        assert "update" in watcher.channel_name

    def test_delete_operation(self):

        watcher = Watcher(
            model=User,
            operation=Operation.DELETE,
            extra_columns=None,
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.operation == Operation.DELETE
        assert "delete" in watcher.channel_name

    def test_trigger_columns_none(self):

        watcher = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.trigger_columns is None

    def test_trigger_columns_single(self):

        watcher = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email", "name"],
            trigger_columns=["email"],
            primary_keys=["id"],
        )

        assert watcher.trigger_columns == ["email"]

    def test_trigger_columns_multiple(self):

        watcher = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email", "name"],
            trigger_columns=["email", "name"],
            primary_keys=["id"],
        )

        assert watcher.trigger_columns == ["email", "name"]

    def test_trigger_columns_empty_list(self):

        watcher = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email"],
            trigger_columns=[],
            primary_keys=["id"],
        )

        assert watcher.trigger_columns == []

    def test_single_primary_key(self):

        watcher = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher.primary_keys == ["id"]

    def test_composite_primary_keys(self):

        watcher = Watcher(
            model=UserPostLink,
            operation=Operation.INSERT,
            extra_columns=["name"],
            trigger_columns=None,
            primary_keys=["user_id", "post_id"],
        )

        assert watcher.primary_keys == ["user_id", "post_id"]
        assert len(watcher.primary_keys) == 2

    def test_primary_keys_order_preserved(self):

        watcher = Watcher(
            model=UserPostLink,
            operation=Operation.INSERT,
            extra_columns=["name"],
            trigger_columns=None,
            primary_keys=["user_id", "post_id"],
        )

        assert watcher.primary_keys[0] == "user_id"
        assert watcher.primary_keys[1] == "post_id"

    def test_different_operations_same_model_creates_unique_watchers(self):

        watcher_insert = Watcher(
            model=User,
            operation=Operation.INSERT,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        watcher_update = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        watcher_delete = Watcher(
            model=User,
            operation=Operation.DELETE,
            extra_columns=None,
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher_insert.channel_name != watcher_update.channel_name
        assert watcher_insert.channel_name != watcher_delete.channel_name
        assert watcher_update.channel_name != watcher_delete.channel_name

        assert watcher_insert.function_name != watcher_update.function_name
        assert watcher_insert.trigger_name != watcher_update.trigger_name

    def test_same_operation_different_columns_creates_unique_watchers(self):

        watcher1 = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["email"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        watcher2 = Watcher(
            model=User,
            operation=Operation.UPDATE,
            extra_columns=["name"],
            trigger_columns=None,
            primary_keys=["id"],
        )

        assert watcher1.channel_name == watcher2.channel_name
        assert watcher1.function_name == watcher2.function_name
        assert watcher1.trigger_name == watcher2.trigger_name

        assert watcher1.extra_columns != watcher2.extra_columns
