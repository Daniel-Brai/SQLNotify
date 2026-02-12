PACKAGE_NAME = "sqlnotify"

MAX_SQLNOTIFY_PAYLOAD_BYTES = 7999  # SQLNotify payload limit (8000 - 1 for terminator)

MAX_SQLNOTIFY_IDENTIFER_BYTES = 63  # SQLNotify identifier limit (63 bytes)

MAX_SQLNOTIFY_EXTRA_COLUMNS = 5  # Limit extra columns to help stay within payload size limit, but this is not a hard limit since column data size can vary greatly

MAX_SQLNOTIFY_EVENT_RETRIES = 3  # Max retries for any event in sqlnotify
