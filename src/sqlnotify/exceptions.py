class SQLNotifyException(Exception):
    """
    Base exception for SQLNotify
    """

    pass


class SQLNotifyConfigurationError(SQLNotifyException):
    """
    Raised when there is a configuration error in SQLNotify
    """

    pass


class SQLNotifyPayloadSizeError(SQLNotifyException):
    """
    Raised when the maximum payload size for SQLNotify is exceeded
    """

    pass


class SQLNotifyIdentifierSizeError(SQLNotifyException):
    """
    Raised when the maximum size of an identifier for SQLNotify is exceeded
    """

    pass


class SQLNotifyUnSupportedDatabaseProviderError(SQLNotifyException):
    """
    Raised when an unsupported database provider is specified for SQLNotify
    """

    pass
