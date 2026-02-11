from importlib.metadata import version

from .constants import PACKAGE_NAME
from .notifiers import Notifier
from .types import ChangeEvent, FilterOnParams, Operation

__version__ = version(PACKAGE_NAME)

__all__ = [
    "ChangeEvent",
    "FilterOnParams",
    "Notifier",
    "Operation",
]
