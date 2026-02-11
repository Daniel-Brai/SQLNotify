from contextlib import asynccontextmanager

from ..notifiers.notifier import Notifier


@asynccontextmanager
async def sqlnotify_lifespan(notifier: Notifier):
    """
    Lifespan context manager for FastAPI.

    Automatically detects whether the notifier is using an async or sync engine
    and uses the appropriate methods (astart/astop for async, start/stop for sync).

    Examples:

        from fastapi import FastAPI # Or any ASGI framework that supports lifespan events
        from contextlib import asynccontextmanager

        notifier = Notifier(...)

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            async with sqlnotify_lifespan(notifier):
                yield

        app = FastAPI(lifespan=lifespan)
    """

    if notifier.async_mode:
        await notifier.astart()
        try:
            yield
        finally:
            await notifier.astop()
    else:
        notifier.start()
        try:
            yield
        finally:
            notifier.stop()
