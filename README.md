# SQLNotify

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Build and Test SQLNotify](https://github.com/Daniel-Brai/SQLNotify/actions/workflows/ci.yml/badge.svg)](https://github.com/Daniel-Brai/SQLNotify/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Daniel-Brai/SQLNotify/branch/main/graph/badge.svg)](https://codecov.io/gh/Daniel-Brai/SQLNotify)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**React to database changes in near real-time.**

SQLNotify leverages database native notification mechanisms (like PostgreSQL's LISTEN/NOTIFY) to provide instant notifications when your database records change. It supports FastAPI, Starlette, and other ASGI frameworks.

## Motivation for SQLNotify?

I started SQLNotify as an experiment for my projects that require real-time updates based on database changes. I wanted a solution that was simple to integrate, efficient, and didn't require external dependencies like message queues or change data capture tools.

SQLNotify uses the underling database system to push notifications the moment data changes. This enables:

- **Near real-time updates** - React to changes almost instantly
- **Lower database load** - No repeated SELECT queries checking for changes
- **Simplified architecture** - No need for message queues or external pub/sub systems
- **Type-safe** - Full typing support with SQLAlchemy 2.0+ with support for SQLModel models too
- **Production-ready** - It handles large payloads with overflow tables and automatic cleanup
- **Extensible** - Pluggable dialect system allows support for different databases

## Installation

By default, sqlnotify install with PostgreSQL support:

```bash
pip install sqlnotify
```

With SQLite support:

```bash
pip install "sqlnotify[sqlite]"
```

With SQLModel support and all database drivers supported by SQLNotify:

```bash
pip install "sqlnotify[all]"
```

Using other package managers:

```bash
# Using uv
uv add sqlnotify
# With SQLite support
uv add "sqlnotify[sqlite]"

# Using poetry
poetry add sqlnotify
# With SQLite support
poetry add "sqlnotify[sqlite]"
```

## Quick Start

### Basic Usage with FastAPI

```python
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel, Field
from sqlnotify import Notifier, Operation, ChangeEvent
from sqlnotify.adapters.asgi import sqlnotify_lifespan
from contextlib import asynccontextmanager

# Define your models
class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    email: str
    name: str

# Create async or syncengine
engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
# engine = create_async_engine("sqlite+aiosqlite:///./myapp.db")

# Initialize notifier
notifier = Notifier(db_engine=engine)

# Register watchers for models and operations
# By default `extra_columns` is None and only primary key(s) (e.g., `id`) are returned in events.
notifier.watch(User, Operation.INSERT)
notifier.watch(User, Operation.UPDATE, extra_columns=["email"])

# Subscribe to changes
@notifier.subscribe(User, Operation.INSERT)
async def on_user_created(event: ChangeEvent):
    print(f"New user created: {event.id}")

@notifier.subscribe(User, Operation.UPDATE)
# or @notifier.subscribe("User", Operation.UPDATE)
async def on_user_updated(event: ChangeEvent):
    print(f"User {event.id} updated")

# You can also filter by specific column values
@notifier.subscribe(User, Operation.UPDATE, filters=[{"column": "id", "value": 42}])
async def on_specific_user_updated(event: ChangeEvent):
    print(f"User 42 was updated")

# Setup lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with sqlnotify_lifespan(notifier):
        yield

app = FastAPI(lifespan=lifespan)

@app.post("/users/")
async def create_user(email: str, name: str):
    # Your user creation logic
    # Notification will fire automatically when database triggers
    return {"email": email, "name": name}
```

### Without ASGI Lifespan Management

```python
from sqlalchemy import create_engine
from sqlnotify import Notifier, Operation

engine = create_engine("postgresql+psycopg2://user:pass@localhost/db")

notifier = Notifier(db_engine=engine)

# Register watchers
notifier.watch(User, Operation.INSERT, extra_columns=["email", "name"])

@notifier.subscribe(User, Operation.INSERT)
def on_user_created(event: ChangeEvent):
    print(f"User {event.id} created")

# Start/stop manually (sync engine)
notifier.start()
# ... your application logic ...
notifier.stop()

# For async engine, use async methods:
# await notifier.astart()
# ... your application logic ...
# await notifier.astop()
```

### Using SQLite

See [SQLITE_QUICKSTART.md](docs/SQLITE_QUICKSTART.md) for the SQLite quick start guide.

## Features

### Pluggable Dialect System

SQLNotify uses a dialect-based architecture to support different databases. The dialect is automatically detected from your SQLAlchemy engine:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from sqlnotify import Notifier

# PostgreSQL dialect is automatically selected
engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
notifier = Notifier(db_engine=engine)

# Access dialect information
print(notifier.dialect_name)  # "postgresql"
```

**Currently supported dialects:**

- **PostgreSQL** - Uses native LISTEN/NOTIFY mechanism (instant, <10ms latency)
- **SQLite** - Uses hybrid polling and in-memory queue (20-50ms latency, adaptive CPU usage)

### Automatic Trigger Management

SQLNotify automatically creates and manages database triggers for your models based on operations you want to listen for:

```python
notifier = Notifier(db_engine=engine)

# Register watchers for different models and operations
notifier.watch(User, Operation.INSERT, extra_columns=["email", "name"])
notifier.watch(User, Operation.UPDATE, extra_columns=["email"])
notifier.watch(Post, Operation.INSERT, extra_columns=["title"])

# No extra columns, just the primary key column which is usually "id"
notifier.watch(Comment, Operation.INSERT)
# if your model has a different primary key column name, specify it with primary_keys parameter
# notifier.watch(Comment, Operation.INSERT, primary_keys=["comment_id"])
# as so the `ChangeEvent.id` will be the value of the `comment_id` column instead of `id`
```

SQLNotify triggers are created on startup and optionally cleaned up when the notifier stops.

### Watch Specific Columns

Monitor only specific columns for changes:

```python
notifier.watch(
    User,
    Operation.UPDATE,
    extra_columns=["email", "name"],
    trigger_columns=["email"]  # Only trigger on email changes
)

@notifier.subscribe(User, Operation.UPDATE)
async def on_email_changed(event: ChangeEvent):
    new_email = event.extra_columns.get("email")
    print(f"User {event.id} changed email to {new_email}")
```

### Custom Primary Keys

Specify custom primary key column names (useful for composite keys or non-standard primary keys):

```python
# Single primary key with custom name
notifier.watch(
    User,
    Operation.INSERT,
    extra_columns=["email"],
    primary_keys=["user_id"]  # Custom primary key column
)

# Composite primary key
notifier.watch(
    OrderItem,
    Operation.UPDATE,
    extra_columns=["quantity"],
    primary_keys=["order_id", "item_id"]  # Composite primary key
)

@notifier.subscribe(OrderItem, Operation.UPDATE)
async def on_order_item_updated(event: ChangeEvent):
    order_id, item_id = event.id
    print(f"Order item ({order_id}, {item_id}) updated")

# Empty extra_columns - only primary key(s) will be in the payload
notifier.watch(
    User,
    Operation.DELETE,
    extra_columns=None,  # No extra columns needed which is the default
    primary_keys=["id"]
)
```

**Note**:

- The default is `primary_keys=["id"]`
- For single primary keys, `event.id` is the value directly
- For composite primary keys, `event.id` is a tuple of values in the order specified
- **All specified `primary_keys` must be actual primary key columns on the model** - the system validates this at runtime
- All columns in `extra_columns`, `trigger_columns`, and `primary_keys` are validated to ensure they exist on the model
- `extra_columns` can be an empty list if you only need the primary key(s)

### Watch Specific Records

Subscribe to changes for specific record IDs or column values:

```python
# Filter by ID
@notifier.subscribe(User, Operation.UPDATE, filters=[{"column": "id", "value": 42}])
async def on_vip_user_updated(event: ChangeEvent):
    print(f"VIP user {event.id} was updated")

# Filter by multiple columns
@notifier.subscribe(
    User,
    Operation.UPDATE,
    filters=[
        {"column": "status", "value": "active"},
        {"column": "role", "value": "admin"}
    ]
)
async def on_active_admin_updated(event: ChangeEvent):
    print(f"Active admin user {event.id} was updated")
```

### Overflow Tables for Large Payloads

SQLNotify uses overflow tables for large payloads. SQLNotify handles this automatically if you enable overflow tables per watcher basis:

```python
notifier.watch(
    User,
    Operation.INSERT,
    extra_columns=["email", "name", "bio", "preferences"],
    use_overflow_table=True  # Large payloads stored in overflow table
)
```

### Notifications

Send custom notifications through the same channel system:

```python
# Notify with a model instance (async engine)
await notifier.anotify(
    User,
    Operation.UPDATE,
    payload={"id": 123, "email": "user@example.com"}
)

# Notify with custom channel label
await notifier.anotify(
    "User",
    Operation.INSERT,
    payload={"id": 456},
    channel_label="custom_channel"
)

# Notify with large payload using overflow table
# This automatically handles payloads larger than 7999 bytes
await notifier.anotify(
    User,
    Operation.INSERT,
    payload=large_payload_dict,
    use_overflow_table=True
)

# For sync engines, use notify() instead:
# notifier.notify(User, Operation.UPDATE, payload={"id": 123})
```

**Note**: The `notify`/`anotify` methods validate payload size and can use overflow tables for large payloads. If `use_overflow_table=True`, payloads exceeding SQLNotify limit are automatically stored in the overflow table, and only an overflow ID is sent through the notification channel.

### Model Change Detection

Automatically recreate triggers when model schemas change:

```python
notifier = Notifier(
    db_engine=engine,
    revoke_on_model_change=True  # Drop and recreate triggers on schema changes
)

# Register watchers
# By default `extra_columns` is None and only primary key(s) (e.g., `id`) are returned in events.
notifier.watch(User, Operation.INSERT)
```

### Custom Channel Labels

Use custom channel names for logical grouping:

```python
notifier.watch(
    User,
    Operation.INSERT,
    extra_columns=["email"],
    channel_label="new_registrations"
)

@notifier.subscribe("User", Operation.INSERT, channel_label="new_registrations")
async def on_new_registration(event: ChangeEvent):
    print("New user registered")
```

## Real-World Example: WebSocket Notifications

```python
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlmodel import SQLModel, Field, select
from sqlnotify import Notifier, Operation, ChangeEvent
from sqlnotify.adapters.asgi import sqlnotify_lifespan
from contextlib import asynccontextmanager
from typing import List

# Models
class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    email: str
    name: str

class Post(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    title: str
    content: str
    user_id: int = Field(foreign_key="user.id")

# Database setup
engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# Notifier setup
notifier = Notifier(db_engine=engine)

# Register watchers
notifier.watch(User, Operation.INSERT, extra_columns=["email", "name"])
notifier.watch(Post, Operation.INSERT, extra_columns=["title", "user_id"])

@notifier.subscribe(User, Operation.INSERT)
async def broadcast_new_user(event: ChangeEvent):
    await manager.broadcast({
        "type": "user_created",
        "id": event.id,
        "email": event.extra_columns.get("email"),
        "name": event.extra_columns.get("name")
    })

@notifier.subscribe(Post, Operation.INSERT)
async def broadcast_new_post(event: ChangeEvent):
    await manager.broadcast({
        "type": "post_created",
        "id": event.id,
        "title": event.extra_columns.get("title"),
        "user_id": event.extra_columns.get("user_id")
    })

@notifier.subscribe(User, Operation.UPDATE)
async def broadcast_user_update(event: ChangeEvent):
    await manager.broadcast({
        "type": "user_updated",
        "id": event.id
    })

# Lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with sqlnotify_lifespan(notifier):
        yield

app = FastAPI(lifespan=lifespan)

# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    async for _ in websocket.iter_text():
        pass  # Just keep connection alive for broadcasts
    manager.disconnect(websocket)

# API endpoints
@app.post("/users/")
async def create_user(email: str, name: str):
    async with async_session_maker() as session:
        user = User(email=email, name=name)
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user

@app.post("/posts/")
async def create_post(title: str, content: str, user_id: int):
    async with async_session_maker() as session:
        post = Post(title=title, content=content, user_id=user_id)
        session.add(post)
        await session.commit()
        await session.refresh(post)
        return post
```

## Configuration

```python
notifier = Notifier(
    db_engine=engine,                    # Required: AsyncEngine or Engine
    revoke_on_model_change=True,         # Drop and recreate triggers when model schema changes
    cleanup_on_start=False,              # Remove existing triggers/functions on startup
    use_logger=True                      # Enable internal logging
)
```

**Note**: Models and operations are registered individually using `notifier.watch()` method.

### Watch Method Options

```python
notifier.watch(
    model=User,                          # Model class to watch
    operation=Operation.UPDATE,          # INSERT, UPDATE, or DELETE
    extra_columns=["email", "name"],     # Columns to include in notifications (can be empty list)
    trigger_columns=["email"],           # Only trigger on these column changes (UPDATE only)
    primary_keys=["id"],                 # Primary key columns (must be actual PKs on model)
    channel_label="custom_name",         # Custom channel identifier
    use_overflow_table=False             # Store large payloads and process them using the overflow table
)
```

## Database Support

SQLNotify uses a pluggable dialect system for database support:

| Database | Dialect | Status | Mechanism | Latency |
|----------|---------|--------|-----------|----------|
| PostgreSQL 9.0+ | `postgresql` | ✅ Supported | LISTEN/NOTIFY | <10ms |
| SQLite 3.0+ | `sqlite` | ✅ Supported | Hybrid Polling and In-Memory Queue | 20-50ms |
| MySQL | `mysql` | 🔜 Planned | - | - |

### When to Use SQLite

✅ **Use SQLite when:**

- When you are building small to medium applications
- You are running in single-process or embedded environments
- You need a lightweight, serverless database
- You want a simple setup without external dependencies or separate database servers
- You can tolerate ~20-50ms notification latency

❌ **Don't use SQLite when:**

- Building large scale distributed systems
- You are running in multi-process or multi-server architecture
- You need almost instant notifications (<10ms latency)
- You need very high throughput requirements (>5000 msgs/sec) and cross server communication

## Framework Support

SQLNotify works with any ASGI framework:

- **FastAPI** - Use `sqlnotify_lifespan` helper
- **Starlette** - Use `sqlnotify_lifespan` helper
- **Other ASGI frameworks** - Implement lifespan management manually

For synchronous frameworks, use sync mode:

```python
notifier = Notifier(db_engine=engine, ...)
notifier.start()  # Synchronous start
```

## How It Works

1. **Dialect Detection** - SQLNotify automatically detects the database dialect from your SQLAlchemy engine
2. **Trigger Creation** - The dialect creates database-specific triggers on your tables
3. **Change Detection** - When data changes, triggers fire and call notification functions
4. **NOTIFY** - Functions use database-native notification mechanisms (e.g., PostgreSQL's NOTIFY)
5. **LISTEN** - SQLNotify maintains a dedicated connection listening for notifications or polls for changes (SQLite)
6. **Event Distribution** - Incoming notifications are routed to subscribed callbacks
7. **Callback Execution** - Your subscriber functions are called with change events

## Performance Considerations

- **Dedicated Connection** - SQLNotify uses a separate connection for LISTEN to avoid blocking if the engine is PostgreSQL
- **Async by Default** - Built for asyncio, but supports sync mode when needed
- **Overflow Handling** - Large payloads automatically routed to overflow tables
- **Automatic Cleanup** - Consumed overflow records are cleaned up after 1 hour
- **Minimal Overhead** - Triggers are efficient as such notification delivery is near instant

## Contributing

I welcome any contribution. Please see the [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [SQLAlchemy](https://www.sqlalchemy.org/)
- PostgreSQL [LISTEN/NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) documentation
