# SQLite Quick Start Guide

Get started with SQLNotify and SQLite

## Installation

```bash
pip install "sqlnotify[sqlite]"
```

## Without ASGI Lifespan

```python
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel, Field
from sqlnotify import Notifier, Operation, ChangeEvent

# 1. Define your model
class User(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    email: str

async def main():
    # 2. Create engine
    engine = create_async_engine("sqlite+aiosqlite:///./myapp.db")
    
    # 3. Create tables
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    
    # 4. Initialize notifier
    notifier = Notifier(db_engine=engine)
    
    # 5. Watch for changes
    # By default `extra_columns` is None which means only primary key(s) (e.g., `id`) will be included in events.
    # To include additional fields pass them in `extra_columns=[...]`.
    notifier.watch(User, Operation.INSERT)
    
    # 6. Subscribe to notifications
    @notifier.subscribe(User, Operation.INSERT)
    async def on_new_user(event: ChangeEvent):
        print(f"New user: {event['extra_columns']['email']}")
    
    # 7. Start listening
    await notifier.astart()
    
    # 8. Your app logic here
    # When done:
    await notifier.astop()

if __name__ == "__main__":
    asyncio.run(main())
```

## With ASGI Lifespan (Recommended for FastAPI)

```python
from fastapi import FastAPI
from contextlib import asynccontextmanager
from sqlnotify.adapters.asgi import sqlnotify_lifespan

# ... define models and notifier ...

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with sqlnotify_lifespan(notifier):
        yield

app = FastAPI(lifespan=lifespan)

@app.post("/users/")
async def create_user(email: str):
    # Insert user - notification fires automatically!
    return {"email": email}
```

## Common Patterns

### Watch Multiple Operations

```python
# Default: no extra_columns specified -> only primary key(s) included in notifications
notifier.watch(User, Operation.INSERT)
# Include specific columns when you need them
notifier.watch(User, Operation.UPDATE, extra_columns=["email"])
notifier.watch(User, Operation.DELETE)  # no extra columns needed
```

### Filter Notifications

```python
# Only notify for specific user
@notifier.subscribe(
    User, 
    Operation.UPDATE,
    filters=[{"column": "id", "value": 42}]
)
async def on_admin_update(event: ChangeEvent):
    print("Admin user was updated!")
```

### Composite Primary Keys

```python
class OrderItem(SQLModel, table=True):
    order_id: int = Field(primary_key=True)
    product_id: int = Field(primary_key=True)
    quantity: int

notifier.watch(
    OrderItem,
    Operation.INSERT,
    primary_keys=["order_id", "product_id"],
    extra_columns=["quantity"]
)
```

## Troubleshooting

**Notifications not received?**

- Ensure `await notifier.astart()` is called
- Wait ~100-200ms for polling cycle
- Check logs for errors

**Database locked?**

- Use WAL mode: `sqlite:///./myapp.db?mode=wal`
- Reduce concurrent writes

**Need faster notifications?**

- Consider PostgreSQL for instant LISTEN/NOTIFY
- SQLite is great for single-process apps
