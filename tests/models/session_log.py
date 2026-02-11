from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel


class SessionLog(SQLModel, table=True):

    __tablename__ = "session_logs"  # type: ignore[assignment]
    __table_args__ = {"schema": "analytics"}
    
    id: int = Field(default=None, primary_key=True)
    session_start: datetime
    session_end: Optional[datetime] = None
    actions_count: int = Field(default=0)
    ip_address: Optional[str] = Field(default=None, unique=True)