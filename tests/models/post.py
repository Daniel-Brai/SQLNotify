from typing import TYPE_CHECKING

from sqlmodel import Field, Relationship, SQLModel

from .user_posts import UserPostLink

if TYPE_CHECKING:
    from .user import User


class Post(SQLModel, table=True):

    __tablename__ = "posts"  # type: ignore[assignment]

    id: int | None = Field(default=None, primary_key=True, index=True)
    title: str
    content: str

    authors: list["User"] = Relationship(
        back_populates="posts", link_model=UserPostLink
    )
