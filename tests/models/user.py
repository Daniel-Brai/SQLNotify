from typing import TYPE_CHECKING

from sqlmodel import Field, Relationship, SQLModel

from .user_posts import UserPostLink

if TYPE_CHECKING:
    from .post import Post


class User(SQLModel, table=True):

    __tablename__ = "users"  # type: ignore[assignment]

    id: int | None = Field(default=None, primary_key=True, index=True)
    email: str
    name: str

    posts: list["Post"] = Relationship(
        back_populates="authors", link_model=UserPostLink
    )
