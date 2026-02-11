from sqlmodel import Field, SQLModel


class UserPostLink(SQLModel, table=True):

    __tablename__ = "user_posts"  # type: ignore[assignment]

    user_id: int = Field(foreign_key="users.id", primary_key=True, ondelete="CASCADE")
    post_id: int = Field(foreign_key="posts.id", primary_key=True, ondelete="CASCADE")
