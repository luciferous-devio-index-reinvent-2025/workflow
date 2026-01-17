from pydantic import BaseModel


class Post(BaseModel):
    url: str
    title: str
    author_id: str
    thumbnail_id: str | None
    thumbnail_url: str | None
    date: str
    unixtime: int
