from pydantic import BaseModel


class Card(BaseModel):
    post_url: str
    post_title: str
    post_date: str
    post_thumbnail: str
    post_unixtime: int
    author_url: str
    author_name: str
    author_thumbnail: str
    meta_category: str
    meta_tags: list[str]
