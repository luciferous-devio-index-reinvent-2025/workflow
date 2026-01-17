from pydantic import BaseModel

from .author import Author
from .meta_post import MetaPost
from .post import Post


class MasterData(BaseModel):
    posts: dict[str, Post] = {}  # key: url
    authors: dict[str, Author] = {}  # key: id
    thumbnails: dict[str, str] = {}  # key: id, value: url
    meta_posts: dict[str, MetaPost] = {}  # key: url
    categories: dict[str, str] = {}  # key: id, value: name
    tags: dict[str, str] = {}  # key: id, value: name
    prev_hash: str = ""
