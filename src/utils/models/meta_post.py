from pydantic import BaseModel


class MetaPost(BaseModel):
    url: str
    notion_id: str
    title: str
    category: str | None
    tags: list[str]
    fixed: bool
    old_title: str
    unixtime_ms: int
