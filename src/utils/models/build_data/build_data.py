from pydantic import BaseModel

from .card import Card


class BuildData(BaseModel):
    cards: list[Card]
    categories: dict[str, str]  # key: id, value: name
    tags: dict[str, str]  # key: id, value: name
