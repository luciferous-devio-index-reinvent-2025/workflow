from base64 import b64encode
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from logging import DEBUG, FileHandler

import orjson
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.data_classes.common import DictWrapper
from compression.zstd import compress
from pydantic import BaseModel

from src.utils.models import EnvironmentVariables, MasterData
from src.utils.variables import FILENAME_LOG


def custom_default(obj):
    if isinstance(obj, EnvironmentVariables):
        return {"type": str(type(obj))}
    if isinstance(obj, MasterData):
        return {
            "type": str(type(obj)),
            "value": {
                "posts": len(obj.posts),
                "authors": len(obj.authors),
                "thumbnails": len(obj.thumbnails),
                "meta_posts": len(obj.meta_posts),
            },
        }
    if isinstance(obj, tuple):
        return list(obj)
    if isinstance(obj, set):
        return {"type": str(type(obj)), "values": list(obj)}
    if isinstance(obj, bytes):
        compressed = compress(obj, level=3)
        encoded = b64encode(compressed)
        return {
            "type": "bytes (base64 encoded, zstd compressed)",
            "value": encoded.decode(),
        }
    if isinstance(obj, Decimal):
        if (num := int(obj)) == obj:
            return num
        else:
            return float(str(obj))
    if isinstance(obj, DictWrapper):
        return obj.raw_event
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    if is_dataclass(obj):
        if isinstance(obj, type):
            return {"type": str(obj)}
        else:
            return asdict(obj)
    try:
        return {"type": str(type(obj)), "value": str(obj)}
    except Exception as e:
        return {
            "type": str(type(obj)),
            "error": {"type": str(type(e)), "value": str(e)},
        }


def create_logger(name: str) -> Logger:
    return Logger(
        service=name,
        level=DEBUG,
        use_rfc3339=True,
        logger_handler=FileHandler(FILENAME_LOG),
        json_deserializer=orjson.loads,
        json_serializer=lambda x: orjson.dumps(x, default=custom_default).decode(),
    )
