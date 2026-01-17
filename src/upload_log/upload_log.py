from __future__ import annotations

from datetime import datetime
from io import BytesIO
from logging import DEBUG
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import boto3
from aws_lambda_powertools import Logger
from compression.zstd import compress
from pydantic_settings import BaseSettings

from src.utils.logger import logging_function
from src.utils.logger.create_logger import custom_default
from src.utils.variables import FILENAME_LOG

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class EnvironmentVariables(BaseSettings):
    bucket_name: str
    key_prefix: str  # Github Organization名を使用する


jst = ZoneInfo("Asia/Tokyo")
logger = Logger(
    service=__name__, level=DEBUG, use_rfc3339=True, json_default=custom_default
)


@logging_function(logger)
def upload_log(*, client: S3Client = boto3.client("s3")):
    env = EnvironmentVariables()
    logger.debug("environment variables", data={"env": env})
    dt_now = datetime.now(tz=jst)
    key = generate_key(key_prefix=env.key_prefix, dt=dt_now)
    binary_raw = load_log()
    binary_compressed = compress_log(binary=binary_raw)
    exec_upload(
        binary=binary_compressed, bucket=env.bucket_name, key=key, client=client
    )


@logging_function(logger)
def generate_rfid(*, dt: datetime) -> str:
    base_number = 9007199254740991  # 2 ** 53 - 1 (JSのNumber.MAX_SAFE_INTEGER)
    unixtime_ms = int(dt.timestamp() * 1000)
    return str(base_number - unixtime_ms)


@logging_function(logger)
def generate_key(*, key_prefix: str, dt: datetime) -> str:
    text_date = dt.strftime("%Y_%m_%d__%H_%M_%S")
    return f"{key_prefix}/logs/{generate_rfid(dt=dt)}__{text_date}.log"


@logging_function(logger, with_return=False)
def load_log() -> bytes:
    with open(FILENAME_LOG, "rb") as f:
        return f.read()


@logging_function(logger, with_args=False, with_return=False)
def compress_log(*, binary: bytes) -> bytes:
    return compress(data=binary, level=22)


@logging_function(logger, with_args=False)
def exec_upload(*, binary: bytes, bucket: str, key: str, client: S3Client):
    io = BytesIO(binary)
    client.upload_fileobj(
        Fileobj=io, Bucket=bucket, Key=key, ExtraArgs={"StorageClass": "ONEZONE_IA"}
    )
