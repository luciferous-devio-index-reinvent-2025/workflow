from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
import compression.zstd as zstd
import orjson
from pydantic import BaseModel

from src.utils.logger import create_logger, logging_function
from src.utils.methods import create_key_master_data
from src.utils.models import EnvironmentVariables, MasterData

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


logger = create_logger(__name__)


@logging_function(logger)
def step_01_initialize(
    *, client_s3: S3Client = boto3.client("s3")
) -> tuple[EnvironmentVariables, MasterData]:
    env = EnvironmentVariables()
    master_data = load_master_data(
        bucket=env.bucket_name, key_prefix=env.key_prefix, client=client_s3
    )
    return env, master_data


@logging_function(logger)
def load_master_data(*, bucket: str, key_prefix: str, client: S3Client) -> MasterData:
    try:
        resp = client.get_object(
            Bucket=bucket, Key=create_key_master_data(key_prefix=key_prefix)
        )
    except client.exceptions.NoSuchKey:
        return MasterData()

    bin_raw = resp["Body"].read()
    bin_decompressed = zstd.decompress(bin_raw)
    raw_dict = orjson.loads(bin_decompressed)
    return MasterData.model_validate(raw_dict)
