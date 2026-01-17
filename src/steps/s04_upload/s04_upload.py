from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

import boto3
import orjson
from compression.zstd import compress

from src.utils.logger import create_logger, logging_function
from src.utils.methods import create_key_build_data, create_key_master_data
from src.utils.models import EnvironmentVariables, MasterData, MetaPost, Post
from src.utils.models.build_data import BuildData, Card

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = create_logger(__name__)


@logging_function(logger)
def step_04_upload(
    *,
    env: EnvironmentVariables,
    master_data: MasterData,
    client_s3: S3Client = boto3.client("s3"),
):
    build_data = create_build_data(master_data=master_data)
    binary_build_data = orjson.dumps(build_data.model_dump())
    hash_build_data = calculate_sha256(binary_build_data=binary_build_data)
    if hash_build_data != master_data.prev_hash:
        upload_build_data(
            binary_build_data=binary_build_data,
            bucket=env.bucket_name,
            key_prefix=env.key_prefix,
            client=client_s3,
        )
        master_data.prev_hash = hash_build_data
    upload_master_data(
        master_data=master_data,
        bucket=env.bucket_name,
        key_prefix=env.key_prefix,
        client=client_s3,
    )


@logging_function(logger)
def convert_to_card(*, meta_post: MetaPost, master_data: MasterData):
    post = master_data.posts[meta_post.url]
    author = master_data.authors[post.author_id]

    def parse_post_url() -> str:
        return meta_post.url

    def parse_post_title() -> str:
        return meta_post.title

    def parse_post_date() -> str:
        return post.date

    def parse_post_thumbnail() -> str:
        if post.thumbnail_url:
            return post.thumbnail_url
        else:
            return master_data.thumbnails[post.thumbnail_id]

    def parse_post_unixtime() -> str:
        return post.unixtime

    def parse_author_url() -> str:
        return author.url

    def parse_author_name() -> str:
        return author.name

    def parse_author_thumbnail() -> str:
        return author.thumbnail_url

    def parse_meta_category() -> str:
        return meta_post.category

    def parse_meta_tags() -> str:
        return meta_post.tags

    return Card(
        post_url=parse_post_url(),
        post_title=parse_post_title(),
        post_date=parse_post_date(),
        post_thumbnail=parse_post_thumbnail(),
        post_unixtime=parse_post_unixtime(),
        author_url=parse_author_url(),
        author_name=parse_author_name(),
        author_thumbnail=parse_author_thumbnail(),
        meta_category=parse_meta_category(),
        meta_tags=parse_meta_tags(),
    )


@logging_function(logger)
def create_build_data(*, master_data: MasterData) -> BuildData:
    all_cards = []
    for meta_post in master_data.meta_posts.values():
        logger.debug("meta_post", data={"meta_post": meta_post})
        if not meta_post.fixed:
            continue
        card = convert_to_card(meta_post=meta_post, master_data=master_data)
        all_cards.append(card)

    return BuildData(
        cards=all_cards, categories=master_data.categories, tags=master_data.tags
    )


@logging_function(logger, with_args=False)
def calculate_sha256(*, binary_build_data: bytes) -> str:
    return sha256(binary_build_data).hexdigest()


@logging_function(logger)
def upload_build_data(
    *, binary_build_data: bytes, bucket: str, key_prefix: str, client: S3Client
):
    io = BytesIO(binary_build_data)
    client.upload_fileobj(
        Fileobj=io, Bucket=bucket, Key=create_key_build_data(key_prefix=key_prefix)
    )


@logging_function(logger)
def upload_master_data(
    *, master_data: MasterData, bucket: str, key_prefix: str, client: S3Client
):
    binary_raw = orjson.dumps(master_data.model_dump())
    binary_compressed = compress(data=binary_raw, level=3)
    io = BytesIO(binary_compressed)
    client.upload_fileobj(
        Fileobj=io, Bucket=bucket, Key=create_key_master_data(key_prefix=key_prefix)
    )
