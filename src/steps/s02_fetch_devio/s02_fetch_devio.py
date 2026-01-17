from datetime import datetime
from typing import TypedDict

import orjson

from src.utils.interval_http_client import http_client_contentful
from src.utils.logger import create_logger, logging_function
from src.utils.models import Author, EnvironmentVariables, MasterData, Post


class ResponseEntries(TypedDict):
    total: int
    skip: int
    limit: int
    items: list[dict]


LIMIT_POSTS = 300

logger = create_logger(__name__)


@logging_function(logger)
def step_02_fetch_devio(*, env: EnvironmentVariables, master_data: MasterData) -> bool:
    """DevIOからre:Inventの特集カテゴリの記事を収集する

    注意: master_dataは中身を更新されます (副作用)
    """
    mapping_all_posts = fetch_mapping_all_posts(
        base_url=env.base_url_api_contentful,
        reference_category=env.reference_category,
        contentful_token=env.contentful_token,
    )

    union_authors, union_thumbnail_ids, flag_update_post = (
        update_posts_and_parse_not_existing_resources(
            mapping_all_posts=mapping_all_posts, master_data=master_data
        )
    )

    mapping_authors = fetch_mapping_authors(
        base_url=env.base_url_api_contentful,
        union_authors=union_authors,
        contentful_token=env.contentful_token,
    )

    mapping_thumbnails = fetch_mapping_thumbnails(
        base_url=env.base_url_api_contentful,
        union_thumbnail_ids=union_thumbnail_ids,
        contentful_token=env.contentful_token,
    )

    flag_update_author_and_thumbnail = update_thumbnails_and_authors(
        mapping_authors=mapping_authors,
        mapping_thumbnails=mapping_thumbnails,
        master_data=master_data,
    )

    return flag_update_post or flag_update_author_and_thumbnail


def fetch_mapping_all_posts(
    *, base_url: str, reference_category: str, contentful_token: str
) -> dict[str, Post]:
    total = 0
    index = 0
    is_first = True

    result = {}
    while is_first or index * LIMIT_POSTS <= total:
        if is_first:
            is_first = False
        url = create_url_fetching_posts(
            base_url=base_url, reference_category=reference_category, index=index
        )

        resp = http_client_contentful.get(
            url=url, headers={"Authorization": f"Bearer {contentful_token}"}
        )

        binary = resp.read()
        raw: ResponseEntries = orjson.loads(binary)
        total = raw["total"]

        for item in raw["items"]:
            post = convert_to_post(item=item)
            result[post.url] = post

        index += 1

    return result


@logging_function(logger)
def create_url_fetching_posts(
    *, base_url: str, reference_category: str, index: int
) -> str:
    return f"{base_url}/public/entries?fields.referenceCategory.en-US.sys.id={reference_category}&content_type=blogPost&skip={index * LIMIT_POSTS}&limit={LIMIT_POSTS}"


@logging_function(logger)
def convert_to_post(*, item: dict) -> Post:
    def parse_url() -> str:
        slug = item["fields"]["slug"]["en-US"]
        return f"https://dev.classmethod.jp/articles/{slug}/"

    def parse_title() -> str:
        return item["fields"]["title"]["en-US"]

    def parse_author_id():
        return item["fields"]["author"]["en-US"]["sys"]["id"]

    def parse_thumbnail_id() -> str | None:
        try:
            return item["fields"]["thumbnail"]["en-US"]["sys"]["id"]
        except KeyError:
            return None

    def parse_thumbnail_url() -> str | None:
        try:
            return item["fields"]["wpThumbnail"]["en-US"]
        except KeyError:
            return None

    def parse_published_at() -> datetime:
        text = item["sys"]["firstPublishedAt"]
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%f%z")

    dt = parse_published_at()

    return Post(
        url=parse_url(),
        title=parse_title(),
        author_id=parse_author_id(),
        thumbnail_id=parse_thumbnail_id(),
        thumbnail_url=parse_thumbnail_url(),
        date=dt.strftime("%Y.%m.%d"),
        unixtime=int(dt.timestamp() * 1000),
    )


@logging_function(logger)
def update_posts_and_parse_not_existing_resources(
    *, mapping_all_posts: dict[str, Post], master_data: MasterData
) -> tuple[set[str], set[str], bool]:
    # master_data.postsに副作用あり

    union_authors = set()
    union_thumbnail_ids = set()

    flag_update = False
    for post_id, post_value in mapping_all_posts.items():
        m_post = master_data.posts.get(post_id)
        if m_post is None or post_value != m_post:
            master_data.posts[post_value.url] = post_value
            flag_update = True
        if post_value.author_id not in master_data.authors:
            union_authors.add(post_value.author_id)
        if (
            post_value.thumbnail_id
            and post_value.thumbnail_id not in master_data.thumbnails
        ):
            union_thumbnail_ids.add(post_value.thumbnail_id)

    return union_authors, union_thumbnail_ids, flag_update


@logging_function(logger)
def fetch_mapping_authors(
    *, base_url: str, union_authors: set[str], contentful_token: str
) -> dict[str, Author]:
    result = {}
    headers = {"Authorization": f"Bearer {contentful_token}"}

    for author_id in union_authors:
        url = create_url_fetching_author(base_url=base_url, author_id=author_id)
        resp = http_client_contentful.get(url=url, headers=headers)
        binary = resp.read()
        raw: ResponseEntries = orjson.loads(binary)
        author = convert_to_author(payload=raw)
        if author:
            result[author.id] = author

    return result


@logging_function(logger)
def create_url_fetching_author(*, base_url: str, author_id: str) -> str:
    return f"{base_url}/entries?sys.id={author_id}&content_type=authorProfile"


@logging_function(logger)
def convert_to_author(*, payload: ResponseEntries) -> Author | None:
    if payload["total"] != 1:
        return None

    item = payload["items"][0]
    slug = item["fields"]["slug"]["en-US"]
    try:
        thumbnail_url = item["fields"]["thumbnail"]["en-US"]
    except KeyError:
        thumbnail_url = "https://images.ctfassets.net/ct0aopd36mqt/1dD7b8HkT2sbiJzUIewMTD/e5cdc6f33c4fdd9d798f11a4564612ff/eyecatch_developersio_darktone_1200x630.jpg?w=256&fm=webp"
    return Author(
        id=item["sys"]["id"],
        name=item["fields"]["displayName"]["en-US"],
        thumbnail_url=thumbnail_url,
        url=f"https://dev.classmethod.jp/author/{slug}/",
    )


@logging_function(logger)
def fetch_mapping_thumbnails(
    *, base_url: str, union_thumbnail_ids: set[str], contentful_token: str
) -> dict[str, str]:
    result = {}
    headers = {"Authorization": f"Bearer {contentful_token}"}

    for thumbnail_id in union_thumbnail_ids:
        url = create_url_thumbnail(base_url=base_url, thumbnail_id=thumbnail_id)

        resp = http_client_contentful.get(url=url, headers=headers)

        binary = resp.read()
        raw = orjson.loads(binary)
        base = raw["fields"]["file"]["en-US"]["url"]
        result[thumbnail_id] = f"https:{base}"

    return result


@logging_function(logger)
def create_url_thumbnail(*, base_url: str, thumbnail_id: str) -> str:
    return f"{base_url}/assets/{thumbnail_id}"


@logging_function(logger)
def update_thumbnails_and_authors(
    *,
    mapping_authors: dict[str, Author],
    mapping_thumbnails: dict[str, str],
    master_data: MasterData,
) -> bool:
    flag_update = False
    for author_id, author_value in mapping_authors.items():
        m_author_value = master_data.authors.get(author_id)
        if m_author_value is None or author_value != m_author_value:
            master_data.authors[author_id] = author_value
            flag_update = True

    for thumbnail_id, thumbnail_url in mapping_thumbnails.items():
        m_thumbnail_url = master_data.thumbnails.get(thumbnail_id)
        if m_thumbnail_url is None or thumbnail_url != m_thumbnail_url:
            master_data.thumbnails[thumbnail_id] = thumbnail_url
            flag_update = True

    return flag_update
