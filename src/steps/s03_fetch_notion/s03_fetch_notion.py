from notion_client import Client
from notion_client.helpers import collect_paginated_api

from src.utils.logger import create_logger, logging_function
from src.utils.models import EnvironmentVariables, MasterData, MetaPost
from src.utils.notion import create_notion_client

logger = create_logger(__name__)


@logging_function(logger)
def step_03_fetch_notion(*, env: EnvironmentVariables, master_data: MasterData) -> bool:
    client = create_notion_client(notion_token=env.notion_token)
    mapping_meta_posts, mapping_categories, mapping_tags = list_pages(
        data_source_id=env.notion_data_source_id, client=client
    )
    union_insert, union_update = parse_process_target_post_urls(
        mapping_meta_posts=mapping_meta_posts, master_data=master_data
    )
    flag_insert = insert_meta_posts(
        union_insert=union_insert,
        notion_data_source_id=env.notion_data_source_id,
        master_data=master_data,
        client=client,
    )

    flag_update = update_meta_posts(
        union_update=union_update, master_data=master_data, client=client
    )

    master_data.categories = mapping_categories
    master_data.tags = mapping_tags

    return flag_insert or flag_update


@logging_function(logger)
def convert_to_meta_post(
    *, page: dict
) -> tuple[MetaPost, dict[str, str], dict[str, str]]:
    props: dict = page["properties"]

    def parse_category() -> tuple[str | None, str | None]:
        select: dict[str, str] | None = props["category"]["select"]
        if select is None:
            return None, None
        else:
            return select["id"], select["name"]

    def parse_tags() -> dict[str, str]:
        multi_select: list[dict[str, str]] = props["tags"]["multi_select"]
        return {x["id"]: x["name"] for x in multi_select}

    category_id, category_name = parse_category()
    if category_id:
        mapping_category = {category_id: category_name}
    else:
        mapping_category = {}
    mapping_tags = parse_tags()

    meta_post = MetaPost(
        url=props["url"]["url"],
        notion_id=page["id"],
        title=props["title"]["title"][0]["plain_text"],
        category=category_id,
        tags=list(mapping_tags.keys()),
        fixed=props["fixed"]["checkbox"],
        old_title=props["old_title"]["rich_text"][0]["plain_text"],
        unixtime_ms=props["unixtime_ms"]["number"],
    )

    return meta_post, mapping_category, mapping_tags


@logging_function(logger)
def list_pages(
    *, data_source_id: str, client: Client
) -> tuple[dict[str, MetaPost], dict[str, str], dict[str, str]]:
    mapping_meta_posts = {}
    mapping_categories = {}
    mapping_tags = {}

    for page in collect_paginated_api(
        client.data_sources.query, data_source_id=data_source_id
    ):
        logger.debug("fetching page", data={"page": page})
        c_post, c_mapping_categories, c_mapping_tags = convert_to_meta_post(page=page)
        mapping_meta_posts[c_post.url] = c_post
        mapping_categories = {**mapping_categories, **c_mapping_categories}
        mapping_tags = {**mapping_tags, **c_mapping_tags}

    return mapping_meta_posts, mapping_categories, mapping_tags


@logging_function(logger)
def parse_process_target_post_urls(
    *, mapping_meta_posts: dict[str, MetaPost], master_data: MasterData
) -> tuple[set[str], set[str]]:
    union_insert = set()
    union_update = set()

    for url, post in master_data.posts.items():
        meta_post_current = mapping_meta_posts.get(url)

        if meta_post_current is None:
            union_insert.add(url)
            continue

        meta_post_prev = master_data.meta_posts.get(url)
        if meta_post_prev is None:
            union_insert.add(url)
            continue

        if meta_post_prev != meta_post_current:
            master_data.meta_posts[url] = meta_post_current

        if post.title != meta_post_current.old_title:
            union_update.add(url)

    return union_insert, union_update


@logging_function(logger)
def insert_meta_posts(
    *,
    union_insert: set[str],
    notion_data_source_id: str,
    master_data: MasterData,
    client: Client,
) -> bool:
    # 副作用: master_data.meta_posts
    flag = False
    for url in union_insert:
        flag = True
        post = master_data.posts[url]
        props = {
            "title": {"title": [{"text": {"content": post.title}}]},
            "old_title": {"rich_text": [{"text": {"content": post.title}}]},
            "url": {"url": post.url},
            "unixtime_ms": {"number": post.unixtime},
        }
        page = client.pages.create(
            parent={"data_source_id": notion_data_source_id}, properties=props
        )
        meta_post, mapping_categories, mapping_tags = convert_to_meta_post(page=page)
        master_data.meta_posts[url] = meta_post
        master_data.categories = {**master_data.categories, **mapping_categories}
        master_data.tags = {**master_data.tags, **mapping_tags}
    return flag


@logging_function(logger)
def update_meta_posts(
    *, union_update: set[str], master_data: MasterData, client: Client
) -> bool:
    # 副作用: master_data.meta_posts
    flag = False
    for url in union_update:
        flag = True
        post = master_data.posts[url]
        meta_post_prev = master_data.meta_posts[url]
        props = {
            "title": {"title": [{"text": {"content": post.title}}]},
            "old_title": {"rich_text": [{"text": {"content": post.title}}]},
            "fixed": {"checkbox": False},
        }
        page = client.pages.update(page_id=meta_post_prev.notion_id, properties=props)
        meta_post_cur, mapping_categories, mapping_tags = convert_to_meta_post(
            page=page
        )
        master_data.meta_posts[url] = meta_post_cur
        master_data.categories = {**master_data.categories, **mapping_categories}
        master_data.tags = {**master_data.tags, **mapping_tags}
    return flag
