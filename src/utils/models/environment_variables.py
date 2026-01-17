from pydantic_settings import BaseSettings


class EnvironmentVariables(BaseSettings):
    contentful_token: str
    notion_token: str
    notion_data_source_id: str
    bucket_name: str
    key_prefix: str  # Github Organization名を使用する
    reference_category: str
    base_url_api_contentful: str
