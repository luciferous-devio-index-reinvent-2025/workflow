from .interval_http_client import IntervalHttpClient

http_client_contentful = IntervalHttpClient(0.2)
http_client_notion = IntervalHttpClient(0.5)

__all__ = ["IntervalHttpClient", "http_client_contentful", "http_client_notion"]
