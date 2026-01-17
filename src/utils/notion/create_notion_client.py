from datetime import datetime
from time import sleep

from httpx import Client as HttpClient
from httpx import HTTPTransport, Request, Response
from notion_client import Client

from src.utils.logger import create_logger, logging_function

logger = create_logger(__name__)


class RateLimitedTransport(HTTPTransport):
    dt_prev: datetime | None = None
    interval_sec: float | int

    def __init__(self, *args, interval_sec: float | int = 0.5, **kwargs):
        super().__init__(*args, **kwargs)
        self.interval_sec = interval_sec

    @logging_function(logger)
    def handle_request(
        self,
        request: Request,
    ) -> Response:
        if self.dt_prev:
            delta = datetime.now() - self.dt_prev
            wait = self.interval_sec - delta.total_seconds()
            if wait > 0:
                sleep(wait)

        try:
            return super().handle_request(request=request)
        finally:
            self.dt_prev = datetime.now()


@logging_function(logger)
def create_notion_client(*, notion_token: str) -> Client:
    transport = RateLimitedTransport()
    http_client = HttpClient(transport=transport)
    return Client(client=http_client, auth=notion_token)
