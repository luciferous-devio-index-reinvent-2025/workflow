from datetime import datetime
from functools import wraps
from http.client import HTTPResponse
from time import sleep
from typing import Callable
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import orjson

from src.utils.logger import create_logger, logging_function

INTERVAL_FOR_INTERNAL_SERVER_ERROR = 90
MAX_REPEAT_COUNT_FOR_INTERNAL_SERVER_ERROR = 10

logger = create_logger(__name__)


class IntervalHttpClient:
    interval: int | float
    dt_prev: datetime | None = None
    count_internal_server_error: int = 0

    def __init__(self, interval_sec: int | float):
        self.interval = interval_sec

    @logging_function(logger)
    def get(self, *, url: str, headers: dict[str, str] | None = None) -> HTTPResponse:
        if self.dt_prev:
            delta = datetime.now() - self.dt_prev
            wait_sec = self.interval - delta.total_seconds()
            if wait_sec > 0:
                sleep(wait_sec)
        try:
            if headers is None:
                req = Request(url=url)
            else:
                req = Request(url=url, headers=headers)
            result = urlopen(req)
            self.count_internal_server_error = 0
            return result
        except HTTPError as e:
            if (
                e.status == 500
                and self.count_internal_server_error
                < MAX_REPEAT_COUNT_FOR_INTERNAL_SERVER_ERROR
            ):
                self.count_internal_server_error += 1
                sleep(INTERVAL_FOR_INTERNAL_SERVER_ERROR)
                return self.get(url=url, headers=headers)
            raise
        finally:
            self.dt_prev = datetime.now()
