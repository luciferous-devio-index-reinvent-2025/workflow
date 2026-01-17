from datetime import datetime, timezone
from functools import wraps
from typing import Callable
from uuid import uuid7

from aws_lambda_powertools import Logger


def logging_function(
    logger: Logger,
    *,
    write: bool = True,
    with_return: bool = True,
    with_args: bool = True,
) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def process(*args, **kwargs):
            function_name = func.__name__
            call_id = str(uuid7())
            dt_start = datetime.now(tz=timezone.utc)
            result = None
            err = None
            data_start = {
                "FunctionName": function_name,
                "CallID": call_id,
                "StartTime": str(dt_start),
            }
            try:
                if with_args:
                    data_start["Args"] = (args,)
                    data_start["KwArgs"] = kwargs
                if write:
                    logger.debug(
                        f"start function `{function_name}` ({call_id})", data=data_start
                    )
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                err = e
                logger.debug(
                    f"error occurred in {function_name}: [{type(e)}] {e}",
                    exc_info=True,
                    data={"ErrorType": str(type(e)), "ErrorMessage": str(e)},
                )
                raise
            finally:
                dt_end = datetime.now(tz=timezone.utc)
                delta = dt_end - dt_start
                data_end = {
                    **data_start,
                    "EndTime": str(dt_end),
                    "Duration": str(delta),
                }
                if with_return and not err:
                    data_end["Return"] = result
                if with_args or err:
                    data_end["Args"] = (args,)
                    data_end["KwArgs"] = kwargs
                if err:
                    data_end["Error"] = {
                        "type": str(type(err)),
                        "message": str(err),
                        "dict": err.__dict__,
                    }
                if err:
                    logger.debug(
                        f"failed function `{function_name}` ({call_id})",
                        data=data_end,
                        exc_info=True,
                    )
                elif write:
                    status = "failed" if err else "succeeded"
                    logger.debug(
                        f"{status} function `{function_name}` ({call_id})",
                        data=data_end,
                    )

        return process

    return decorator
