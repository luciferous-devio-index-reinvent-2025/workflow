from src.utils.logger import create_logger, logging_function
from src.utils.variables import KEY_SUFFIX_MASTER_DATA

logger = create_logger(__name__)


@logging_function(logger)
def create_key_master_data(*, key_prefix: str) -> str:
    return f"{key_prefix}/{KEY_SUFFIX_MASTER_DATA}"
