import json

from src.steps.s01_initialize import step_01_initialize
from src.steps.s02_fetch_devio import step_02_fetch_devio
from src.steps.s03_fetch_notion import step_03_fetch_notion
from src.steps.s04_upload import step_04_upload
from src.utils.logger import create_logger, logging_function

logger = create_logger(__name__)


@logging_function(logger)
def main():
    env, master_data = step_01_initialize()
    flag_devio = step_02_fetch_devio(env=env, master_data=master_data)
    logger.debug("start fetch notion")
    flag_notion = step_03_fetch_notion(env=env, master_data=master_data)

    with open("tmp/sample_data.json", "w") as f:
        json.dump(master_data.model_dump(), f, indent=2, ensure_ascii=False)

    step_04_upload(env=env, master_data=master_data)


if __name__ == "__main__":
    main()
