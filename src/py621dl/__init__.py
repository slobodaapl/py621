# __init__.py

import logging
import tomllib
from importlib import resources


def enable_logging(level: int = logging.INFO, /, *, filename: str = None):
    logger = logging.getLogger()
    logger.disabled = False

    if filename:
        handler = logging.FileHandler(filename)
    else:
        handler = logging.StreamHandler()

    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    handler.setLevel(level)
    logger.addHandler(handler)


logging.getLogger().disabled = True

__version__ = "0.1a0.dev0"

_cfg = tomllib.loads(resources.read_text("py621dl", "config.toml"))
DATA_COLUMNS = ["id", "md5", "rating", "image_width", "image_height", "tag_string", "score", "is_deleted", "is_flagged"]
