# __init__.py

import logging
import tomllib
from importlib import resources
from typing import Union


# Constants
__NUMERIC = Union[int, float]

# Setup
logging.getLogger().disabled = True

__version__ = "0.0.1"
resource_folder = resources.files("py621dl")

_cfg = tomllib.loads(resource_folder.joinpath("config.toml").read_text())


# Functions
def enable_logging(
        level: int = logging.INFO,
        /,
        *,
        filename: str = None
        ):

    logger = logging.getLogger()
    logger.disabled = False

    if filename:
        handler = logging.FileHandler(filename)
    else:
        handler = logging.StreamHandler()

    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    handler.setLevel(level)
    logger.addHandler(handler)


# Module imports

from py621dl.downloader import E621Downloader
from py621dl.reader import Reader
