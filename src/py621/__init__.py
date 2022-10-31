import tomllib
from importlib import resources

__version__ = "1.0.0"

_cfg = tomllib.loads(resources.read_text("py621", "config.toml"))
DATA_COLUMNS = ["id", "md5", "rating", "image_width", "image_height", "tag_string", "score", "is_deleted", "is_flagged"]
