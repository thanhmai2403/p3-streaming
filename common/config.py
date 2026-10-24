"""Đọc config.yaml – nguồn duy nhất của mọi tham số (README mục 4).

Code không được hard-code tham số; mọi thứ đọc qua đây:

    from common.config import load_config
    cfg = load_config()
    print(cfg["window"], cfg["kafka"]["topic_in"])

Thứ tự tìm file: biến môi trường P3_CONFIG -> config.yaml ở thư mục gốc repo
(tính từ vị trí file này). Trả về dict thuần (đã parse YAML).
"""
import os
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "config.yaml"

_cache = {}


def resolve_config_path(path=None):
    """Đường dẫn file config: đối số -> P3_CONFIG -> config.yaml ở gốc repo."""
    if path:
        return Path(path)
    env = os.environ.get("P3_CONFIG")
    if env:
        return Path(env)
    return DEFAULT_CONFIG_PATH


def load_config(path=None, refresh=False):
    """Đọc và cache config.yaml. `refresh=True` để đọc lại từ đĩa."""
    p = resolve_config_path(path)
    key = str(p.resolve())
    if key not in _cache or refresh:
        with open(p, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        _cache[key] = cfg
    return _cache[key]


def get(path_dotted, default=None, config=None):
    """Lấy giá trị theo đường dẫn chấm, ví dụ get('kafka.topic_in')."""
    cfg = config if config is not None else load_config()
    node = cfg
    for part in path_dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node
