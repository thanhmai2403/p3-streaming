"""Đọc config.yaml (nguồn duy nhất của mọi tham số).

Thứ tự tìm file: biến môi trường P3_CONFIG -> <thư mục gốc repo>/config.yaml.
Trong container Flink/Spark, repo được mount ở /opt/p3 nên file là /opt/p3/config.yaml.

Hai bộ API cùng tồn tại (thống nhất khi hợp nhất nhánh A và B):

    from common import config

    cfg = config.load()                         # A: đọc + cache, trả dict thô
    config.get("detector.zscore.window")        # -> 24
    config.get("kafka.bootstrap_internal")      # -> "kafka:9092"

    cfg = config.load_config()                  # B: tên cũ, tương đương load()
    config.get("window")                        # B: tên khóa phẳng cũ (LEGACY_KEYS)

Các khóa phẳng thời B (`window`, `z_threshold`, ...) vẫn đọc được qua `get()` nhờ
LEGACY_KEYS, nhưng code mới phải dùng đường dẫn lồng nhau của config.yaml.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "config.yaml"

_cache: dict[str, dict] = {}

# Tên khóa PHẲNG thời B -> đường dẫn LỒNG NHAU trong config.yaml hiện tại.
# Chỉ để code cũ chạy được; code mới dùng thẳng đường dẫn bên phải.
LEGACY_KEYS = {
    "window": "detector.zscore.window",
    "z_threshold": "detector.zscore.threshold",
    "ewma_lambda": "detector.ewma.lambda",
    "ewma_L": "detector.ewma.L",
    "warmup": "detector.warmup",
    "tolerance_ms": "late.tolerance_ms",
    "probe_mod": "probe.every",
    "parallelism": "engine.parallelism",
    "state_backend": "engine.state_backend",
    "kafka.docker_brokers": "kafka.bootstrap_internal",
    "kafka.host_brokers": "kafka.bootstrap_host",
    "kafka.consumer_group": "flink.consumer_group",
}


def resolve_config_path(path=None) -> Path:
    """Đường dẫn file config: đối số -> P3_CONFIG -> config.yaml ở gốc repo."""
    if path:
        return Path(path)
    env = os.environ.get("P3_CONFIG")
    if env:
        return Path(env)
    return DEFAULT_CONFIG_PATH


def load(path: str | None = None, refresh: bool = False) -> dict:
    """Đọc và cache config.yaml. `refresh=True` để đọc lại từ đĩa."""
    p = resolve_config_path(path)
    key = str(p.resolve())
    if refresh or key not in _cache:
        with open(p, encoding="utf-8") as f:
            _cache[key] = yaml.safe_load(f) or {}
    return _cache[key]


def load_config(path: str | None = None, refresh: bool = False) -> dict:
    """Bí danh của load() – giữ tên cũ của B."""
    return load(path, refresh=refresh)


def get(dotted: str, default: Any = None, cfg: dict | None = None,
        config: dict | None = None) -> Any:
    """get("detector.zscore.window") -> 24

    `cfg=` (A) và `config=` (B) đều nhận dict đã đọc sẵn; bỏ trống thì tự load().
    Tên khóa phẳng cũ được dịch qua LEGACY_KEYS.
    """
    node: Any = cfg if cfg is not None else (config if config is not None else load())
    dotted = LEGACY_KEYS.get(dotted, dotted)
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            if default is not None:
                return default
            raise KeyError(f"config.yaml thiếu khóa: {dotted}")
        node = node[part]
    return node
