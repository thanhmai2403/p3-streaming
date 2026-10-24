"""Đọc config.yaml (nguồn duy nhất của mọi tham số).

Thứ tự tìm file: biến môi trường P3_CONFIG -> <thư mục gốc repo>/config.yaml.
Trong container Flink, repo được mount ở /opt/p3 nên file là /opt/p3/config.yaml.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


@lru_cache(maxsize=None)
def load(path: str | None = None) -> dict:
    p = Path(path or os.environ.get("P3_CONFIG") or REPO_ROOT / "config.yaml")
    with open(p, encoding="utf-8") as f:
        return yaml.safe_load(f)


def get(dotted: str, default: Any = None, cfg: dict | None = None) -> Any:
    """get("detector.zscore.window") -> 24"""
    node: Any = cfg if cfg is not None else load()
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            if default is not None:
                return default
            raise KeyError(f"config.yaml thiếu khóa: {dotted}")
        node = node[part]
    return node
