"""Logic phát hiện của job Flink, viết bằng Python thuần (không phụ thuộc PyFlink) để test được riêng.

Cài đúng README mục 6:
  6.3 quy tắc trễ  -> 6.1 rolling z-score -> 6.2 EWMA (kiểm tra trước, cập nhật sau)
State của MỘT key (station_id, param) là một dict:
  {"buf": list[float] (<= window), "n": int, "m": float, "v": float, "max_et": int | None}
Flink lưu dict này trong ValueState (RocksDB).
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class Params:
    warmup: int = 24
    z_window: int = 24
    z_threshold: float = 3.0
    z_min_std: float = 1e-9
    ewma_lambda: float = 0.3
    ewma_L: float = 3.0
    ewma_min_var: float = 1e-9
    tolerance_ms: int = 7_200_000
    probe_every: int = 100

    @classmethod
    def from_config(cls, cfg: dict) -> "Params":
        d = cfg["detector"]
        return cls(
            warmup=int(d["warmup"]),
            z_window=int(d["zscore"]["window"]),
            z_threshold=float(d["zscore"]["threshold"]),
            z_min_std=float(d["zscore"]["min_std"]),
            ewma_lambda=float(d["ewma"]["lambda"]),
            ewma_L=float(d["ewma"]["L"]),
            ewma_min_var=float(d["ewma"]["min_var"]),
            tolerance_ms=int(cfg["late"]["tolerance_ms"]),
            probe_every=int(cfg["probe"]["every"]),
        )


def new_state() -> dict:
    return {"buf": [], "n": 0, "m": 0.0, "v": 0.0, "max_et": None}


def step(state: dict, value: float, event_time_ms: int, p: Params):
    """Xử lý một sự kiện của một key.

    Trả về (dropped, alerts) với alerts = [(detector, score), ...] theo thứ tự zscore rồi ewma.
    dropped=True nghĩa là sự kiện trễ quá ngưỡng: state KHÔNG đổi, không phát gì (kể cả probe).
    Hàm sửa `state` tại chỗ.
    """
    # 6.3 dữ liệu trễ: so với event_time lớn nhất đã thấy của key
    max_et = state["max_et"]
    if max_et is not None and event_time_ms < max_et - p.tolerance_ms:
        return True, []
    state["max_et"] = event_time_ms if max_et is None else max(max_et, event_time_ms)

    x = float(value)
    n = state["n"]
    alerts = []

    # 6.1 rolling z-score: so với 24 giá trị TRƯỚC, std quần thể (ddof = 0)
    buf = state["buf"]
    if n >= p.warmup:
        arr = np.asarray(buf, dtype=np.float64)
        mean = arr.mean()
        std = math.sqrt(((arr - mean) ** 2).mean())
        if std > p.z_min_std and abs(x - mean) / std > p.z_threshold:
            alerts.append(("zscore", (x - mean) / std))
    buf.append(x)
    if len(buf) > p.z_window:
        del buf[0]

    # 6.2 EWMA: kiểm tra trước, cập nhật sau (cập nhật cả bằng giá trị bất thường)
    if n == 0:
        state["m"], state["v"] = x, 0.0
    else:
        m, v = state["m"], state["v"]
        if n >= p.warmup and v > p.ewma_min_var and abs(x - m) > p.ewma_L * math.sqrt(v):
            alerts.append(("ewma", (x - m) / math.sqrt(v)))
        d = x - m
        state["m"] = m + p.ewma_lambda * d
        state["v"] = (1.0 - p.ewma_lambda) * (v + p.ewma_lambda * d * d)

    state["n"] = n + 1
    return False, alerts


def is_probe(seq: int, p: Params) -> bool:
    return seq % p.probe_every == 0
