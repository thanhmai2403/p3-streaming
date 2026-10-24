"""Schema dùng chung (README mục 5). Mọi script và cả hai engine dùng các hàm ở đây.

Sự kiện vào (topic `sensor`):
  {"v", "station_id", "param", "value", "event_time_ms", "ingest_ts_ms", "seq"}
Bản ghi ra (topic `alerts`), kind = "alert" | "probe":
  {"v", "kind", "engine", "detector", "station_id", "param", "event_time_ms", "value",
   "score", "seq", "ingest_ts_ms", "emit_ts_ms"}
`recv_ts_ms` KHÔNG do engine phát: measure_perf.py thêm vào khi ghi alerts.jsonl / latency.csv.
"""
from __future__ import annotations

import json
import math
import time

SCHEMA_VERSION = 1
SEQ_BASE = 10 ** 12                      # seq = producer_id * 10^12 + bộ đếm

EVENT_FIELDS = {
    "v": int, "station_id": str, "param": str, "value": float,
    "event_time_ms": int, "ingest_ts_ms": int, "seq": int,
}
OUT_FIELDS = ("v", "kind", "engine", "detector", "station_id", "param", "event_time_ms",
              "value", "score", "seq", "ingest_ts_ms", "emit_ts_ms")
KINDS = {"alert", "probe"}
ENGINES = {"flink", "spark", "offline"}
DETECTORS = {"zscore", "ewma", "mp", "hst"}


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def make_seq(producer_id: int, counter: int) -> int:
    return producer_id * SEQ_BASE + counter


def split_seq(seq: int) -> tuple[int, int]:
    """-> (producer_id, bộ đếm)"""
    return divmod(seq, SEQ_BASE)


def dumps(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


# ---------- sự kiện vào ----------
def make_event(station_id: str, param: str, value: float, event_time_ms: int,
               ingest_ts_ms: int, seq: int) -> dict:
    return {"v": SCHEMA_VERSION, "station_id": station_id, "param": param, "value": float(value),
            "event_time_ms": int(event_time_ms), "ingest_ts_ms": int(ingest_ts_ms), "seq": int(seq)}


def validate_event(ev: dict) -> dict:
    """Kiểm tra đủ trường + đúng kiểu. Trả lại chính ev (value ép về float). Sai -> ValueError."""
    for k, typ in EVENT_FIELDS.items():
        if k not in ev or ev[k] is None:
            raise ValueError(f"sự kiện thiếu trường '{k}': {ev}")
        if typ is float:
            if isinstance(ev[k], bool) or not isinstance(ev[k], (int, float)):
                raise ValueError(f"'{k}' phải là số: {ev}")
            ev[k] = float(ev[k])
            if not math.isfinite(ev[k]):
                raise ValueError(f"'{k}' không hữu hạn: {ev}")
        elif typ is int:
            if isinstance(ev[k], bool) or not isinstance(ev[k], int):
                raise ValueError(f"'{k}' phải là int: {ev}")
        elif not isinstance(ev[k], typ):
            raise ValueError(f"'{k}' phải là {typ.__name__}: {ev}")
    if ev["v"] != SCHEMA_VERSION:
        raise ValueError(f"schema v={ev['v']} không hỗ trợ (cần {SCHEMA_VERSION})")
    return ev


def parse_event(raw: str | bytes) -> dict:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    return validate_event(json.loads(raw))


# ---------- bản ghi ra ----------
def make_alert(ev: dict, engine: str, detector: str, score: float, emit_ts_ms: int) -> dict:
    return {"v": SCHEMA_VERSION, "kind": "alert", "engine": engine, "detector": detector,
            "station_id": ev["station_id"], "param": ev["param"],
            "event_time_ms": ev["event_time_ms"], "value": ev["value"], "score": float(score),
            "seq": ev.get("seq"), "ingest_ts_ms": ev.get("ingest_ts_ms"), "emit_ts_ms": emit_ts_ms}


def make_probe(ev: dict, engine: str, emit_ts_ms: int) -> dict:
    return {"v": SCHEMA_VERSION, "kind": "probe", "engine": engine, "detector": None,
            "station_id": ev["station_id"], "param": ev["param"],
            "event_time_ms": ev["event_time_ms"], "value": ev["value"], "score": None,
            "seq": ev["seq"], "ingest_ts_ms": ev["ingest_ts_ms"], "emit_ts_ms": emit_ts_ms}


def validate_output(rec: dict) -> dict:
    missing = [k for k in OUT_FIELDS if k not in rec]
    if missing:
        raise ValueError(f"bản ghi ra thiếu {missing}: {rec}")
    if rec["kind"] not in KINDS:
        raise ValueError(f"kind sai: {rec['kind']}")
    if rec["engine"] not in ENGINES:
        raise ValueError(f"engine sai: {rec['engine']}")
    if rec["kind"] == "alert" and rec["detector"] not in DETECTORS:
        raise ValueError(f"detector sai: {rec['detector']}")
    if rec["kind"] == "probe" and (rec["detector"] is not None or rec["score"] is not None):
        raise ValueError("probe phải có detector = null và score = null")
    return rec


def parse_output(raw: str | bytes) -> dict:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    return validate_output(json.loads(raw))
