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
    """Mốc đo độ trễ (ingest_ts_ms, emit_ts_ms, recv_ts_ms, kill_ts_ms): CLOCK_MONOTONIC, ms.

    KHÔNG phải giờ thật (epoch). Trên WSL2 + Docker Desktop, mọi tiến trình (kể cả trong container)
    chạy chung một kernel nên dùng chung đồng hồ này; nó không bao giờ bị chỉnh nhảy như giờ thật
    (đã đo được giờ thật nhảy +66 s, +31 s, -34 s giữa lúc chạy). Chỉ dùng để TRỪ nhau.
    """
    return time.monotonic_ns() // 1_000_000


def wall_ms() -> int:
    """Giờ thật (epoch ms, UTC). Dùng cho event_time_ms của dữ liệu tổng hợp và thông tin trong meta."""
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


# ---------- tương thích tên gọi cũ của B ----------
# B đặt SENSOR_VERSION trước khi hai người thống nhất SCHEMA_VERSION (cùng giá trị 1).
SENSOR_VERSION = SCHEMA_VERSION

# Thứ tự trường theo README 5.1 / 5.2 (dùng làm tài liệu và kiểm tra nhanh).
EVENT_FIELD_ORDER = ("v", "station_id", "param", "value", "event_time_ms",
                     "ingest_ts_ms", "seq")
ALERT_FIELD_ORDER = OUT_FIELDS


def serialize_event(ev: dict) -> str:
    """Sự kiện -> 1 dòng JSON UTF-8 (đã kiểm tra hợp lệ)."""
    return dumps(validate_event(dict(ev)))


def serialize_record(rec: dict) -> str:
    """Bản ghi ra (alert/probe) -> 1 dòng JSON UTF-8."""
    return dumps(validate_output(dict(rec)))


def make_record(ev: dict, kind: str, engine: str, detector, score, emit_ts_ms=None) -> dict:
    """Tạo bản ghi ra theo kind (README 5.2); giữ API cũ của B."""
    if kind == "probe":
        return make_probe(ev, engine, emit_ts_ms)
    return make_alert(ev, engine, detector, score, emit_ts_ms)


def parse_record(raw) -> dict | None:
    """Parse 1 dòng JSON ra bản ghi; dòng rỗng -> None (API cũ của B)."""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    raw = (raw or "").strip()
    if not raw:
        return None
    return json.loads(raw)


# ---------- StructType cho Spark (chỉ job Spark cần) ----------
def spark_event_struct():
    """StructType của sự kiện vào cho from_json (README 5.1)."""
    from pyspark.sql.types import (DoubleType, IntegerType, LongType,
                                   StringType, StructField, StructType)
    return StructType([
        StructField("v", IntegerType(), False),
        StructField("station_id", StringType(), False),
        StructField("param", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("event_time_ms", LongType(), False),
        StructField("ingest_ts_ms", LongType(), True),
        StructField("seq", LongType(), False),
    ])


def spark_record_struct():
    """StructType của bản ghi ra (alert + probe) cho applyInPandasWithState/Kafka."""
    from pyspark.sql.types import (DoubleType, IntegerType, LongType,
                                   StringType, StructField, StructType)
    return StructType([
        StructField("v", IntegerType(), False),
        StructField("kind", StringType(), False),
        StructField("engine", StringType(), False),
        StructField("detector", StringType(), True),
        StructField("station_id", StringType(), False),
        StructField("param", StringType(), False),
        StructField("event_time_ms", LongType(), False),
        StructField("value", DoubleType(), False),
        StructField("score", DoubleType(), True),
        StructField("seq", LongType(), False),
        StructField("ingest_ts_ms", LongType(), True),
        StructField("emit_ts_ms", LongType(), True),
    ])
