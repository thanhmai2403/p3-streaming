"""Schema dữ liệu (README mục 5): sự kiện vào (topic sensor) và bản ghi ra (topic alerts).

Chỉ dùng thư viện chuẩn, không phụ thuộc pyspark, để mọi thành phần (producer,
measure, job) dùng chung được. Các hàm spark_*_struct() import pyspark bên trong
khi thật sự cần (chạy job Spark).
"""
import json

SENSOR_VERSION = 1

# Thứ tự trường theo README 5.1
EVENT_FIELDS = [
    ("v", int),
    ("station_id", str),
    ("param", str),
    ("value", float),
    ("event_time_ms", int),
    ("ingest_ts_ms", int),
    ("seq", int),
]

# Thứ tự trường theo README 5.2 (cả alert và probe)
ALERT_FIELDS = [
    ("v", int),
    ("kind", str),          # "alert" hoặc "probe"
    ("engine", str),        # flink | spark | offline
    ("detector", str),      # zscore | ewma | mp | hst ; probe -> null
    ("station_id", str),
    ("param", str),
    ("event_time_ms", int),
    ("value", float),
    ("score", float),       # probe -> null
    ("seq", int),
    ("ingest_ts_ms", int),
    ("emit_ts_ms", int),
]


def validate_event(obj):
    """Kiểm tra một sự kiện vào. Trả về dict chuẩn hóa; sai thì raise ValueError."""
    if not isinstance(obj, dict):
        raise ValueError(f"sự kiện phải là JSON object, nhận {type(obj).__name__}")
    for name, typ in EVENT_FIELDS:
        if name not in obj or obj[name] is None:
            raise ValueError(f"thiếu trường bắt buộc: {name}")
    if int(obj["v"]) != SENSOR_VERSION:
        raise ValueError(f"v phải là {SENSOR_VERSION}, nhận {obj['v']}")
    if int(obj["seq"]) < 0:
        raise ValueError(f"seq phải >= 0, nhận {obj['seq']}")
    return {
        "v": int(obj["v"]),
        "station_id": str(obj["station_id"]),
        "param": str(obj["param"]),
        "value": float(obj["value"]),
        "event_time_ms": int(obj["event_time_ms"]),
        "ingest_ts_ms": int(obj["ingest_ts_ms"]),
        "seq": int(obj["seq"]),
    }


def parse_event(raw):
    """Parse chuỗi (hoặc bytes) JSON của sự kiện vào thành dict đã kiểm tra."""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    return validate_event(json.loads(raw))


def serialize_event(ev):
    """Dict sự kiện -> chuỗi JSON UTF-8 một dòng (đúng README 5.1)."""
    return json.dumps(validate_event(ev), ensure_ascii=False)


def make_record(ev, kind, engine, detector, score, emit_ts_ms=None):
    """Tạo bản ghi ra (alert hoặc probe) từ một sự kiện, đúng README 5.2."""
    return {
        "v": SENSOR_VERSION,
        "kind": kind,
        "engine": engine,
        "detector": detector,
        "station_id": str(ev["station_id"]),
        "param": str(ev["param"]),
        "event_time_ms": int(ev["event_time_ms"]),
        "value": float(ev["value"]),
        "score": None if score is None else float(score),
        "seq": int(ev["seq"]),
        "ingest_ts_ms": ev.get("ingest_ts_ms"),
        "emit_ts_ms": emit_ts_ms,
    }


def parse_record(raw):
    """Parse chuỗi/bản ghi JSON ra thành dict; bản ghi rỗng/None trả về None."""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    raw = (raw or "").strip()
    if not raw:
        return None
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError(f"bản ghi phải là JSON object, nhận {type(obj).__name__}")
    return obj


def serialize_record(rec):
    """Dict bản ghi (alert/probe) -> chuỗi JSON UTF-8 một dòng."""
    return json.dumps(rec, ensure_ascii=False)


def spark_event_struct():
    """StructType của sự kiện vào cho from_json (chỉ job Spark cần)."""
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
