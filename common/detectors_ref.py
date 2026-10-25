"""Bản cài đặt THAM CHIẾU (Python thuần, chạy tuần tự) của z-score + EWMA + bỏ dữ liệu trễ.
Quy ước theo README mục 6. Flink và Spark phải cho kết quả khớp với file này.

Chạy thử:
    python common/detectors_ref.py --input data/processed/equiv_10k.parquet --output data/processed/ref_alerts.jsonl
    python common/detectors_ref.py --input data/processed/openaq_hn_injected.parquet --output data/processed/ref_alerts_full.jsonl
    (thêm --with-probes để phát thêm bản ghi kind=probe cho seq % 100 == 0, dùng khi kiểm tra RQ3)
"""
import argparse
import json
import math
import sys
from collections import deque

# Windows: tránh UnicodeEncodeError khi chạy từ cmd/redirect (cp1252)
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# Tham số (README mục 4): đọc từ config.yaml khi có (nguồn duy nhất của mọi tham số),
# nếu không thì dùng mặc định bên dưới.
WINDOW = 24
Z_THRESHOLD = 3.0
EWMA_LAMBDA = 0.3
EWMA_L = 3.0
WARMUP = 24
TOLERANCE_MS = 7_200_000
PROBE_EVERY = 100
EPS = 1e-9


def _load_params_from_config():
    """Đọc lại tham số từ config.yaml (nếu tồn tại). Lỗi thì giữ nguyên mặc định."""
    global WINDOW, Z_THRESHOLD, EWMA_LAMBDA, EWMA_L, WARMUP, TOLERANCE_MS, PROBE_EVERY
    try:
        import sys as _sys
        from pathlib import Path as _Path
        _sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
        from common import config as _config
        cfg = _config.load()      # config.yaml là nguồn duy nhất của tham số (README 4)
        WINDOW = int(_config.get("detector.zscore.window", cfg=cfg))
        Z_THRESHOLD = float(_config.get("detector.zscore.threshold", cfg=cfg))
        EWMA_LAMBDA = float(_config.get("detector.ewma.lambda", cfg=cfg))
        EWMA_L = float(_config.get("detector.ewma.L", cfg=cfg))
        WARMUP = int(_config.get("detector.warmup", cfg=cfg))
        TOLERANCE_MS = int(_config.get("late.tolerance_ms", cfg=cfg))
        PROBE_EVERY = int(_config.get("probe.every", cfg=cfg))
    except Exception:
        pass  # giữ mặc định


_load_params_from_config()


class KeyState:
    __slots__ = ("buf", "n", "m", "v", "max_et")

    def __init__(self):
        self.buf = deque(maxlen=WINDOW)
        self.n = 0
        self.m = 0.0
        self.v = 0.0
        self.max_et = None


def _alert(ev, detector, score, engine):
    return {
        "v": 1, "kind": "alert", "engine": engine, "detector": detector,
        "station_id": ev["station_id"], "param": ev["param"],
        "event_time_ms": ev["event_time_ms"], "value": ev["value"],
        "score": score, "seq": ev["seq"],
        "ingest_ts_ms": ev.get("ingest_ts_ms"), "emit_ts_ms": None,
    }


def process_event(state, ev, engine="offline"):
    """Xử lý 1 sự kiện. Trả về (list_alert, bị_bỏ_vì_trễ)."""
    et = ev["event_time_ms"]
    # 6.3: bỏ sự kiện trễ
    if state.max_et is not None and et < state.max_et - TOLERANCE_MS:
        return [], True
    state.max_et = et if state.max_et is None else max(state.max_et, et)

    x = float(ev["value"])
    out = []

    # 6.1 z-score: kiểm tra trước, cập nhật sau
    if state.n >= WARMUP and len(state.buf) == WINDOW:
        mean = sum(state.buf) / WINDOW
        std = math.sqrt(sum((b - mean) ** 2 for b in state.buf) / WINDOW)  # ddof = 0
        if std > EPS and abs(x - mean) / std > Z_THRESHOLD:
            out.append(_alert(ev, "zscore", (x - mean) / std, engine))

    # 6.2 EWMA: kiểm tra trước, cập nhật sau
    if state.n == 0:
        state.m, state.v = x, 0.0
    else:
        if state.n >= WARMUP and state.v > EPS and abs(x - state.m) > EWMA_L * math.sqrt(state.v):
            out.append(_alert(ev, "ewma", (x - state.m) / math.sqrt(state.v), engine))
        d = x - state.m
        state.m = state.m + EWMA_LAMBDA * d
        state.v = (1 - EWMA_LAMBDA) * (state.v + EWMA_LAMBDA * d * d)

    state.buf.append(x)
    state.n += 1
    return out, False


def run_reference(events, engine="offline", with_probes=False):
    """events: iterable các dict theo ĐÚNG thứ tự đến (thứ tự seq).
    Trả về (records, late_dropped)."""
    states = {}
    records = []
    late_dropped = 0
    for ev in events:
        key = (ev["station_id"], ev["param"])
        st = states.setdefault(key, KeyState())
        alerts, dropped = process_event(st, ev, engine)
        if dropped:
            late_dropped += 1
            continue
        records.extend(alerts)
        if with_probes and ev["seq"] % PROBE_EVERY == 0:
            p = _alert(ev, None, None, engine)
            p["kind"] = "probe"
            records.append(p)
    return records, late_dropped


def main():
    import pandas as pd
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="file parquet có cột station_id,param,value,event_time_ms[,seq]")
    ap.add_argument("--output", required=True, help="file alerts.jsonl")
    ap.add_argument("--with-probes", action="store_true", help="phát thêm bản ghi kind=probe cho seq % 100 == 0")
    ap.add_argument("--engine", default="offline", help="giá trị trường engine trong bản ghi (mặc định offline)")
    args = ap.parse_args()

    df = pd.read_parquet(args.input)
    if "seq" not in df.columns:
        df = df.reset_index(drop=True)
        df["seq"] = df.index
    df = df.sort_values("seq")
    events = df.to_dict("records")
    records, dropped = run_reference(events, engine=args.engine, with_probes=args.with_probes)
    with open(args.output, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    n_alert = sum(1 for r in records if r["kind"] == "alert")
    n_probe = sum(1 for r in records if r["kind"] == "probe")
    print(f"{len(events)} sự kiện -> {n_alert} alert + {n_probe} probe, bỏ vì trễ: {dropped}")


if __name__ == "__main__":
    main()
