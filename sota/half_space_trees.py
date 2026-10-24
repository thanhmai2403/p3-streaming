"""SOTA Half-Space Trees (B) — chạy offline trên dữ liệu OpenAQ đã chèn bất thường (README mục 10.3).

Pipeline: river.preprocessing.MinMaxScaler() | river.anomaly.HalfSpaceTrees(...).
- Chạy độc lập theo từng key (station_id, param), `score_one` trước rồi mới `learn_one`.
- Ngưỡng cảnh báo: phân vị 99% của điểm số trên 20% dữ liệu đầu của mỗi trạm (không dùng nhãn).
- Warm-up 24 mẫu đầu không phát alert (đúng quy ước chung).
- Xuất JSONL đúng schema mục 5.2 (engine="offline", detector="hst").

    python sota/half_space_trees.py --input data/processed/openaq_hn_injected.parquet \
        --output results/sota_hst/alerts.jsonl
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from river import anomaly, preprocessing

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

WARMUP = 24
THRESHOLD_FRACTION = 0.20
THRESHOLD_QUANTILE = 0.99
N_TREES = 25
HEIGHT = 15
WINDOW_SIZE = 250
SEED = 42


def detect_key(df_key, warmup):
    """df_key: 1 key (station_id, param), đã sắp theo event_time_ms. Trả về list alert dict."""
    n = len(df_key)
    if n <= warmup:
        return []

    x = df_key["value"].to_numpy(dtype="float64")
    model = preprocessing.MinMaxScaler() | anomaly.HalfSpaceTrees(
        n_trees=N_TREES, height=HEIGHT, window_size=WINDOW_SIZE, seed=SEED)

    scores = np.empty(n, dtype="float64")
    for i in range(n):
        d = {"value": float(x[i])}
        scores[i] = float(model.score_one(d))
        model.learn_one(d)

    n_first = int(np.ceil(THRESHOLD_FRACTION * n))
    threshold = float(np.percentile(scores[:n_first], THRESHOLD_QUANTILE * 100))

    alerts = []
    for i in range(warmup, n):
        if scores[i] > threshold:
            alerts.append({
                "v": 1,
                "kind": "alert",
                "engine": "offline",
                "detector": "hst",
                "station_id": str(df_key["station_id"].iloc[i]),
                "param": str(df_key["param"].iloc[i]),
                "event_time_ms": int(df_key["event_time_ms"].iloc[i]),
                "value": float(df_key["value"].iloc[i]),
                "score": float(scores[i]),
                "seq": int(df_key["seq"].iloc[i]),
                "ingest_ts_ms": None,
                "emit_ts_ms": None,
            })
    return alerts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/processed/openaq_hn_injected.parquet")
    ap.add_argument("--output", default="results/sota_hst/alerts.jsonl")
    ap.add_argument("--warmup", type=int, default=WARMUP)
    args = ap.parse_args()

    df = pd.read_parquet(args.input)
    df = df.sort_values(["event_time_ms", "station_id"], kind="stable").reset_index(drop=True)
    df["seq"] = np.arange(len(df))

    alerts = []
    for (sid, param), g in df.groupby(["station_id", "param"], sort=True):
        g = g.sort_values("event_time_ms", kind="stable").reset_index(drop=True)
        alerts.extend(detect_key(g, args.warmup))

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        for a in alerts:
            f.write(json.dumps(a, ensure_ascii=False) + "\n")

    print(f"{len(df)} sự kiện -> {len(alerts)} alert (engine=offline, detector=hst)")
    if alerts:
        per = pd.DataFrame(alerts).groupby("station_id").size().to_dict()
        print("per_station:", {k: int(v) for k, v in per.items()})


if __name__ == "__main__":
    main()
