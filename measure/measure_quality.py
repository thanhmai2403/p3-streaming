"""Chấm chất lượng phát hiện: Precision / Recall / F1 / detection delay (README mục 10.2).

    python measure/measure_quality.py --labels data/processed/labels.csv \
        --alerts results/raw/rq2_flink_r1/alerts.jsonl results/raw/rq2_spark_r1/alerts.jsonl \
        --sent results/raw/rq2_flink_r1/sent_events.csv --out results/summary/rq2_flink_r1.csv

Định nghĩa:
  - Cửa sổ của một nhãn: [start_ms, end_ms + 1 giờ] theo event_time_ms, cùng station_id/param.
  - Precision = số alert rơi vào một cửa sổ / tổng số alert.
  - Recall    = số bất thường có >= 1 alert trong cửa sổ / tổng số bất thường.
  - Tính riêng cho từng (engine, detector). Alert trùng (seq, detector) chỉ tính 1 lần.
  - Detection delay (giờ máy) = recv_ts_ms của alert ĐẦU TIÊN trong cửa sổ - ingest_ts_ms của mẫu
    bất thường đầu tiên. Cần: alerts.jsonl có trường recv_ts_ms, và --sent là CSV
    (station_id,event_time_ms,ingest_ts_ms) do producer ghi. Thiếu thì cột delay để trống.
  - Thêm delay_hours_event = (event_time của alert đầu tiên - start_ms) / 1 giờ, dùng được cho cả SOTA offline.
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

HOUR_MS = 3_600_000


def load_alerts(paths):
    rows = []
    for p in paths:
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    r = json.loads(line)
                    if r.get("kind", "alert") == "alert":
                        rows.append(r)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if "recv_ts_ms" not in df.columns:
        df["recv_ts_ms"] = np.nan
    return df.drop_duplicates(subset=["engine", "detector", "seq"], keep="first")


def evaluate(labels, alerts, sent=None, window_ms=HOUR_MS):
    """Trả về DataFrame, mỗi dòng một (engine, detector)."""
    sent_map = {}
    if sent is not None:
        sent_map = {(r.station_id, int(r.event_time_ms)): float(r.ingest_ts_ms) for r in sent.itertuples()}
    results = []
    for (engine, det), a in alerts.groupby(["engine", "detector"]):
        n_alert = len(a)
        in_window = np.zeros(n_alert, dtype=bool)
        a = a.reset_index(drop=True)
        detected, delays_wall, delays_event = 0, [], []
        for lab in labels.itertuples():
            m = ((a["station_id"] == lab.station_id) & (a["param"] == lab.param) &
                 (a["event_time_ms"] >= lab.start_ms) & (a["event_time_ms"] <= lab.end_ms + window_ms))
            if not m.any():
                continue
            detected += 1
            in_window |= m.to_numpy()
            w = a[m]
            delays_event.append((w["event_time_ms"].min() - lab.start_ms) / HOUR_MS)
            ing = sent_map.get((lab.station_id, int(lab.start_ms)))
            recv = w["recv_ts_ms"].min()
            if ing is not None and not np.isnan(recv):
                delays_wall.append(recv - ing)
        tp = int(in_window.sum())
        precision = tp / n_alert if n_alert else 0.0
        recall = detected / len(labels) if len(labels) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        dw = np.array(delays_wall, dtype=float)
        results.append({
            "engine": engine, "detector": det,
            "n_alerts": n_alert, "alerts_in_window": tp,
            "n_anomalies": len(labels), "detected": detected,
            "precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4),
            "delay_ms_median": float(np.median(dw)) if len(dw) else None,
            "delay_ms_mean": float(dw.mean()) if len(dw) else None,
            "delay_ms_p95": float(np.percentile(dw, 95)) if len(dw) else None,
            "delay_hours_event_median": float(np.median(delays_event)) if delays_event else None,
        })
    return pd.DataFrame(results)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", required=True)
    ap.add_argument("--alerts", nargs="+", required=True)
    ap.add_argument("--sent", default=None, help="CSV station_id,event_time_ms,ingest_ts_ms (producer ghi)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    labels = pd.read_csv(args.labels)
    alerts = load_alerts(args.alerts)
    if alerts.empty:
        raise SystemExit("Không có alert nào trong file đầu vào.")
    sent = pd.read_csv(args.sent) if args.sent else None
    res = evaluate(labels, alerts, sent)
    print(res.to_string(index=False))
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        res.to_csv(args.out, index=False)


if __name__ == "__main__":
    main()