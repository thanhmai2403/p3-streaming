"""Chèn bất thường vào dữ liệu OpenAQ đã làm sạch và xuất nhãn (README mục 7.2). seed = 42.

    python data/inject_anomalies.py
Đầu vào : data/processed/openaq_hn_clean.parquet
Đầu ra  : data/processed/openaq_hn_injected.parquet  (station_id, param, value, event_time_ms)
          data/processed/labels.csv                  (anomaly_id,station_id,param,type,start_ms,end_ms)
          data/processed/equiv_10k.parquet           (N sự kiện đầu theo thời gian, cho kiểm tra tương đương)
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Windows: tránh UnicodeEncodeError khi chạy từ cmd/redirect (cp1252)
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

OUT = Path("data/processed")
TYPES = ["spike", "level_shift", "drift"]


def pick_positions(rng, n, count, skip_first, min_gap, max_len):
    """Chọn (start, length, type) sao cho các bất thường cách nhau >= min_gap mẫu."""
    chosen = []
    attempts = 0
    while len(chosen) < count and attempts < 20000:
        attempts += 1
        typ = TYPES[rng.integers(len(TYPES))]
        if typ == "spike":
            length = 1
        elif typ == "level_shift":
            length = int(rng.integers(6, 25))      # 6-24 mẫu
        else:
            length = int(rng.integers(24, 49))     # 24-48 mẫu
        start = int(rng.integers(skip_first, n - length - min_gap))
        end = start + length - 1
        ok = all(start > e + min_gap or end < s - min_gap for s, e, _ in chosen)
        if ok:
            chosen.append((start, end, typ))
    return sorted(chosen)


def inject(df, seed=42, per_station=30, skip_first=48, min_gap=48):
    rng = np.random.default_rng(seed)
    parts, labels = [], []
    aid = 1
    for st in sorted(df["station_id"].unique()):
        g = df[df["station_id"] == st].sort_values("event_time_ms").reset_index(drop=True)
        x = g["value"].to_numpy(dtype="float64").copy()
        sigma = float(np.std(x))
        pos = pick_positions(rng, len(g), per_station, skip_first, min_gap, 48)
        for start, end, typ in pos:
            length = end - start + 1
            if typ == "spike":
                x[start] += rng.uniform(4, 6) * sigma
            elif typ == "level_shift":
                x[start:end + 1] += 3 * sigma
            else:  # drift: dốc tuyến tính, mẫu cuối đạt 3σ (mẫu đầu đã > 0 để có thật sự bất thường)
                x[start:end + 1] += np.linspace(0, 3 * sigma, length + 1)[1:]
            labels.append((aid, st, "pm25", typ, int(g.event_time_ms[start]), int(g.event_time_ms[end])))
            aid += 1
        g["value"] = x
        parts.append(g)
        print(f"{st}: {len(g)} mẫu, σ={sigma:.2f}, chèn {len(pos)} bất thường")
    out = pd.concat(parts).sort_values(["event_time_ms", "station_id"], kind="stable").reset_index(drop=True)
    lab = pd.DataFrame(labels, columns=["anomaly_id", "station_id", "param", "type", "start_ms", "end_ms"])
    return out, lab


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(OUT / "openaq_hn_clean.parquet"))
    ap.add_argument("--per-station", type=int, default=30)
    ap.add_argument("--equiv-n", type=int, default=10000)
    args = ap.parse_args()

    df = pd.read_parquet(args.input)
    out, lab = inject(df, per_station=args.per_station)
    out.to_parquet(OUT / "openaq_hn_injected.parquet", index=False)
    lab.to_csv(OUT / "labels.csv", index=False)
    out.head(args.equiv_n).to_parquet(OUT / "equiv_10k.parquet", index=False)
    print(f"Tổng {len(out)} sự kiện, {len(lab)} bất thường. equiv_10k có {min(len(out), args.equiv_n)} sự kiện.")
    print(lab["type"].value_counts().to_string())


if __name__ == "__main__":
    main()