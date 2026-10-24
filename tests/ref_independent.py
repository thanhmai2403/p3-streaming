"""Bản tham chiếu ĐỘC LẬP để test detector.py: viết lại README 6.1–6.3 theo cách khác
(pandas rolling cho z-score, vòng lặp riêng cho EWMA). Không thay thế detectors_ref.py của B."""
import math
import pandas as pd

def run(events, W=24, T=3.0, LAM=0.3, L=3.0, TOL=7_200_000, WARM=24, PROBE=100):
    df = pd.DataFrame(events)
    alerts, probes, dropped = [], [], []
    for (st, p), g in df.groupby(["station_id", "param"], sort=False):
        g = g.sort_values("seq")
        keep, mx = [], None
        for r in g.itertuples():
            if mx is not None and r.event_time_ms < mx - TOL:
                dropped.append(r.seq); continue
            mx = r.event_time_ms if mx is None else max(mx, r.event_time_ms)
            keep.append(r)
        k = pd.DataFrame([r._asdict() for r in keep]).reset_index(drop=True)
        prev = k["value"].shift(1)
        mean = prev.rolling(W).mean()
        std = prev.rolling(W).std(ddof=0)
        for i, r in k.iterrows():
            if i >= WARM and std[i] > 1e-9 and abs(r.value - mean[i]) / std[i] > T:
                alerts.append((int(r.seq), "zscore", (r.value - mean[i]) / std[i]))
        m = v = 0.0
        for i, r in k.iterrows():
            x = r.value
            if i == 0:
                m, v = x, 0.0; continue
            if i >= WARM and v > 1e-9 and abs(x - m) > L * math.sqrt(v):
                alerts.append((int(r.seq), "ewma", (x - m) / math.sqrt(v)))
            d = x - m; m += LAM * d; v = (1 - LAM) * (v + LAM * d * d)
        probes += [int(s) for s in k["seq"] if s % PROBE == 0]
    return alerts, sorted(probes), sorted(dropped)
