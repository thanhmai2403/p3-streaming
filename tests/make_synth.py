"""Sinh dữ liệu test nhỏ: nhiều trạm xen kẽ, theo giờ, có spike và sự kiện gửi trễ 1–4 vị trí cùng trạm."""
import json, random

def make(n_stations=3, n_hours=400, late_fraction=0.05, seed=7):
    rnd = random.Random(seed)
    per = {}
    for s in range(n_stations):
        x, rows = 50.0, []
        for h in range(n_hours):
            x = max(0.0, x + rnd.gauss(0, 2))
            v = x + (rnd.uniform(25, 40) if rnd.random() < 0.02 else 0.0)   # spike
            rows.append((f"hn_{s+1:02d}", "pm25", round(v, 3), 1767600000000 + h * 3_600_000))
        # gửi trễ: giữ lại rồi chèn sau k sự kiện cùng trạm
        order = list(range(n_hours))
        for i in range(n_hours - 5):
            if rnd.random() < late_fraction:
                k = rnd.randint(1, 4)
                j = order.index(i)
                order.insert(min(j + k, len(order) - 1), order.pop(j))
        per[s] = [rows[i] for i in order]
    # xen kẽ các trạm
    out, idx = [], [0] * n_stations
    while any(idx[s] < n_hours for s in range(n_stations)):
        s = rnd.randrange(n_stations)
        if idx[s] < n_hours:
            out.append(per[s][idx[s]]); idx[s] += 1
    events = []
    for seq, (st, p, v, et) in enumerate(out):
        events.append({"v": 1, "station_id": st, "param": p, "value": v, "event_time_ms": et,
                       "ingest_ts_ms": 1000 + seq, "seq": seq})
    return events
