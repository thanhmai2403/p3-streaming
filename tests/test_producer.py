"""python -m tests.test_producer : test producer không cần Kafka."""
import tempfile, types
from collections import defaultdict
import pandas as pd
from common import config, schema
from producer import producer as P

cfg = config.load()
cap = []
def fake(key, value): cap.append((key, value))

# ---- rate: 3 producer song song chia trạm, mỗi cái 3000 ev/s trong 1 s
allst = set()
for pid in range(3):
    cap.clear()
    s = P.Sender(pid, "sensor", None, produce_fn=fake)
    a = types.SimpleNamespace(n_stations=1000, producer_id=pid, n_producers=3, seed=42, rate=3000, duration=1.0)
    n = P.run_rate(a, s, cfg)
    evs = [schema.parse_event(v) for _, v in cap]
    assert all(k == e["station_id"] for (k, _), e in zip(cap, evs))
    assert all(int(e["station_id"][4:]) % 3 == pid for e in evs)
    assert [e["seq"] for e in evs] == [pid * 10**12 + i for i in range(len(evs))]
    assert all(e["event_time_ms"] == e["ingest_ts_ms"] for e in evs)
    allst |= {e["station_id"] for e in evs}
    print(f"rate pid={pid}: sent={n} (mục tiêu ~3000), trạm={len({e['station_id'] for e in evs})}")
    assert 2700 <= n <= 3300
assert len(allst) == 1000

# ---- replay: 3 trạm x 400 giờ, late 10%, lùi 1–4
rows = []
for s in range(3):
    for h in range(400):
        rows.append({"station_id": f"hn_{s+1:02d}", "param": "pm25", "value": 50.0 + h % 7, "event_time_ms": 1767600000000 + h * 3_600_000})
f = tempfile.mktemp(suffix=".parquet"); pd.DataFrame(rows).to_parquet(f)
cap.clear()
s = P.Sender(0, "sensor", None, produce_fn=fake)
a = types.SimpleNamespace(file=f, producer_id=0, n_producers=1, late_shift=[1, 4], late_fraction=0.10, seed=42, speed=50000)
n = P.run_replay(a, s, cfg)
evs = [schema.parse_event(v) for _, v in cap]
assert n == len(evs) == 1200 and len({(e["station_id"], e["event_time_ms"]) for e in evs}) == 1200
by = defaultdict(list)
for e in evs: by[e["station_id"]].append(e)
shift = defaultdict(int); dropped = 0
for st, lst in by.items():
    mx = None
    for pos, e in enumerate(lst):
        true_pos = (e["event_time_ms"] - 1767600000000) // 3_600_000
        if pos > true_pos: shift[pos - true_pos] += 1
        if mx is not None and e["event_time_ms"] < mx - 7_200_000: dropped += 1
        mx = e["event_time_ms"] if mx is None else max(mx, e["event_time_ms"])
late = sum(shift.values())
print(f"replay: sent={n}, sự kiện bị lùi={late} (~10%), phân bố độ lùi={dict(sorted(shift.items()))}, "
      f"sẽ bị bỏ ở ngưỡng 2h={dropped}")
assert 80 <= late <= 160 and max(shift) <= 4 + 2   # +2: các sự kiện giữ cùng lúc có thể đẩy nhau
# replay không late: thứ tự = thứ tự thời gian
cap.clear(); s = P.Sender(0, "sensor", None, produce_fn=fake)
a.late_fraction = 0.0; P.run_replay(a, s, cfg)
ets = [schema.parse_event(v)["event_time_ms"] for _, v in cap]
assert ets == sorted(ets)
print("PRODUCER_OK")
