"""python -m tests.test_measure : Recorder + summarize trên dữ liệu giả, không cần Kafka."""
import csv, json, random, shutil
from common import config, schema
from measure import measure_perf as M

cfg = config.load()
rid = "rq3_flink_testkill_r9"
out = M.run_dir(rid); shutil.rmtree(out, ignore_errors=True)
rec = M.Recorder(out)
T0, RATE, DUR = 1_767_600_000_000, 10_000, 210
KILL = T0 + 90_000
rnd = random.Random(1)
with open(out.parent / "_tmp_sent.csv", "w", newline="") as f: pass
sent_rows = []
lost = {T0 + 100_500}                              # 1 probe bị mất
for sec in range(DUR):
    sent_rows.append([T0 + sec * 1000, 0, RATE])
    for k in range(RATE // 100):
        ctr = (sec * RATE) + k * 100
        ing = T0 + sec * 1000 + k * 10
        lat = 50 + rnd.random() * 30
        if KILL <= ing < KILL + 20_000: lat += 15_000 - (ing - KILL) * 0.7     # đứng hình rồi đuổi kịp
        ev = schema.make_event("syn_0001", "pm25", 1.0, ing, ing, ctr)
        r = schema.make_probe(ev, "flink", int(ing + lat - 5))
        if ing in lost: continue
        rec.handle(schema.dumps(r).encode(), int(ing + lat))
        if KILL + 5_000 <= ing < KILL + 6_000: rec.handle(schema.dumps(r).encode(), int(ing + lat + 1))  # trùng
    if sec % 50 == 0:
        a = schema.make_alert(ev, "flink", "zscore", 3.5, ing + 5); rec.handle(schema.dumps(a).encode(), ing + 20)
rec.handle(b"garbage", T0)
rec.close()
with open(out / "sent.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["ts_ms", "producer_id", "sent"]); w.writerows(sent_rows)
(out.parent / "_tmp_sent.csv").unlink()

s = M.summarize(rid, cfg, kill_ts_ms=KILL)
al = [json.loads(l) for l in open(out / "alerts.jsonl")]
assert all("recv_ts_ms" in a for a in al) and len(al) == 5 and rec.n_bad == 1
assert abs(s["throughput_mean_ev_s"] - RATE) / RATE < 0.02, s["throughput_mean_ev_s"]
assert s["missing_seq"] == 1 and s["dup_seq"] == 100, (s["missing_seq"], s["dup_seq"])
assert 15 <= s["recovery_s"] <= 30, s["recovery_s"]
assert s["sustainable"] is False          # p99 trong cửa sổ đo bị kéo lên do kill
print({k: s[k] for k in ["lat_p50_ms", "lat_p99_ms", "throughput_mean_ev_s", "dup_seq", "missing_seq",
                         "baseline_p99_ms", "recovery_s"]})
shutil.rmtree(out); (M.ROOT / "results/summary/rq3.csv").unlink()
print("MEASURE_OK")

# ---- phát hiện lệch/nhảy đồng hồ (tình huống thật đã gặp ở rq1_flink_try5000_r3)
import pandas as pd, tempfile, pathlib
lat = pd.DataFrame({"seq": [0, 100, 200], "ingest_ts_ms": [1000, 70_000, 3000],
                    "emit_ts_ms": [1100, 3_100, 3100], "recv_ts_ms": [1200, 3_300, 3300]})
sf = pathlib.Path(tempfile.mkdtemp()) / "sent.csv"
pd.DataFrame({"ts_ms": [0, 1000, 68_000], "producer_id": 0, "sent": 1}).to_csv(sf, index=False)
c = M.clock_check(lat, [sf])
assert c["clock_ok"] is False and "emit < ingest" in c["clock_problems"] and "nhảy" in c["clock_problems"], c
ok = M.clock_check(lat.iloc[[0, 2]], [])
assert ok["clock_ok"] is True, ok
print("CLOCK_CHECK_OK")
