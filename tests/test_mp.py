"""python -m tests.test_mp : chạy matrix_profile.py trên chuỗi giả (chu kỳ ngày + nhiễu + spike/level_shift/drift).
Chỉ để kiểm tra code chạy đúng và hiểu hành vi từng biến thể — KHÔNG phải kết quả thật."""
import json, shutil, tempfile
import numpy as np, pandas as pd
from sota import matrix_profile as MP
from measure.measure_perf import ROOT

rng = np.random.default_rng(42)
H, rows, labels = 24 * 120, [], []
for s in ("hn_01", "hn_02"):
    t = np.arange(H)
    x = 60 + 20 * np.sin(2 * np.pi * t / 24) + rng.normal(0, 4, H)
    sigma = x.std()
    pos = 24 * 30
    while pos < H - 60:
        typ = rng.choice(["spike", "level_shift", "drift"])
        L = 1 if typ == "spike" else int(rng.integers(6, 25)) if typ == "level_shift" else int(rng.integers(24, 49))
        if typ == "spike": x[pos] += rng.uniform(4, 6) * sigma
        elif typ == "level_shift": x[pos:pos + L] += 3 * sigma
        else: x[pos:pos + L] += np.linspace(0, 3 * sigma, L)
        labels.append((s, typ, pos, pos + L - 1)); pos += L + int(rng.integers(48, 120))
    et = 1767600000000 + t * 3_600_000
    rows.append(pd.DataFrame({"station_id": s, "param": "pm25", "value": x, "event_time_ms": et}))
f = tempfile.mktemp(suffix=".parquet"); pd.concat(rows).to_parquet(f)

for flags, rid in [([], "rq2_offline_mptest_r9"), (["--no-normalize"], "rq2_offline_mprawtest_r9"),
                   (["--causal"], "rq2_offline_mpcausaltest_r9")]:
    MP.main(["--file", f, "--run-id", rid, "--force", *flags])
    out = ROOT / "results/raw" / rid
    al = [json.loads(l) for l in open(out / "alerts.jsonl")]
    meta = json.load(open(out / "meta.json"))
    hit = {}
    for s, typ, a, b in labels:
        a_ms, b_ms = 1767600000000 + a * 3_600_000, 1767600000000 + (b + 1) * 3_600_000
        ok = any(r["station_id"] == s and a_ms <= r["event_time_ms"] <= b_ms for r in al)
        hit.setdefault(typ, []).append(ok)
    tp = sum(any(r["station_id"] == s and 1767600000000 + a * 3_600_000 <= r["event_time_ms"] <= 1767600000000 + (b + 1) * 3_600_000
                 for s, typ, a, b in labels) for r in al)
    print(f"{rid}: alerts={len(al)} precision~{tp/len(al):.2f} recall theo loại="
          + ", ".join(f"{k} {sum(v)}/{len(v)}" for k, v in sorted(hit.items())) + f", {meta['runtime_s']}s")
    assert all(r["engine"] == "offline" and r["detector"] == "mp" and r["seq"] is None for r in al)
    shutil.rmtree(out)
print("MP_OK")
