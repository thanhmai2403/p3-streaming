"""Spark: NHIỀU micro-batch + DỮ LIỆU TRẾ (README mục 6.3, handover_B mục 6 việc 3).

    python -m tests.test_spark_multibatch_late

Cách làm (không cần Kafka, chỉ cần Java + pyspark):
  1. Lấy data/processed/openaq_hn_injected.parquet và sinh thứ tự GỬI bằng đúng hàm của
     producer (producer.replay_order) với tham số y như rq2: 5 % sự kiện bị lùi 1..4 vị trí
     (--late-fraction 0.05 --late-shift 1 4), seed = config.yaml (producer.seed).
     Dữ liệu OpenAQ là chuỗi theo giờ, nên sự kiện bị lùi >= 3 vị trí là trễ >= 3 giờ >
     late.tolerance_ms (2 giờ) -> engine PHẢI bỏ (late_dropped > 0).
  2. Cắt thứ tự gửi đó thành N_FILE khối LIÊN TIẾP theo seq, mỗi khối ghi 1 file parquet
     (part-00000.parquet ...). Thứ tự tên file = thứ tự seq.
  3. Chạy applyInPandasWithState THẬT với maxFilesPerTrigger=1 -> mỗi micro-batch chỉ đọc
     1 file, nên state BUỘC phải được lưu xuống / đọc lại từ state store giữa các batch
     (đúng thứ mà trigger once của các test trước đây chưa kiểm tra).
  4. So với common/detectors_ref.py chạy trên ĐÚNG thứ tự gửi: tập (seq, detector) + score
     phải trùng khít, số probe phải bằng nhau, và late_dropped của Spark phải BẰNG bản
     tham chiếu (> 0).
"""
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pyarrow as pa                                  # noqa: E402
import pyarrow.parquet as pq                          # noqa: E402

from common import config                             # noqa: E402
from common.detectors_ref import run_reference        # noqa: E402
from measure.compare_outputs import compare_pair, load_alerts  # noqa: E402
from producer.producer import load_replay, replay_order        # noqa: E402
from spark_job.job import run_test_parquet_spark      # noqa: E402

INPUT = ROOT / "data" / "processed" / "openaq_hn_injected.parquet"
OUT_DIR = ROOT / "results" / "raw" / "rq0_spark_multibatch_late"
WORK = ROOT / "checkpoints" / "_multibatch_check"
N_FILE = 20                     # = số micro-batch mong đợi (maxFilesPerTrigger=1)
LATE_FRACTION = 0.05            # y như scripts/run_experiment.sh cho rq2
LATE_SHIFT = (1, 4)             # --late-shift 1 4
COLS = ["station_id", "param", "value", "event_time_ms", "ingest_ts_ms", "seq"]
SCHEMA = pa.schema([("station_id", pa.string()), ("param", pa.string()),
                    ("value", pa.float64()), ("event_time_ms", pa.int64()),
                    ("ingest_ts_ms", pa.int64()), ("seq", pa.int64())])

cfg = config.load()
SEED = int(cfg["producer"]["seed"])
TOLERANCE_MS = int(config.get("late.tolerance_ms", cfg=cfg))


# ---------------------------------------------------------------- 1) thứ tự gửi
df = load_replay(str(INPUT))                 # sắp (event_time_ms, station_id) như producer
rows = list(df.itertuples(index=False, name=None))
a, b = LATE_SHIFT
events = []
for i, row in enumerate(replay_order(rows, LATE_FRACTION, a, b, SEED)):
    events.append({"station_id": str(row[0]), "param": str(row[1]), "value": float(row[2]),
                   "event_time_ms": int(row[3]), "ingest_ts_ms": None, "seq": i})

# Bao nhiêu sự kiện thực sự đến muộn hơn ngưỡng (chỉ để in ra cho dễ đọc log).
mx, n_late = {}, 0
for e in events:
    k = (e["station_id"], e["param"])
    if k in mx and e["event_time_ms"] < mx[k] - TOLERANCE_MS:
        n_late += 1
    mx[k] = max(mx.get(k, e["event_time_ms"]), e["event_time_ms"])
print(f"[multibatch] {len(events)} sự kiện, {len(mx)} key, {n_late} sự kiện trễ > "
      f"{TOLERANCE_MS / 3.6e6:.0f} h (late_fraction={LATE_FRACTION}, shift={a}..{b}, seed={SEED})")

# ------------------------------------------------- 2) bản tham chiếu (đúng thứ tự gửi)
ref_records, late_ref = run_reference(events, engine="offline", with_probes=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
ref_path = OUT_DIR / "ref_alerts.jsonl"
with open(ref_path, "w", encoding="utf-8") as f:
    for r in ref_records:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
probe_ref = sum(1 for r in ref_records if r["kind"] == "probe")
print(f"[multibatch] tham chiếu: {len(ref_records) - probe_ref} alert + {probe_ref} probe, "
      f"late_dropped={late_ref}")

# ------------------------------------- 3) cắt thành N_FILE khối liên tiếp theo seq
if WORK.exists():
    shutil.rmtree(WORK, ignore_errors=True)
in_dir = WORK / "in"
in_dir.mkdir(parents=True)
bounds = [round(k * len(events) / N_FILE) for k in range(N_FILE + 1)]
sizes = []
for k in range(N_FILE):
    chunk = events[bounds[k]:bounds[k + 1]]
    sizes.append(len(chunk))
    tbl = pa.Table.from_pydict({c: [e[c] for e in chunk] for c in COLS}, schema=SCHEMA)
    pq.write_table(tbl, str(in_dir / f"part-{k:05d}.parquet"))   # tên file = thứ tự micro-batch
print(f"[multibatch] {N_FILE} file: seq {bounds[0]}..{bounds[-1] - 1}, "
      f"{min(sizes)}..{max(sizes)} sự kiện/file")

# ------------------------- 4) Spark thật, mỗi micro-batch đọc tối đa 1 file
out_path = OUT_DIR / "spark_alerts.jsonl"
info = run_test_parquet_spark(cfg, in_dir, out_path, max_files_per_trigger=1)
shutil.rmtree(WORK, ignore_errors=True)

# ------------------------------------------------------------- 5) so sánh
sa = load_alerts(str(ref_path))
sb = load_alerts(str(out_path))
cmp = compare_pair("offline", sa, "spark", sb)
probe_spark = sum(1 for line in out_path.read_text(encoding="utf-8").splitlines()
                  if line.strip() and json.loads(line).get("kind") == "probe")
print(f"[multibatch] spark: {cmp['n_b']} alert + {probe_spark} probe, "
      f"{info['n_batch']} micro-batch, late_dropped={info['late_dropped']}, "
      f"state_store={info['state_store']}")
print(f"[multibatch] so với tham chiếu: only_ref={cmp['n_only_a']} only_spark={cmp['n_only_b']} "
      f"score_mismatch={cmp['n_score_mismatch']} max|score diff|={cmp['max_score_diff']:.2e}")

evidence = {
    "check": "Spark nhieu micro-batch + du lieu tre (README 6.3 / handover_B muc 6 viec 3)",
    "input": str(INPUT.relative_to(ROOT)).replace("\\", "/"),
    "seed": SEED,
    "late_fraction": LATE_FRACTION,
    "late_shift": [a, b],
    "late_tolerance_ms": TOLERANCE_MS,
    "n_events": len(events),
    "n_keys": len(mx),
    "n_events_later_than_tolerance": n_late,
    "n_input_files": N_FILE,
    "events_per_file_min": min(sizes),
    "events_per_file_max": max(sizes),
    "max_files_per_trigger": 1,
    "micro_batches": info["n_batch"],
    "state_store": info["state_store"],
    "state_store_provider": info["state_store_provider"],
    "late_dropped_ref": late_ref,
    "late_dropped_spark": info["late_dropped"],
    "n_alert_ref": cmp["n_a"],
    "n_alert_spark": cmp["n_b"],
    "n_probe_ref": probe_ref,
    "n_probe_spark": probe_spark,
    "n_only_ref": cmp["n_only_a"],
    "n_only_spark": cmp["n_only_b"],
    "n_score_mismatch": cmp["n_score_mismatch"],
    "max_score_diff": cmp["max_score_diff"],
    "ok": bool(cmp["ok"] and late_ref > 0 and info["late_dropped"] == late_ref
               and probe_ref == probe_spark and info["n_batch"] >= 2
               and info["state_store"] == "rocksdb"),
}
(OUT_DIR / "check.json").write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n",
                                    encoding="utf-8")
print(f"[multibatch] -> {OUT_DIR / 'check.json'}")

assert n_late > 0, "dữ liệu sinh ra không có sự kiện trễ -> không kiểm tra được gì"
assert late_ref > 0, "bản tham chiếu không bỏ sự kiện trễ nào (kiểm tra late.tolerance_ms)"
assert info["late_dropped"] == late_ref, (f"late_dropped Spark={info['late_dropped']} "
                                          f"!= tham chiếu={late_ref}")
assert info["n_batch"] >= 2, f"chỉ có {info['n_batch']} micro-batch -> chưa kiểm tra được state"
assert cmp["ok"], cmp
assert probe_ref == probe_spark, (probe_ref, probe_spark)
assert info["state_store"] == "rocksdb", (
    f"state store = {info['state_store']} ({info['state_store_provider']}) -> phải chạy "
    "trong image p3-spark (có native lib RocksDB), xem handover_B mục 3.8")
print("SPARK_MULTIBATCH_LATE_OK")
