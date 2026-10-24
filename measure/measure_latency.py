"""Measure độ trễ end-to-end runtime (README mục 7): tiêu thụ topic `alerts`,
nhóm theo cửa sổ 5 giây theo recv_ts_ms, tính p50/p90/p99 + max, riêng `alert`
và `probe`.

    python measure/measure_latency.py --bootstrap-servers localhost:29092 \
        --group measure_rq2_spark_r1 --output-dir results/raw/rq2_spark_r1/measure \
        --warmup-s 30 --measure-s 180

Quy ước (README mục 7):
  - T0 = recv_ts_ms của bản ghi đầu tiên nhận được (job phải RUNNING trước khi
    producer chạy, bước 4 của scripts/run_experiment.sh).
  - Cửa sổ đo = [T0 + warmup_s*1000, T0 + (warmup_s + measure_s)*1000).
  - Độ trễ = recv_ts_ms - ingest_ts_ms của mẫu (fallback: recv - emit nếu thiếu).
  - RQ3 (--kill-offset-s 60): kill worker ở T0 + warmup + 60 giây. Thời gian phục
    hồi = cửa sổ 5s đầu tiên SAU kill có p99 <= 2 * p99 trước kill (p99 trước kill
    tính trên các cửa sổ [T0+warmup, T0+warmup+60)). Không phục hồi -> null.

Đầu ra (--output-dir):
  - latency_windows.csv : mỗi dòng một cửa sổ 5s: window_start_ms, kind, count,
                          p50_ms, p90_ms, p99_ms, max_ms.
  - summary.json        : tổng hợp trong cửa sổ đo + kết quả RQ3 (nếu có).
Dừng khi không có bản ghi mới trong --stop-after-idle-s giây (hoặc Ctrl-C).
"""
import argparse
import json
import math
import signal
import sys
import time
import uuid
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap-servers", default="localhost:29092")
    ap.add_argument("--topic", default="alerts")
    ap.add_argument("--group", default=None, help="mặc định: measure-<uuid>")
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--warmup-s", type=int, default=30)
    ap.add_argument("--measure-s", type=int, default=180)
    ap.add_argument("--window-s", type=int, default=5)
    ap.add_argument("--kill-offset-s", type=int, default=None,
                    help="RQ3: worker bị kill sau T0 + warmup + <n> giây")
    ap.add_argument("--stop-after-idle-s", type=int, default=60,
                    help="tự dừng sau N giây không có bản ghi mới")
    ap.add_argument("--max-s", type=int, default=3600)
    return ap.parse_args()


def percentile_sorted(sorted_vals, p):
    """Percentile nội suy tuyến tính (khớp numpy, method='linear')."""
    n = len(sorted_vals)
    if n == 0:
        return None
    if n == 1:
        return float(sorted_vals[0])
    rank = (p / 100.0) * (n - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return float(sorted_vals[lo])
    frac = rank - lo
    return float(sorted_vals[lo]) + frac * (sorted_vals[hi] - sorted_vals[lo])


def window_stats(lats):
    lats = sorted(lats)
    return {
        "count": len(lats),
        "p50_ms": percentile_sorted(lats, 50),
        "p90_ms": percentile_sorted(lats, 90),
        "p99_ms": percentile_sorted(lats, 99),
        "max_ms": lats[-1] if lats else None,
    }
def finalize(messages, args):
    """Từ danh sách (recv_ms, kind, lat) tính windows + summary + RQ3."""
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not messages:
        print("[measure] KHÔNG nhận được bản ghi nào.", flush=True)
        with open(out_dir / "summary.json", "w", encoding="utf-8") as f:
            json.dump({"error": "no messages"}, f, ensure_ascii=False, indent=2)
        return

    t0 = min(m[0] for m in messages)
    win_ms = args.window_s * 1000
    measure_start = t0 + args.warmup_s * 1000
    measure_end = t0 + (args.warmup_s + args.measure_s) * 1000

    buckets = {}  # (window_index, kind) -> list độ trễ
    for recv_ms, kind, lat in messages:
        idx = (recv_ms - t0) // win_ms
        buckets.setdefault((idx, kind), []).append(lat)

    rows = []
    for (idx, kind), lats in sorted(buckets.items()):
        ws = t0 + idx * win_ms
        st = window_stats(lats)
        rows.append({"window_start_ms": ws, "kind": kind, **st})

    summary = {
        "t0_ms": t0,
        "measure_start_ms": measure_start,
        "measure_end_ms": measure_end,
        "window_s": args.window_s,
        "n_messages": len(messages),
        "kinds": {},
        "rq3": None,
    }
    for kind in sorted({m[1] for m in messages}):
        in_w = [m[2] for m in messages
                if m[1] == kind and measure_start <= m[0] < measure_end]
        summary["kinds"][kind] = window_stats(in_w)

    # ---- RQ3: thời gian phục hồi (README mục 7)
    if args.kill_offset_s is not None:
        warm_start = t0 + args.warmup_s * 1000
        kill_ts = warm_start + args.kill_offset_s * 1000
        pre = [row for row in rows
               if row["kind"] == "alert"
               and warm_start <= row["window_start_ms"] < kill_ts
               and row["count"] >= 1]
        post = [row for row in rows
                if row["kind"] == "alert" and row["window_start_ms"] >= kill_ts]
        pre_p99 = max(r["p99_ms"] for r in pre) if pre else None
        recovery_s = None
        if pre_p99 is not None:
            for r in sorted(post, key=lambda r: r["window_start_ms"]):
                if r["count"] >= 1 and r["p99_ms"] <= 2.0 * pre_p99:
                    recovery_s = (r["window_start_ms"] - kill_ts) / 1000.0
                    break
        summary["rq3"] = {
            "kill_ts_ms": kill_ts,
            "pre_kill_p99_ms": pre_p99,
            "recovery_s": recovery_s,
        }

    import csv
    with open(out_dir / "latency_windows.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["window_start_ms", "kind", "count",
                                          "p50_ms", "p90_ms", "p99_ms", "max_ms"])
        w.writeheader()
        w.writerows(rows)
    with open(out_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[measure] T0={t0} n_messages={len(messages)} "
          f"cửa sổ đo [{measure_start}, {measure_end})", flush=True)
    for kind, st in summary["kinds"].items():
        print(f"[measure] {kind}: n={st['count']} p50={st['p50_ms']}ms "
              f"p90={st['p90_ms']}ms p99={st['p99_ms']}ms max={st['max_ms']}ms",
              flush=True)
    if summary["rq3"]:
        print(f"[measure] RQ3: pre_kill_p99={summary['rq3']['pre_kill_p99_ms']}ms "
              f"recovery_s={summary['rq3']['recovery_s']}", flush=True)
    print(f"[measure] ghi {out_dir}", flush=True)


def main():
    args = parse_args()
    from confluent_kafka import Consumer, KafkaError

    group = args.group or f"measure-{uuid.uuid4().hex[:8]}"
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": group,
        "auto.offset.reset": "earliest",   # mỗi lần chạy đọc từ đầu topic (đã xóa trước đó)
        "enable.auto.commit": False,       # không commit: lần chạy sau vẫn đọc từ đầu
    })
    consumer.subscribe([args.topic])

    messages = []
    last_msg_ts = time.time()
    start_ts = time.time()

    def _stop(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"[measure] group={group} topic={args.topic} "
          f"warmup={args.warmup_s}s measure={args.measure_s}s", flush=True)
    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()
            if msg is None:
                if now - last_msg_ts >= args.stop_after_idle_s:
                    print("[measure] hết bản ghi -> dừng.", flush=True)
                    break
                if now - start_ts >= args.max_s:
                    print("[measure] hết thời gian tối đa -> dừng.", flush=True)
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka error: {msg.error()}")
            last_msg_ts = now
            recv_ms = time.time_ns() // 1_000_000
            raw = msg.value()
            try:
                r = json.loads(raw.decode("utf-8"))
            except Exception:
                continue  # bỏ qua bản ghi hỏng
            if not isinstance(r, dict):
                continue
            kind = r.get("kind", "alert")
            if kind not in ("alert", "probe"):
                continue
            base = r.get("ingest_ts_ms") or r.get("emit_ts_ms")
            if base is None:
                continue
            lat = recv_ms - int(base)
            messages.append((recv_ms, kind, lat))
            if len(messages) == 1:
                # T0 = recv_ts_ms của bản ghi đầu tiên; ghi ra để run_experiment.sh
                # căn đúng thời điểm kill worker (RQ3).
                with open(Path(args.output_dir) / "t0.txt", "w", encoding="utf-8") as f:
                    f.write(str(recv_ms))
    except KeyboardInterrupt:
        print("[measure] nhận tín hiệu dừng.", flush=True)
    finally:
        consumer.close()

    finalize(messages, args)


if __name__ == "__main__":
    main()

