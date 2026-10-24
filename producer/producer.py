"""Producer gửi sự kiện vào Kafka (README mục 8).

Phần --mode replay do B viết; --mode rate do A viết (chưa có trong repo).

    python producer/producer.py --mode replay --file data/processed/openaq_hn_injected.parquet \
        --speed 200 --late-fraction 0.05 --late-shift 1 4 --producer-id 0
    # test logic giữ trễ + thứ tự seq mà không cần Kafka:
    python producer/producer.py --mode replay --file data/processed/openaq_hn_injected.parquet \
        --dry-run --output /tmp/replay.jsonl --producer-id 0
"""
import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

DEFAULT_TOPIC = "sensor"
DEFAULT_BOOTSTRAP = "localhost:29092"
# Cấu hình confluent-kafka dùng chung (README mục 8).
KAFKA_CONFIG = {
    "linger.ms": 5,
    "batch.size": 65536,
    "acks": "1",
    "compression.type": "none",
}


def station_index(station_id):
    """Chỉ số trạm dùng để phân mảnh: hn_01 -> 1, syn_0042 -> 42."""
    return int(str(station_id).split("_")[-1])


def select_stations(df, producer_id, n_producers):
    mask = df["station_id"].map(
        lambda s: station_index(s) % n_producers == producer_id)
    return df[mask]


def build_send_order(df, producer_id, n_producers, rng, late_fraction, late_shift):
    """Trả về list dict {station_id,param,value,event_time_ms} theo thứ tự GỬI.

    Giữ trễ: chọn late_fraction sự kiện (seed cố định qua rng), mỗi sự kiện bị giữ
    rồi gửi sau `shift` (random trong late_shift) sự kiện kế tiếp CÙNG TRẠM.
    """
    df = select_stations(df, producer_id, n_producers)
    df = df.sort_values(["event_time_ms", "station_id"], kind="stable").reset_index(drop=True)

    send_order = []
    pending = {}  # station_id -> list of [event_dict, remaining]
    for row in df.itertuples(index=False):
        ev = {
            "station_id": str(row.station_id),
            "param": str(row.param),
            "value": float(row.value),
            "event_time_ms": int(row.event_time_ms),
        }
        s = ev["station_id"]
        if rng.random() < late_fraction:
            shift = int(rng.integers(late_shift[0], late_shift[1] + 1))
            pending.setdefault(s, []).append([ev, shift])
        else:
            send_order.append(ev)
            q = pending.get(s)
            if q:
                for item in q:
                    item[1] -= 1
                kept = []
                for item in q:
                    if item[1] <= 0:
                        send_order.append(item[0])
                    else:
                        kept.append(item)
                pending[s] = kept

    # Xả nốt các sự kiện giữ trễ còn tồn đọng (không đủ sự kiện kế tiếp cùng trạm để nhả).
    for s in sorted(pending):
        send_order.extend(item[0] for item in pending[s])
    return send_order


def build_message(ev, seq, ingest_ms):
    return {
        "v": 1,
        "station_id": ev["station_id"],
        "param": ev["param"],
        "value": float(ev["value"]),
        "event_time_ms": int(ev["event_time_ms"]),
        "ingest_ts_ms": int(ingest_ms),
        "seq": int(seq),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["rate", "replay"], required=True)
    ap.add_argument("--file", default="data/processed/openaq_hn_injected.parquet")
    ap.add_argument("--speed", type=float, default=200, help="sự kiện/giây cho replay")
    ap.add_argument("--late-fraction", type=float, default=0.05)
    ap.add_argument("--late-shift", nargs=2, type=int, default=[1, 4])
    ap.add_argument("--producer-id", type=int, default=0)
    ap.add_argument("--n-producers", type=int, default=1)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--dry-run", action="store_true", help="ghi ra file thay vì bắn Kafka")
    ap.add_argument("--output", help="file .jsonl khi --dry-run")
    ap.add_argument("--rate", type=int, default=20000, help="[rate mode - A]")
    ap.add_argument("--duration", type=int, default=210, help="[rate mode - A]")
    args = ap.parse_args()

    if args.mode == "rate":
        raise SystemExit("--mode rate chưa cài đặt (phần của Thành viên A).")

    df = pd.read_parquet(args.file)
    rng = np.random.default_rng(args.seed)
    send_order = build_send_order(
        df, args.producer_id, args.n_producers, rng, args.late_fraction, args.late_shift)
    print(f"[replay] producer-id={args.producer_id} n-producers={args.n_producers} "
          f"-> {len(send_order)} sự kiện (late-fraction={args.late_fraction}, "
          f"late-shift={args.late_shift}, seed={args.seed})")

    if args.dry_run:
        out_path = args.output or f"replay_p{args.producer_id}.jsonl"
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            for i, ev in enumerate(send_order):
                seq = args.producer_id * 10**12 + i
                ingest_ms = time.time_ns() // 1_000_000
                f.write(json.dumps(build_message(ev, seq, ingest_ms), ensure_ascii=False) + "\n")
        print(f"[dry-run] ghi {len(send_order)} sự kiện -> {out_path}")
        return

    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": args.bootstrap_servers, **KAFKA_CONFIG})

    start = time.perf_counter()
    fell_behind = False
    for i, ev in enumerate(send_order):
        deadline = start + i / args.speed
        now = time.perf_counter()
        if deadline - now > 0.25:
            fell_behind = True
        if deadline > now:
            time.sleep(deadline - now)
        seq = args.producer_id * 10**12 + i
        ingest_ms = time.time_ns() // 1_000_000
        producer.produce(
            args.topic,
            key=ev["station_id"],
            value=json.dumps(build_message(ev, seq, ingest_ms), ensure_ascii=False),
        )
        if (i + 1) % 1000 == 0:
            producer.poll(0)
            print(f"đã gửi {i + 1}/{len(send_order)}")
    producer.flush()

    elapsed = time.perf_counter() - start
    expected = len(send_order) / args.speed if args.speed > 0 else 0.0
    print(f"đã gửi xong {len(send_order)} sự kiện trong {elapsed:.2f}s "
          f"(kỳ vọng {expected:.2f}s, speed={args.speed}/s)")
    if fell_behind and elapsed > expected * 1.1:
        print("CẢNH BÁO: không giữ nổi tốc độ yêu cầu (chậm hơn kỳ vọng).")


if __name__ == "__main__":
    main()
