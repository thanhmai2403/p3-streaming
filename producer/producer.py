"""Producer đẩy sự kiện vào topic `sensor` (README mục 8).

  --mode rate   (A): 1000 trạm tổng hợp syn_0000..syn_0999, random walk x_t = x_{t-1} + N(0,1), bắt đầu 50,
                     tốc độ cố định --rate sự kiện/giây trong --duration giây. event_time_ms = giờ máy lúc gửi.
  --mode replay (B): phát lại file parquet OpenAQ theo thứ tự thời gian, --speed sự kiện/giây,
                     có thể giữ lại --late-fraction sự kiện rồi gửi sau a–b sự kiện kế tiếp CÙNG trạm.
                     Chia trạm theo chỉ_số_trạm % n_producers (bản chuẩn của B) và
                     --dry-run --output ghi JSON từng sự kiện ra file để kiểm tra thứ tự/seq.

Quy ước chung:
  - key Kafka = station_id  -> mọi sự kiện của 1 trạm vào cùng partition, giữ thứ tự gửi.
  - seq = producer_id * 10^12 + bộ đếm, gán LÚC GỬI (sự kiện bị giữ lại sẽ có seq lớn hơn).
  - Nhiều producer song song: mỗi producer 1 --producer-id, chỉ gửi trạm có
    (chỉ_số_trạm % --n-producers == --producer-id). --rate là tốc độ CỦA MỖI tiến trình.
  - sent.csv (nếu có --out-dir): ts_ms, producer_id, sent  (số sự kiện đã gửi trong từng giây).

Ví dụ:
  python producer/producer.py --mode rate --rate 20000 --duration 210 --producer-id 0 --out-dir results/raw/<run_id>
  python producer/producer.py --mode replay --file data/processed/equiv_10k.parquet --speed 2000
  python producer/producer.py --mode rate --rate 200000 --duration 10 --dry-run   # đo trần của chính producer
"""
from __future__ import annotations

import argparse
import csv
import random
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common import config  # noqa: E402
from common.schema import SEQ_BASE, now_ms  # noqa: E402


# ---------------------------------------------------------------- gửi + đếm
class Sender:
    """Gán seq/ingest_ts lúc gửi, tạo JSON, gọi produce. `produce_fn=None` -> dry-run (không Kafka)."""

    def __init__(self, producer_id: int, topic: str, kafka_conf: dict | None, dry_run=False,
                 produce_fn=None):
        self.pid = producer_id
        self.counter = 0
        self.topic = topic
        self.per_sec = defaultdict(int)
        self.errors = 0
        self._p = None
        self._dump = None
        if produce_fn is not None:
            self._produce = produce_fn
        elif dry_run:
            self._produce = lambda key, value: None
        else:
            from confluent_kafka import Producer
            conf = dict(kafka_conf)
            conf.setdefault("queue.buffering.max.messages", 2_000_000)
            conf.setdefault("queue.buffering.max.kbytes", 2_097_151)
            conf["error_cb"] = self._on_error
            self._p = Producer(conf)
            self._produce = self._kafka_produce

    def _on_error(self, err):
        self.errors += 1
        print(f"[producer] Kafka error: {err}", file=sys.stderr)

    def _kafka_produce(self, key: str, value: str):
        while True:
            try:
                self._p.produce(self.topic, key=key, value=value)
                return
            except BufferError:          # hàng đợi nội bộ đầy: chờ gửi bớt rồi thử lại
                self._p.poll(0.01)

    def send(self, station_id: str, param: str, value: float, event_time_ms: int | None = None):
        t = now_ms()
        seq = self.pid * SEQ_BASE + self.counter
        self.counter += 1
        et = t if event_time_ms is None else int(event_time_ms)
        # f-string nhanh hơn json.dumps ~3 lần; repr(float) cho đúng định dạng số của JSON
        msg = (f'{{"v":1,"station_id":"{station_id}","param":"{param}","value":{float(value)!r},'
               f'"event_time_ms":{et},"ingest_ts_ms":{t},"seq":{seq}}}')
        self._produce(station_id, msg)
        if self._dump is not None:      # --dry-run --output: ghi nguyên văn JSON đã "gửi"
            self._dump.write(msg + "\n")
        self.per_sec[t // 1000 * 1000] += 1
        if self._p is not None and (self.counter & 0x3FFF) == 0:
            self._p.poll(0)

    def set_dump(self, path: str):
        """Dry-run: ghi từng sự kiện ra file .jsonl (--output)."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        self._dump = open(p, "w", encoding="utf-8")

    def close(self, timeout=60.0) -> int:
        if self._dump is not None:
            self._dump.close()
            self._dump = None
        if self._p is not None:
            return self._p.flush(timeout)
        return 0

    def write_sent_csv(self, out_dir: str | None):
        if not out_dir:
            return
        p = Path(out_dir)
        p.mkdir(parents=True, exist_ok=True)
        f = p / ("sent.csv" if self.pid == 0 else f"sent_p{self.pid}.csv")
        with open(f, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["ts_ms", "producer_id", "sent"])
            for ts in sorted(self.per_sec):
                w.writerow([ts, self.pid, self.per_sec[ts]])


class Pacer:
    """Giữ tốc độ trung bình `rate` sự kiện/giây: trả về số sự kiện được phép gửi ngay lúc này."""

    def __init__(self, rate: float, max_burst: int = 2000):
        self.rate = float(rate)
        self.t0 = time.perf_counter()
        self.done = 0
        self.max_burst = max_burst

    def allowance(self) -> int:
        due = int((time.perf_counter() - self.t0) * self.rate) - self.done
        if due <= 0:
            time.sleep(min(0.002, 1.0 / self.rate))
            return 0
        return min(due, self.max_burst)

    def sent(self, k: int):
        self.done += k


def my_indices(n_total: int, producer_id: int, n_producers: int):
    return [i for i in range(n_total) if i % n_producers == producer_id]


def station_index(station_id: str) -> int:
    """Chỉ số trạm dùng để phân mảnh: hn_01 -> 1, syn_0042 -> 42 (README mục 8)."""
    return int(str(station_id).split("_")[-1])


def select_stations(df, producer_id: int, n_producers: int):
    """Chỉ giữ trạm có (chỉ_số_trạm % n_producers == producer_id) - bản chuẩn của B."""
    mask = df["station_id"].map(
        lambda s: station_index(s) % n_producers == producer_id)
    return df[mask].reset_index(drop=True)


# ---------------------------------------------------------------- mode rate
def run_rate(args, sender: Sender, cfg: dict):
    syn = cfg["producer"]["synthetic"]
    n = int(args.n_stations or syn["n_stations"])
    idx = my_indices(n, args.producer_id, args.n_producers)
    if not idx:
        sys.exit("producer này không có trạm nào (kiểm tra --producer-id / --n-producers)")
    stations = [f"{syn['prefix']}{i:04d}" for i in idx]
    param = syn["param"]
    x = np.full(len(stations), float(syn["start_value"]))
    rng = np.random.default_rng(args.seed + args.producer_id)
    pacer = Pacer(args.rate)
    end = time.perf_counter() + args.duration
    k = 0                                   # trạm kế tiếp (round-robin)
    n_st = len(stations)
    while time.perf_counter() < end:
        a = pacer.allowance()
        if a == 0:
            continue
        steps = rng.standard_normal(a)
        for j in range(a):
            x[k] += steps[j]
            sender.send(stations[k], param, x[k])
            k += 1
            if k == n_st:
                k = 0
        pacer.sent(a)
    return pacer.done


# ---------------------------------------------------------------- mode replay
REPLAY_COLS = ["station_id", "param", "value", "event_time_ms"]


def load_replay(path: str):
    import pandas as pd
    df = pd.read_parquet(path)
    miss = [c for c in REPLAY_COLS if c not in df.columns]
    if miss:
        sys.exit(f"{path} thiếu cột {miss}; cần {REPLAY_COLS}")
    df = df[REPLAY_COLS].copy()
    if df["value"].isna().any():
        sys.exit("file replay có value null (README 5.1: phải bỏ từ bước làm sạch)")
    df["event_time_ms"] = df["event_time_ms"].astype("int64")
    return df.sort_values(["event_time_ms", "station_id"], kind="stable").reset_index(drop=True)


def replay_order(rows, late_fraction: float, shift_a: int, shift_b: int, seed: int):
    """Sinh thứ tự gửi. Mỗi sự kiện bị chọn (xác suất late_fraction) được giữ lại và gửi ngay sau
    k sự kiện KẾ TIẾP cùng trạm (k ngẫu nhiên trong [a, b]). Cuối file gửi nốt các sự kiện còn giữ."""
    rnd = random.Random(seed)
    pending = defaultdict(list)             # station -> [[còn_lại, row], ...]
    for row in rows:
        st = row[0]
        if late_fraction > 0 and rnd.random() < late_fraction:
            pending[st].append([rnd.randint(shift_a, shift_b), row])
            continue
        yield row
        lst = pending.get(st)
        if lst:
            for it in lst:
                it[0] -= 1
            ready = [it for it in lst if it[0] <= 0]
            if ready:
                pending[st] = [it for it in lst if it[0] > 0]
                for it in ready:
                    yield it[1]
    for st in list(pending):
        for it in pending[st]:
            yield it[1]


def run_replay(args, sender: Sender, cfg: dict):
    df = load_replay(args.file)
    df = select_stations(df, args.producer_id, args.n_producers)
    if df.empty:
        sys.exit(f"producer {args.producer_id} không có trạm nào "
                 f"(kiểm tra --producer-id / --n-producers)")
    rows = df.itertuples(index=False, name=None)
    a, b = args.late_shift
    pacer = Pacer(args.speed)
    it = replay_order(rows, args.late_fraction, a, b, args.seed)
    done = False
    while not done:
        allow = pacer.allowance()
        if allow == 0:
            continue
        k = 0
        for row in it:
            sender.send(row[0], row[1], row[2], row[3])
            k += 1
            if k == allow:
                break
        else:
            done = True
        pacer.sent(k)
    return pacer.done


# ---------------------------------------------------------------- main
def main(argv=None):
    cfg = config.load()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["rate", "replay"], required=True)
    ap.add_argument("--rate", type=float, help="rate: sự kiện/giây (của tiến trình này)")
    ap.add_argument("--duration", type=float, help="rate: số giây")
    ap.add_argument("--n-stations", type=int, help="rate: mặc định producer.synthetic.n_stations")
    ap.add_argument("--file", help="replay: file parquet (station_id, param, value, event_time_ms)")
    ap.add_argument("--speed", type=float, help="replay: sự kiện/giây")
    ap.add_argument("--late-fraction", type=float, default=0.0)
    ap.add_argument("--late-shift", type=int, nargs=2, default=[1, 4], metavar=("A", "B"))
    ap.add_argument("--producer-id", type=int, default=0)
    ap.add_argument("--n-producers", type=int, default=1)
    ap.add_argument("--seed", type=int, default=int(cfg["producer"]["seed"]))
    ap.add_argument("--bootstrap", "--bootstrap-servers", dest="bootstrap",
                    default=cfg["kafka"]["bootstrap_host"],
                    help="broker Kafka (tên cũ --bootstrap-servers của B vẫn dùng được)")
    ap.add_argument("--topic", default=cfg["kafka"]["topic_in"],
                    help="topic đầu vào (mặc định: config.yaml kafka.topic_in)")
    ap.add_argument("--output", help="--dry-run: ghi JSON từng sự kiện ra file .jsonl")
    ap.add_argument("--out-dir", help="ghi sent.csv vào đây, vd results/raw/<run_id>")
    ap.add_argument("--dry-run", action="store_true", help="không gửi Kafka, chỉ đo tốc độ tạo bản ghi")
    args = ap.parse_args(argv)

    if args.mode == "rate" and (args.rate is None or args.duration is None):
        ap.error("--mode rate cần --rate và --duration")
    if args.mode == "replay" and (args.file is None or args.speed is None):
        ap.error("--mode replay cần --file và --speed")
    if not 0 <= args.producer_id < args.n_producers:
        ap.error("cần 0 <= --producer-id < --n-producers")

    kconf = {"bootstrap.servers": args.bootstrap,
             **{k: str(v) for k, v in cfg["kafka"]["producer"].items()}}
    sender = Sender(args.producer_id, args.topic, kconf, dry_run=args.dry_run)
    if args.dry_run and args.output:
        sender.set_dump(args.output)

    t0 = time.perf_counter()
    n = run_rate(args, sender, cfg) if args.mode == "rate" else run_replay(args, sender, cfg)
    left = sender.close()
    el = time.perf_counter() - t0
    sender.write_sent_csv(args.out_dir)

    achieved = n / el if el > 0 else 0.0
    print(f"[producer {args.producer_id}] mode={args.mode} sent={n} in {el:.1f}s "
          f"-> {achieved:,.0f} ev/s; chưa flush={left}; lỗi Kafka={sender.errors}", file=sys.stderr)
    target = args.rate if args.mode == "rate" else args.speed
    if achieved < 0.95 * target:
        print(f"[producer {args.producer_id}] CẢNH BÁO: chỉ đạt {achieved:,.0f}/{target:,.0f} ev/s "
              f"-> chạy thêm tiến trình producer (--n-producers)", file=sys.stderr)
    if left or sender.errors:
        sys.exit(2)


if __name__ == "__main__":
    main()
