"""Đo hiệu năng từ topic `alerts` (README mục 10). Dùng chung cho Flink và Spark.

Hai lệnh con:
  collect    đọc `alerts`, gắn recv_ts_ms (giờ máy lúc nhận) cho từng bản ghi, ghi vào results/raw/<run_id>/:
               latency.csv       seq, ingest_ts_ms, emit_ts_ms, recv_ts_ms     (chỉ bản ghi probe)
               alerts.jsonl      mọi bản ghi kind=alert, THÊM trường recv_ts_ms (cho detection delay của B)
               docker_stats.csv  ts_ms, container, cpu_pct, mem_mb             (nếu --docker-stats)
               meta.json         run_id, commit git, config.yaml, giờ bắt đầu/kết thúc, tham số
             rồi tự chạy summarize.
  summarize  tính metric từ file đã lưu (chạy lại được bao nhiêu lần cũng được, không cần Kafka):
               summary.json, timeseries_1s.csv, và cập nhật 1 dòng trong results/summary/<rq>.csv

Metric (README 10.2):
  - độ trễ = recv_ts_ms - ingest_ts_ms của probe; p50/p95/p99 trong cửa sổ đo
  - thông lượng = số probe nhận mỗi giây x 100, theo recv_ts_ms
  - cửa sổ đo: bỏ warmup_s đầu (tính từ sự kiện đầu tiên producer gửi), đo duration_s
  - bền vững: p99 < sustain_p99_ms VÀ trung vị 30 s cuối <= 1.5 x trung vị 30 s đầu
  - trùng/mất (RQ3): số seq probe xuất hiện > 1 lần; số seq chia hết cho 100 đã gửi mà không thấy
  - phục hồi (RQ3, cần --kill-ts-ms hoặc meta.json có kill_ts_ms): xem hàm recovery()

Ví dụ:
  python measure/measure_perf.py collect --run-id rq1_flink_rate20000_r1 --max-seconds 240 --docker-stats
  python measure/measure_perf.py summarize --run-id rq3_flink_kill_r1 --kill-ts-ms 1767600060000
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from common import config  # noqa: E402
from common.schema import SEQ_BASE, dumps, now_ms, wall_ms  # noqa: E402

RUN_RE = re.compile(r"^(?P<rq>rq\d+)_(?P<engine>[a-z]+)_(?P<cfg>.+)_r(?P<rep>\d+)$")


def run_dir(run_id: str) -> Path:
    return ROOT / "results" / "raw" / run_id


def parse_run_id(run_id: str) -> dict:
    m = RUN_RE.match(run_id)
    if not m:
        raise SystemExit(f"run_id '{run_id}' sai quy ước <rq>_<engine>_<cấu hình>_r<lần> (README 10.4)")
    d = m.groupdict()
    d["rep"] = int(d["rep"])
    return d


# ====================================================================== collect
class Recorder:
    """Nhận value (bytes) + recv_ts_ms, ghi ra file. Tách riêng để test không cần Kafka."""

    def __init__(self, out: Path):
        out.mkdir(parents=True, exist_ok=True)
        self._lat_f = open(out / "latency.csv", "w", newline="")
        self._lat = csv.writer(self._lat_f)
        self._lat.writerow(["seq", "ingest_ts_ms", "emit_ts_ms", "recv_ts_ms"])
        self._al = open(out / "alerts.jsonl", "w", encoding="utf-8")
        self.n_probe = self.n_alert = self.n_bad = 0
        self.first_recv = self.last_recv = None

    def handle(self, value: bytes, recv: int):
        try:
            rec = json.loads(value)
            kind = rec["kind"]
        except Exception:  # noqa: BLE001
            self.n_bad += 1
            return
        if self.first_recv is None:
            self.first_recv = recv
        self.last_recv = recv
        if kind == "probe":
            self._lat.writerow([rec["seq"], rec["ingest_ts_ms"], rec["emit_ts_ms"], recv])
            self.n_probe += 1
        elif kind == "alert":
            rec["recv_ts_ms"] = recv
            self._al.write(dumps(rec) + "\n")
            self.n_alert += 1
        else:
            self.n_bad += 1

    def close(self):
        self._lat_f.close()
        self._al.close()


class DockerStats(threading.Thread):
    """Mỗi ~1 s ghi CPU% và RAM của mọi container đang chạy (docker stats --no-stream)."""

    def __init__(self, path: Path):
        super().__init__(daemon=True)
        self.path = path
        self.stop_ev = threading.Event()

    @staticmethod
    def _mem_mb(s: str) -> float:
        used = s.split("/")[0].strip()
        m = re.match(r"([\d.]+)\s*([KMGT]?i?B)", used)
        if not m:
            return float("nan")
        mult = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "TiB": 2**20,
                "kB": 1 / 1024, "KB": 1 / 1024, "MB": 1, "GB": 1024}.get(m.group(2), float("nan"))
        return float(m.group(1)) * mult

    def run(self):
        with open(self.path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["ts_ms", "container", "cpu_pct", "mem_mb"])
            while not self.stop_ev.is_set():
                t = now_ms()
                try:
                    out = subprocess.run(
                        ["docker", "stats", "--no-stream", "--format",
                         "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"],
                        capture_output=True, text=True, timeout=10).stdout
                except Exception as ex:  # noqa: BLE001
                    print(f"[measure] docker stats lỗi: {ex}", file=sys.stderr)
                    return
                for line in out.splitlines():
                    parts = line.split("\t")
                    if len(parts) == 3:
                        cpu = parts[1].strip().rstrip("%")
                        w.writerow([t, parts[0], cpu, round(self._mem_mb(parts[2]), 1)])
                f.flush()
                self.stop_ev.wait(max(0.0, 1.0 - (now_ms() - t) / 1000))


def git_info() -> dict:
    def g(*a):
        try:
            return subprocess.run(["git", *a], cwd=ROOT, capture_output=True, text=True,
                                  timeout=5).stdout.strip()
        except Exception:  # noqa: BLE001
            return None
    return {"commit": g("rev-parse", "HEAD"), "dirty": bool(g("status", "--porcelain"))}


def write_meta(out: Path, update: dict):
    out.mkdir(parents=True, exist_ok=True)
    p = out / "meta.json"
    meta = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    meta.update(update)
    p.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")


def collect(args, cfg: dict):
    from confluent_kafka import Consumer, KafkaError

    info = parse_run_id(args.run_id)
    out = run_dir(args.run_id)
    if (out / "latency.csv").exists() and not args.force:
        raise SystemExit(f"{out} đã có dữ liệu. README: không ghi đè, chạy lại với số r mới "
                         f"(hoặc --force nếu chắc chắn).")
    write_meta(out, {"run_id": args.run_id, **info, "git": git_info(), "config": cfg,
                     "measure_args": vars(args), "measure_start_ms": now_ms(),
                     "measure_start_wall_ms": wall_ms(), "clock": "monotonic"})

    c = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"measure-{args.run_id}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed" if args.read_committed else "read_uncommitted",
        "fetch.wait.max.ms": 50,
    })
    c.subscribe([cfg["kafka"]["topic_out"]])
    rec = Recorder(out)
    stats = DockerStats(out / "docker_stats.csv") if args.docker_stats else None
    if stats:
        stats.start()

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    t_start = time.monotonic()
    last_msg = None
    print(f"[measure] đang đọc '{cfg['kafka']['topic_out']}' -> {out}  (Ctrl+C để dừng)", file=sys.stderr)
    try:
        while not stop["flag"]:
            if args.max_seconds and time.monotonic() - t_start > args.max_seconds:
                break
            if last_msg and args.idle_timeout and time.monotonic() - last_msg > args.idle_timeout:
                print(f"[measure] {args.idle_timeout}s không có bản ghi mới -> dừng", file=sys.stderr)
                break
            msgs = c.consume(num_messages=10000, timeout=0.5)
            if not msgs:
                continue
            recv = now_ms()
            last_msg = time.monotonic()
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[measure] lỗi consumer: {m.error()}", file=sys.stderr)
                    continue
                rec.handle(m.value(), recv)
    finally:
        rec.close()
        c.close()
        if stats:
            stats.stop_ev.set()
            stats.join(timeout=5)
        write_meta(out, {"measure_end_ms": now_ms(), "measure_end_wall_ms": wall_ms(), "n_probe": rec.n_probe,
                         "n_alert": rec.n_alert, "n_bad": rec.n_bad})
    print(f"[measure] probe={rec.n_probe} alert={rec.n_alert} hỏng={rec.n_bad}", file=sys.stderr)
    if not args.no_summary:
        summarize(args.run_id, cfg, args.kill_ts_ms)


# ====================================================================== summarize
def _pct(s, q):
    return float(s.quantile(q)) if len(s) else float("nan")


def recovery(lat, kill_ts: int, mc: dict, end_ts: int | None = None):
    """RQ3. Cửa sổ 5 s theo recv_ts_ms. Baseline = p99 của 30 s ngay trước kill.
    Cửa sổ 'xấu' = không nhận được probe nào, hoặc p99 >= 2 x baseline.
    Thời gian phục hồi = từ lúc kill tới HẾT cửa sổ xấu CUỐI CÙNG sau kill (tức là từ đó trở đi
    p99 luôn dưới ngưỡng). Không lấy 'cửa sổ tốt đầu tiên' vì ngay sau kill vẫn có thể còn probe
    của TaskManager còn sống đi qua bình thường, trước khi job bị restart."""
    win = int(mc["recovery_window_s"]) * 1000
    base = lat[(lat.recv_ts_ms >= kill_ts - int(mc["recovery_baseline_s"]) * 1000)
               & (lat.recv_ts_ms < kill_ts)]["lat"]
    if base.empty:
        return {"recovery_s": None, "note": "không có probe trước kill"}
    base_p99 = _pct(base, 0.99)
    thr = float(mc["recovery_factor"]) * base_p99
    after = lat[lat.recv_ts_ms >= kill_ts]
    if after.empty:
        return {"recovery_s": None, "baseline_p99_ms": base_p99, "recovered": False,
                "note": "không có probe sau kill"}
    # Xét tới hết lúc producer gửi xong (end_ts), không chỉ tới probe cuối cùng nhận được: nếu engine
    # KHÔNG phục hồi thì sau kill không còn probe nào, các cửa sổ trống đó phải tính là 'xấu'.
    last = max(int(after.recv_ts_ms.max()), int(end_ts or 0) - 1)
    bad_end = None
    rows = []
    for w0 in range(kill_ts, last + 1, win):
        s = after[(after.recv_ts_ms >= w0) & (after.recv_ts_ms < w0 + win)]["lat"]
        p99 = _pct(s, 0.99)
        bad = s.empty or p99 >= thr
        rows.append({"window_start_ms": w0, "n": int(len(s)), "p99_ms": p99, "bad": bad})
        if bad:
            bad_end = w0 + win
    rec_s = 0.0 if bad_end is None else (bad_end - kill_ts) / 1000
    still_bad = bool(rows and rows[-1]["bad"])
    return {"recovery_s": None if still_bad else rec_s, "baseline_p99_ms": base_p99,
            "threshold_ms": thr, "max_p99_after_ms": max((r["p99_ms"] for r in rows
                                                          if not math.isnan(r["p99_ms"])), default=None),
            "recovered": not still_bad, "windows": rows}


def dup_missing(lat, sent_files):
    """Trùng: số seq probe xuất hiện > 1 lần. Mất: seq % 100 == 0 đã GỬI mà không thấy probe."""
    counts = lat.seq.value_counts()
    dup_seq = int((counts > 1).sum())
    dup_extra = int((counts[counts > 1] - 1).sum())
    seen = set(int(s) for s in counts.index)
    sent_by_pid = {}
    for f in sent_files:
        with open(f) as fh:
            for r in csv.DictReader(fh):
                pid = int(r["producer_id"])
                sent_by_pid[pid] = sent_by_pid.get(pid, 0) + int(r["sent"])
    source = "sent.csv"
    if not sent_by_pid:                     # không có sent.csv: ước theo seq lớn nhất đã thấy
        source = "max_seq_seen"
        for s in seen:
            pid, ctr = divmod(s, SEQ_BASE)
            sent_by_pid[pid] = max(sent_by_pid.get(pid, 0), ctr + 1)
    expected = missing = 0
    for pid, n in sent_by_pid.items():
        for ctr in range(0, n, 100):
            expected += 1
            if pid * SEQ_BASE + ctr not in seen:
                missing += 1
    return {"dup_seq": dup_seq, "dup_extra": dup_extra, "expected_probes": expected,
            "missing_seq": missing, "missing_basis": source}


def clock_check(lat, sent_files, tol_ms: int = 50) -> dict:
    """Phát hiện đồng hồ bị nhảy/lệch trong lúc chạy (đã gặp trên WSL2 + Docker Desktop).
    - emit < ingest hoặc recv < emit (quá tol_ms): đồng hồ container và WSL lệch nhau
    - sent.csv có khoảng trống > 2 s giữa hai giây liên tiếp: đồng hồ WSL nhảy trong lúc gửi
    Run có clock_ok = False thì KHÔNG dùng số liệu độ trễ, phải đồng bộ giờ rồi chạy lại."""
    import pandas as pd
    neg_engine = int((lat.emit_ts_ms < lat.ingest_ts_ms - tol_ms).sum())
    neg_sink = int((lat.recv_ts_ms < lat.emit_ts_ms - tol_ms).sum())
    max_gap = 0
    for f in sent_files:
        ts = pd.read_csv(f).ts_ms.sort_values()
        if len(ts) > 1:
            max_gap = max(max_gap, int(ts.diff().max()))
    reasons = []
    if neg_engine:
        reasons.append(f"{neg_engine} probe có emit < ingest")
    if neg_sink:
        reasons.append(f"{neg_sink} probe có recv < emit")
    if max_gap > 2000:
        reasons.append(f"sent.csv nhảy {max_gap / 1000:.0f} s")
    return {"clock_ok": not reasons, "clock_problems": "; ".join(reasons),
            "sink_p50_ms": float((lat.recv_ts_ms - lat.emit_ts_ms).median())}


def summarize(run_id: str, cfg: dict, kill_ts_ms: int | None = None) -> dict:
    import pandas as pd

    mc = cfg["measure"]
    out = run_dir(run_id)
    meta = json.loads((out / "meta.json").read_text(encoding="utf-8")) if (out / "meta.json").exists() else {}
    kill_ts_ms = kill_ts_ms or meta.get("kill_ts_ms")
    lat = pd.read_csv(out / "latency.csv")
    if lat.empty:
        raise SystemExit("latency.csv rỗng: engine chưa phát probe nào")
    lat["lat"] = lat.recv_ts_ms - lat.ingest_ts_ms

    sent_files = sorted(out.glob("sent*.csv"))
    t_first = None
    if sent_files:
        t_first = min(int(pd.read_csv(f).ts_ms.min()) for f in sent_files)
    if t_first is None:
        t_first = int(lat.ingest_ts_ms.min())
    w0 = t_first + int(mc["warmup_s"]) * 1000
    w1 = w0 + int(mc["duration_s"]) * 1000
    win = lat[(lat.recv_ts_ms >= w0) & (lat.recv_ts_ms < w1)]

    # chuỗi theo giây (cả run, để vẽ hình)
    lat["sec"] = (lat.recv_ts_ms - t_first) // 1000
    ts = lat.groupby("sec").agg(n=("lat", "size"), p50_ms=("lat", "median"),
                               p99_ms=("lat", lambda s: s.quantile(0.99))).reset_index()
    ts["throughput_ev_s"] = ts.n * 100
    ts.to_csv(out / "timeseries_1s.csv", index=False)

    wsec = win.groupby((win.recv_ts_ms - w0) // 1000).size() if len(win) else pd.Series(dtype=float)
    wsec = wsec.reindex(range(int(mc["duration_s"])), fill_value=0) if len(win) else wsec
    dw = int(mc["drift_window_s"]) * 1000
    first = win[win.recv_ts_ms < w0 + dw]["lat"]
    last = win[win.recv_ts_ms >= w1 - dw]["lat"]
    med_first, med_last = (float(first.median()) if len(first) else float("nan"),
                           float(last.median()) if len(last) else float("nan"))
    p99 = _pct(win["lat"], 0.99)
    drift_ok = bool(len(first) and len(last) and med_last <= float(mc["sustain_drift_ratio"]) * med_first)

    sent_rate = None
    if sent_files:
        sent = pd.concat([pd.read_csv(f) for f in sent_files])
        sw = sent[(sent.ts_ms >= w0) & (sent.ts_ms < w1)].groupby("ts_ms").sent.sum()
        sent_rate = float(sw.mean()) if len(sw) else None

    info = parse_run_id(run_id)
    summ = {
        "run_id": run_id, **info,
        "window_ms": [w0, w1], "n_probe_window": int(len(win)),
        "lat_p50_ms": _pct(win["lat"], 0.50), "lat_p95_ms": _pct(win["lat"], 0.95),
        "lat_p99_ms": p99, "lat_max_ms": float(win["lat"].max()) if len(win) else float("nan"),
        "throughput_mean_ev_s": float(wsec.mean() * 100) if len(wsec) else 0.0,
        "throughput_median_ev_s": float(wsec.median() * 100) if len(wsec) else 0.0,
        "sent_rate_ev_s": sent_rate,
        "median_first30_ms": med_first, "median_last30_ms": med_last,
        "sustainable": bool(p99 < float(mc["sustain_p99_ms"]) and drift_ok),
        **dup_missing(lat, sent_files),
        **clock_check(lat, sent_files),
    }
    if kill_ts_ms:
        sent_end = None
        if sent_files:
            sent_end = max(int(pd.read_csv(f).ts_ms.max()) for f in sent_files) + 1000
        r = recovery(lat, int(kill_ts_ms), mc, sent_end)
        pd.DataFrame(r.pop("windows", [])).to_csv(out / "recovery_windows.csv", index=False)
        summ.update({"kill_ts_ms": int(kill_ts_ms), **r})
    (out / "summary.json").write_text(json.dumps(summ, indent=2), encoding="utf-8")

    # 1 dòng / run_id trong results/summary/<rq>.csv
    sdir = ROOT / "results" / "summary"
    sdir.mkdir(parents=True, exist_ok=True)
    sp = sdir / f"{info['rq']}.csv"
    row = {k: v for k, v in summ.items() if not isinstance(v, (list, dict))}
    df = pd.read_csv(sp) if sp.exists() else pd.DataFrame()
    if len(df) and "run_id" in df:
        df = df[df.run_id != run_id]
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True).sort_values("run_id")
    df.to_csv(sp, index=False)

    print(f"[summary] {run_id}: p50={summ['lat_p50_ms']:.0f} p95={summ['lat_p95_ms']:.0f} "
          f"p99={summ['lat_p99_ms']:.0f} ms | thông lượng={summ['throughput_mean_ev_s']:,.0f} ev/s "
          f"(gửi {sent_rate or float('nan'):,.0f}) | bền vững={summ['sustainable']} | "
          f"trùng={summ['dup_seq']} mất={summ['missing_seq']}"
          + (f" | phục hồi={summ.get('recovery_s')} s" if kill_ts_ms else ""), file=sys.stderr)
    if not summ["clock_ok"]:
        print(f"[summary] CẢNH BÁO ĐỒNG HỒ: {summ['clock_problems']} -> số liệu độ trễ của run này "
              f"KHÔNG dùng được. Đồng bộ giờ (scripts/check_clock.sh) rồi chạy lại với r mới.",
              file=sys.stderr)
    return summ


def main(argv=None):
    cfg = config.load()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("collect")
    c.add_argument("--run-id", required=True)
    c.add_argument("--bootstrap", default=cfg["kafka"]["bootstrap_host"])
    c.add_argument("--max-seconds", type=float, default=None, help="dừng sau N giây (mặc định: không giới hạn)")
    c.add_argument("--idle-timeout", type=float, default=20.0, help="dừng khi N giây không có bản ghi mới")
    c.add_argument("--read-committed", action=argparse.BooleanOptionalAction, default=True,
                   help="RQ3 EXACTLY_ONCE bắt buộc read_committed (mặc định bật)")
    c.add_argument("--docker-stats", action="store_true")
    c.add_argument("--kill-ts-ms", type=int, default=None)
    c.add_argument("--no-summary", action="store_true")
    c.add_argument("--force", action="store_true")

    s = sub.add_parser("summarize")
    s.add_argument("--run-id", required=True)
    s.add_argument("--kill-ts-ms", type=int, default=None)

    args = ap.parse_args(argv)
    if args.cmd == "collect":
        collect(args, cfg)
    else:
        summarize(args.run_id, cfg, args.kill_ts_ms)


if __name__ == "__main__":
    main()
