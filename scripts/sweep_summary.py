"""Tổng hợp vòng DÒ NGƯỠNG TẢI (chạy ngắn, trước RQ1) để chốt 4 mức tải.

Các run dò ngưỡng tên rq1_<engine>_sweep<R>_r<n> (DURATION_S ngắn, vd 60 s). Với run ngắn, cửa sổ đo
30 s + 180 s của summarize không áp dụng được, nên script này tự tính trên phần đang GỬI:
  - bỏ --skip-s giây đầu (mặc định 20) cho engine ổn định, xét đến hết lúc producer dừng gửi
  - sent_ev_s : tốc độ producer gửi thực tế (sent.csv)
  - out_ev_s  : số probe nhận mỗi giây x 100 (cùng khoảng thời gian, theo recv_ts_ms)
  - p50/p99   : độ trễ probe có ingest trong khoảng đó
  - trend     : trung vị độ trễ 10 s cuối / 10 s đầu của khoảng đó (> 1,5 = đang dồn việc)
  - drain_s   : từ lúc gửi xong tới khi nhận probe cuối cùng
  - theo_kip  : p99 < 1000 ms VÀ trend <= 1,5  (cùng tiêu chí "bền vững" của README 10.2)
Mọi mốc là CLOCK_MONOTONIC nên trừ nhau được trực tiếp.

  python scripts/sweep_summary.py                  # in bảng, ghi results/calibration/sweep.csv
  python scripts/sweep_summary.py --archive        # thêm: chuyển run sweep sang results/calibration/
                                                   #       và xoá dòng sweep khỏi results/summary/rq1.csv
"""
import argparse
import re
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "results" / "raw"
CAL = ROOT / "results" / "calibration"
PAT = re.compile(r"^rq1_(?P<engine>[a-z]+)_sweep(?P<rate>\d+)_r(?P<rep>\d+)$")


def one(d: Path, skip_s: float) -> dict:
    m = PAT.match(d.name)
    row = {"engine": m["engine"], "rate": int(m["rate"]), "rep": int(m["rep"]), "run_id": d.name}
    sent_files = sorted(d.glob("sent*.csv"))
    lat = pd.read_csv(d / "latency.csv") if (d / "latency.csv").exists() else pd.DataFrame()
    lat = lat.dropna(subset=[c for c in ("ingest_ts_ms", "recv_ts_ms") if c in lat.columns]) if len(lat) else lat
    if not sent_files:
        row["note"] = "HỎNG: không có sent.csv (producer không chạy?)"
        return row
    if lat.empty:
        row["note"] = "HỎNG: không nhận được probe nào (job không chạy / không ghi ra Kafka?)"
        return row
    sent = pd.concat([pd.read_csv(f) for f in sent_files])
    per_sec = sent.groupby("ts_ms").sent.sum().sort_index()
    t0, t_end = int(per_sec.index.min()), int(per_sec.index.max()) + 1000
    a = t0 + int(skip_s * 1000)
    if t_end - a < 20_000:
        row["note"] = "chạy quá ngắn"
        return row
    lat["lat"] = lat.recv_ts_ms - lat.ingest_ts_ms
    w = lat[(lat.ingest_ts_ms >= a) & (lat.ingest_ts_ms < t_end)]
    r = lat[(lat.recv_ts_ms >= a) & (lat.recv_ts_ms < t_end)]
    first = w[w.ingest_ts_ms < a + 10_000].lat.median()
    last = w[w.ingest_ts_ms >= t_end - 10_000].lat.median()
    trend = float(last / first) if first and first > 0 else float("nan")
    p99 = float(w.lat.quantile(0.99)) if len(w) else float("nan")
    row.update({
        "sent_ev_s": round(float(per_sec[(per_sec.index >= a) & (per_sec.index < t_end)].mean())),
        "out_ev_s": round(len(r) * 100 / ((t_end - a) / 1000)),
        "p50_ms": round(float(w.lat.median())) if len(w) else None,
        "p99_ms": round(p99) if len(w) else None,
        "trend": round(trend, 2),
        "drain_s": round((int(lat.recv_ts_ms.max()) - t_end) / 1000, 1),
        "neg_lat": int((lat.lat < 0).sum()),
        "theo_kip": bool(p99 < 1000 and trend <= 1.5),
    })
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-s", type=float, default=20)
    ap.add_argument("--archive", action="store_true")
    a = ap.parse_args()
    dirs = sorted(d for d in RAW.iterdir() if d.is_dir() and PAT.match(d.name)) if RAW.exists() else []
    if CAL.exists():
        dirs += sorted(d for d in CAL.iterdir() if d.is_dir() and PAT.match(d.name))
    if not dirs:
        raise SystemExit("chưa có run rq1_<engine>_sweep<R>_r<n> nào")
    df = pd.DataFrame([one(d, a.skip_s) for d in dirs]).sort_values(["engine", "rate", "rep"])
    if "note" not in df:
        df["note"] = ""
    df["note"] = df["note"].fillna("")
    print(df.drop(columns=["run_id"]).to_string(index=False))
    good = df[~df.note.str.startswith("HỎNG")]
    done = set(zip(good.engine, good.rate))
    broken = df[df.note.str.startswith("HỎNG") & ~df.apply(lambda r: (r.engine, r.rate) in done, axis=1)]
    if len(broken):
        print("\nRun hỏng (xem results/raw/<run_id>/ : measure.log, producer_*.log, submit.log):")
        for rid, note in zip(broken.run_id, broken.note):
            print(f"  {rid}: {note}")
    CAL.mkdir(parents=True, exist_ok=True)
    df.to_csv(CAL / "sweep.csv", index=False)
    for e, g in df.groupby("engine"):
        ok = g[g.get("theo_kip", pd.Series(False, index=g.index)) == True]  # noqa: E712
        print(f"{e}: mức cao nhất còn theo kịp = {int(ok.rate.max()) if len(ok) else 'không có'} ev/s")
    if a.archive and len(broken):
        print("Còn run hỏng chưa chạy lại -> KHÔNG archive. Chạy lại mức đó với số r mới "
              "(vd ... sweep2000 2) rồi chạy lại script.")
        return
    if a.archive:
        for d in dirs:
            if d.parent == RAW:
                shutil.move(str(d), str(CAL / d.name))
        sp = ROOT / "results" / "summary" / "rq1.csv"
        if sp.exists():
            s = pd.read_csv(sp)
            s = s[~s.run_id.str.contains(r"_sweep\d+_r", regex=True)]
            s.to_csv(sp, index=False)
        print(f"đã chuyển run sweep sang {CAL} và xoá dòng sweep khỏi results/summary/rq1.csv")


if __name__ == "__main__":
    main()
