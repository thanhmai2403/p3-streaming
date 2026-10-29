"""Bảng chi tiết RQ3 (chịu lỗi) cho báo cáo: tách thời gian phục hồi thành 2 phần.

Với mỗi run results/raw/rq3_<engine>_<cfg>_r<n>/ (đọc meta.json, latency.csv, sent*.csv,
flink_checkpoints.json nếu có) tính:
  outage_s      : khoảng trống DÀI NHẤT không nhận được probe nào sau kill (= "mất điện").
                  Chỉ số tuyệt đối, không phụ thuộc độ trễ nền -> so Flink/Spark trực tiếp.
  resume_s      : thời điểm (tính từ kill) nhận lại probe sau khoảng trống đó
  peak_lat_s    : độ trễ probe lớn nhất sau kill (độ sâu backlog phải xử lý bù)
  base_p99_s    : p99 của 30 s trước kill
  rec_w5_s      : thời gian phục hồi theo README 10.2 (cửa sổ 5 s, p99 < 2 x nền), = summarize
  rec_w15_s     : như trên với cửa sổ 15 s. Dùng cho EXACTLY_ONCE: sink chỉ commit theo checkpoint
                  (10 s/lần) nên probe đến theo đợt 10 s; cửa sổ 5 s xen kẽ cửa sổ rỗng và bị tính
                  "xấu" dù hệ thống đã ổn định -> rec_w5_s của EO là giả.
  catchup_s     : rec - resume = thời gian xử lý bù backlog (rec = w15 cho EO, w5 cho còn lại)
  censored      : True nếu tới hết lúc quan sát p99 vẫn chưa về ngưỡng (ghi "> X s" trong báo cáo)
  restored, ckpt_failed (Flink, từ REST), dup_seq, dup_extra, missing_seq (từ summarize)

  python scripts/rq3_report.py                 # in bảng, ghi results/summary/rq3_detail.csv
"""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from common import config  # noqa: E402
from measure.measure_perf import RUN_RE, dup_missing, recovery  # noqa: E402

RAW = ROOT / "results" / "raw"


def one(d: Path, mc: dict) -> dict:
    m = RUN_RE.match(d.name)
    eo = "eo" in m["cfg"].lower()
    row = {"run_id": d.name, "engine": m["engine"], "delivery": "EXACTLY_ONCE" if eo else
           ("AT_LEAST_ONCE" if m["engine"] == "flink" else "spark-ckpt"), "rep": int(m["rep"])}
    try:
        kill = int(json.loads((d / "meta.json").read_text(encoding="utf-8"))["kill_ts_ms"])
        lat = pd.read_csv(d / "latency.csv").sort_values("recv_ts_ms")
    except Exception as ex:  # noqa: BLE001
        row["note"] = f"HỎNG: {type(ex).__name__} {getattr(ex, 'filename', '') and Path(ex.filename).name}"
        return row
    lat["lat"] = lat.recv_ts_ms - lat.ingest_ts_ms
    sent_files = sorted(d.glob("sent*.csv"))
    end_ts = max(int(pd.read_csv(f).ts_ms.max()) for f in sent_files) + 1000 if sent_files else None

    after = lat[lat.recv_ts_ms >= kill]
    gaps = pd.concat([pd.Series([kill]), after.recv_ts_ms]).diff().dropna().reset_index(drop=True)
    i = int(gaps.values.argmax()) if len(gaps) else None
    row["outage_s"] = round(float(gaps.max()) / 1000, 1) if len(gaps) else None
    row["resume_s"] = round((int(after.recv_ts_ms.iloc[i]) - kill) / 1000, 1) if i is not None else None
    row["peak_lat_s"] = round(float(after.lat.max()) / 1000, 1) if len(after) else None

    for w in (5, 15):
        r = recovery(lat, kill, {**mc, "recovery_window_s": w}, end_ts)
        row["base_p99_s"] = round(r.get("baseline_p99_ms", float("nan")) / 1000, 2)
        row[f"rec_w{w}_s"] = r.get("recovery_s")
        row[f"ok_w{w}"] = bool(r.get("recovered"))
    w = 15 if eo else 5
    row["censored"] = not row[f"ok_w{w}"]
    rec = row[f"rec_w{w}_s"]
    row["recovery_s"] = rec
    row["catchup_s"] = round(rec - row["resume_s"], 1) if rec is not None and row["resume_s"] is not None else None
    if row["censored"] and end_ts:
        row["observed_s"] = round((max(end_ts, int(after.recv_ts_ms.max())) - kill) / 1000)

    cp = d / "flink_checkpoints.json"
    if cp.exists():
        c = json.loads(cp.read_text(encoding="utf-8")).get("counts", {})
        row["restored"], row["ckpt_failed"] = c.get("restored"), c.get("failed")
    row.update({k: v for k, v in dup_missing(lat, sent_files).items()
                if k in ("dup_seq", "dup_extra", "missing_seq")})
    return row


def main():
    mc = config.load()["measure"]
    dirs = sorted(p for p in RAW.glob("rq3_*") if p.is_dir() and RUN_RE.match(p.name))
    if not dirs:
        raise SystemExit("chưa có run rq3_* nào trong results/raw")
    df = pd.DataFrame([one(d, mc) for d in dirs])
    cols = ["run_id", "delivery", "outage_s", "resume_s", "peak_lat_s", "base_p99_s", "rec_w5_s",
            "rec_w15_s", "recovery_s", "catchup_s", "censored", "observed_s", "restored", "ckpt_failed",
            "dup_seq", "dup_extra", "missing_seq", "note"]
    df = df[[c for c in cols if c in df.columns]]
    pd.set_option("display.width", 250)
    print(df.drop(columns=[c for c in ("note",) if c in df and df[c].isna().all()]).to_string(index=False))

    ok = df[df.get("note", pd.Series(index=df.index, dtype=object)).isna()] if "note" in df else df
    print("\nTrung vị theo nhóm (run kiểm duyệt 'censored' tính bằng thời gian quan sát, tức là cận dưới):")
    ok = ok.assign(rec_lb=ok.recovery_s.fillna(ok.get("observed_s")))
    g = ok.groupby("delivery").agg(n=("run_id", "size"), outage_s=("outage_s", "median"),
                                   recovery_s=("rec_lb", "median"), catchup_s=("catchup_s", "median"),
                                   peak_lat_s=("peak_lat_s", "median"), censored=("censored", "sum"),
                                   dup_seq=("dup_seq", "median"), missing=("missing_seq", "sum"))
    print(g.to_string())
    out = ROOT / "results" / "summary" / "rq3_detail.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"\n-> {out}")


if __name__ == "__main__":
    main()
