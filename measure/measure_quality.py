"""Chấm chất lượng phát hiện: Precision / Recall / F1 / detection delay (README mục 10.2).

    # engine streaming (measure_perf.py đã gắn recv_ts_ms) -> có cả P/R/F1 và detection delay
    python measure/measure_quality.py --labels data/processed/labels.csv \
        --alerts results/raw/rq2_flink_replay_r1/alerts.jsonl \
                 results/raw/rq2_spark_replay_r1/alerts.jsonl \
        --out results/summary/rq2_replay_quality.csv

    # SOTA offline (seq/ingest_ts_ms/emit_ts_ms = null, KHÔNG có recv_ts_ms) -> chỉ P/R/F1
    python measure/measure_quality.py --labels data/processed/labels.csv \
        --alerts results/raw/rq2_offline_mp_r1/alerts.jsonl results/sota_hst/alerts.jsonl \
        --out results/summary/rq2_offline_quality.csv

Định nghĩa (README 10.2):
  - Cửa sổ của một nhãn: [start_ms, end_ms + 1 giờ] theo event_time_ms, cùng station_id/param.
  - Precision = số alert rơi vào một cửa sổ nhãn / tổng số alert.
  - Recall    = số bất thường có >= 1 alert trong cửa sổ / tổng số bất thường.
  - F1        = trung bình điều hoà của hai số trên.
  - Tính riêng cho từng cặp (engine, detector); alert trùng chỉ tính 1 lần.
  - detection_delay (giờ máy) = recv_ts_ms - ingest_ts_ms của alert SỚM NHẤT đo được trong cửa sổ.
      * recv_ts_ms do measure_perf.py gắn lúc nhận bản ghi từ Kafka.
      * ingest_ts_ms đã có trong chính bản ghi alert (engine copy từ sự kiện gốc) nên KHÔNG cần
        file sent.csv; --sent chỉ là nguồn dự phòng.
  - detection_delay_hours_event = (event_time_ms của alert đầu tiên - start_ms) / 1 giờ: dùng được
    cho cả SOTA offline vì chỉ cần event-time.

Chịu được đầu vào "thiếu": seq / ingest_ts_ms / emit_ts_ms = null và không có recv_ts_ms (output của
matrix_profile.py và half_space_trees.py). Khi đó các cột detection_delay_* = NaN (CSV để trống),
P/R/F1 vẫn tính bình thường — không bao giờ crash.
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HOUR_MS = 3_600_000

# Cột của bảng kết quả (giữ cố định thứ tự để CSV ổn định giữa các lần chạy).
RESULT_COLS = [
    "engine", "detector",
    "n_alerts", "alerts_in_window", "n_anomalies", "detected",
    "precision", "recall", "f1",
    "detection_delay_ms_median", "detection_delay_ms_mean", "detection_delay_ms_p95",
    "detection_delay_n", "detection_delay_hours_event_median",
    "has_recv_ts_ms",
]

# Trường bắt buộc của một alert; thiếu thì bỏ bản ghi đó (đếm lại và báo ra stderr).
ALERT_KEYS = ("station_id", "param", "event_time_ms")


def _num(series) -> np.ndarray:
    """Ép về float, giá trị null/không đọc được thành NaN (không raise)."""
    return pd.to_numeric(series, errors="coerce").to_numpy(dtype="float64")


def load_alerts(paths) -> pd.DataFrame:
    """Đọc nhiều alerts.jsonl -> DataFrame đã chuẩn hoá, đã bỏ bản ghi trùng.

    Chấp nhận: kind vắng mặt (coi là "alert"), seq/ingest_ts_ms/emit_ts_ms = null,
    không có trường recv_ts_ms (SOTA offline).
    """
    rows, n_bad = [], 0
    for p in paths:
        n_file = 0
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    n_bad += 1
                    continue
                if r.get("kind", "alert") != "alert":
                    continue
                if any(r.get(k) is None for k in ALERT_KEYS):
                    n_bad += 1
                    continue
                n_file += 1
                rows.append(r)
        if n_file == 0:
            print(f"[quality] {p}: không có bản ghi kind=alert nào", file=sys.stderr)
    if n_bad:
        print(f"[quality] bỏ {n_bad} dòng không đọc được / thiếu station_id|param|event_time_ms",
              file=sys.stderr)
    if not rows:
        return pd.DataFrame(columns=list(ALERT_KEYS) + ["engine", "detector", "value", "score",
                                                        "seq", "ingest_ts_ms", "emit_ts_ms",
                                                        "recv_ts_ms"])

    df = pd.DataFrame(rows)
    for c in ("value", "score", "seq", "ingest_ts_ms", "emit_ts_ms", "recv_ts_ms"):
        if c not in df.columns:            # recv_ts_ms: chỉ có khi đi qua measure_perf.py
            df[c] = np.nan
    for c in ("engine", "detector"):
        if c not in df.columns:
            df[c] = "?"
        df[c] = df[c].astype("string").fillna("?").astype(str).str.strip()
    df["station_id"] = df["station_id"].astype(str).str.strip()
    df["param"] = df["param"].astype(str).str.strip()

    # Bỏ bản ghi trùng: khoá KHÔNG phụ thuộc seq (SOTA để seq = null) nên dùng
    # (engine, detector, trạm, param, event_time) + score làm tròn 9 chữ số.
    # Bản ghi bị Kafka gửi lại nhiều lần giống nhau tuyệt đối -> gộp đúng;
    # hai alert khác nhau thật (score khác) vẫn được giữ.
    df["_score_key"] = pd.to_numeric(df["score"], errors="coerce").round(9)
    n0 = len(df)
    df = df.drop_duplicates(
        subset=["engine", "detector", "station_id", "param", "event_time_ms", "_score_key"],
        keep="first").drop(columns="_score_key")
    if len(df) < n0:
        print(f"[quality] gộp {n0 - len(df)} alert trùng "
              f"(cùng engine/detector/trạm/thời điểm/score)", file=sys.stderr)
    return df.reset_index(drop=True)



def load_labels(path) -> pd.DataFrame:
    """Đọc labels.csv (anomaly_id, station_id, param, type, start_ms, end_ms)."""
    lab = pd.read_csv(path)
    need = ["station_id", "param", "start_ms", "end_ms"]
    miss = [c for c in need if c not in lab.columns]
    if miss:
        raise SystemExit(f"{path} thiếu cột {miss} (cần {need})")
    lab["station_id"] = lab["station_id"].astype(str).str.strip()
    lab["param"] = lab["param"].astype(str).str.strip()
    lab["start_ms"] = pd.to_numeric(lab["start_ms"], errors="coerce")
    lab["end_ms"] = pd.to_numeric(lab["end_ms"], errors="coerce")
    n0 = len(lab)
    lab = lab.dropna(subset=["start_ms", "end_ms"]).reset_index(drop=True)
    if len(lab) < n0:
        print(f"[quality] bỏ {n0 - len(lab)} nhãn thiếu start_ms/end_ms", file=sys.stderr)
    if lab.empty:
        raise SystemExit(f"{path} không có nhãn dùng được")
    return lab


def load_sent(path) -> dict:
    """(tuỳ chọn) CSV station_id,event_time_ms,ingest_ts_ms -> {(trạm, event_time): ingest_ts_ms}.

    Chỉ là nguồn dự phòng khi bản ghi alert không có ingest_ts_ms. sent.csv của producer
    (ts_ms, producer_id, sent) là thống kê theo giây, KHÔNG khớp định dạng này -> bỏ qua.
    """
    df = pd.read_csv(path)
    need = {"station_id", "event_time_ms", "ingest_ts_ms"}
    if not need.issubset(df.columns):
        print(f"[quality] --sent {path}: thiếu cột {sorted(need - set(df.columns))} -> bỏ qua, "
              f"dùng ingest_ts_ms trong alerts.jsonl", file=sys.stderr)
        return {}
    return {(str(r.station_id).strip(), int(r.event_time_ms)): float(r.ingest_ts_ms)
            for r in df.itertuples() if pd.notna(r.event_time_ms) and pd.notna(r.ingest_ts_ms)}


def evaluate(labels: pd.DataFrame, alerts: pd.DataFrame, sent=None,
             window_ms: int = HOUR_MS) -> pd.DataFrame:
    """Trả về DataFrame, mỗi dòng một (engine, detector). Không có dữ liệu -> bảng rỗng đúng cột."""
    sent_map = load_sent(sent) if isinstance(sent, (str, Path)) else (sent or {})
    if alerts is None or alerts.empty:
        return pd.DataFrame(columns=RESULT_COLS)

    lab_st = labels["station_id"].to_numpy()
    lab_pr = labels["param"].to_numpy()
    lab_s = labels["start_ms"].to_numpy(dtype="float64")
    lab_e = labels["end_ms"].to_numpy(dtype="float64")
    n_anom = len(labels)

    rows = []
    for (engine, det), a in alerts.groupby(["engine", "detector"], sort=True):
        a = a.reset_index(drop=True)
        n_alert = len(a)
        a_st = a["station_id"].to_numpy()
        a_pr = a["param"].to_numpy()
        a_et = _num(a["event_time_ms"])
        a_recv = _num(a["recv_ts_ms"])
        a_ing = _num(a["ingest_ts_ms"])
        has_recv = bool(np.isfinite(a_recv).any())

        in_window = np.zeros(n_alert, dtype=bool)
        detected = 0
        delays_ev, delays_wall = [], []
        for st, pr, s_ms, e_ms in zip(lab_st, lab_pr, lab_s, lab_e):
            m = (a_st == st) & (a_pr == pr) & (a_et >= s_ms) & (a_et <= e_ms + window_ms)
            if not m.any():
                continue
            detected += 1
            in_window |= m
            delays_ev.append(float(a_et[m].min() - s_ms) / HOUR_MS)      # theo event-time
            # giờ máy: recv_ts_ms - ingest_ts_ms của alert sớm nhất đo được trong cửa sổ
            d = a_recv[m] - a_ing[m]
            d = d[np.isfinite(d)]
            if d.size:
                delays_wall.append(float(d.min()))
            else:                                                        # dự phòng: --sent
                ing0 = sent_map.get((st, int(s_ms)))
                recv_in = a_recv[m]
                if ing0 is not None and np.isfinite(recv_in).any():
                    delays_wall.append(float(np.nanmin(recv_in) - ing0))

        tp = int(in_window.sum())
        precision = tp / n_alert if n_alert else 0.0
        recall = detected / n_anom if n_anom else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        dw = np.array(delays_wall, dtype="float64")
        rows.append({
            "engine": engine, "detector": det,
            "n_alerts": n_alert, "alerts_in_window": tp,
            "n_anomalies": n_anom, "detected": detected,
            "precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4),
            "detection_delay_ms_median": float(np.median(dw)) if dw.size else np.nan,
            "detection_delay_ms_mean": float(dw.mean()) if dw.size else np.nan,
            "detection_delay_ms_p95": float(np.percentile(dw, 95)) if dw.size else np.nan,
            "detection_delay_n": int(dw.size),
            "detection_delay_hours_event_median": (float(np.median(delays_ev))
                                                   if delays_ev else np.nan),
            "has_recv_ts_ms": has_recv,
        })
    out = pd.DataFrame(rows)
    return out[RESULT_COLS] if len(out) else pd.DataFrame(columns=RESULT_COLS)



def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--labels", required=True, help="data/processed/labels.csv")
    ap.add_argument("--alerts", nargs="+", required=True, help="một hoặc nhiều alerts.jsonl")
    ap.add_argument("--sent", default=None,
                    help="(tuỳ chọn) CSV station_id,event_time_ms,ingest_ts_ms — chỉ là dự phòng")
    ap.add_argument("--window-hours", type=float, default=1.0,
                    help="cửa sổ sau end_ms, mặc định 1 giờ (README 10.2)")
    ap.add_argument("--out", default=None, help="ghi CSV kết quả (vd results/summary/rq2_quality.csv)")
    args = ap.parse_args(argv)

    labels = load_labels(args.labels)
    alerts = load_alerts(args.alerts)
    if alerts.empty:
        raise SystemExit("Không có alert nào trong file đầu vào.")
    res = evaluate(labels, alerts, args.sent, window_ms=int(round(args.window_hours * HOUR_MS)))
    with pd.option_context("display.width", 200, "display.max_columns", None):
        print(res.to_string(index=False))
    if res["detection_delay_n"].sum() == 0:
        print("[quality] không có recv_ts_ms (hoặc thiếu ingest_ts_ms) -> detection_delay = NaN, "
              "chỉ chấm P/R/F1 (đúng cho SOTA offline)", file=sys.stderr)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        res.to_csv(args.out, index=False)
        print(f"[quality] -> {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
