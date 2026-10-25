"""SOTA (A): Matrix Profile – discord (Yeh et al., ICDM 2016) bằng STUMPY, chạy OFFLINE (README 10.3).

Với mỗi chuỗi (station_id, param), sắp theo event_time_ms:
  - điểm số = khoảng cách matrix profile của đoạn con dài m (m = 24 = 1 ngày dữ liệu giờ),
    GÁN CHO MẪU CUỐI của đoạn con (m-1 mẫu đầu không có điểm)
  - ngưỡng = phân vị 99% điểm số trên 20% dữ liệu đầu của chuỗi, KHÔNG dùng nhãn
  - alert khi điểm > ngưỡng; sau mỗi alert bỏ qua `refractory` mẫu (mặc định = m), vì một discord
    làm m đoạn con liên tiếp cùng có điểm cao -> tránh một bất thường sinh tới m alert, phần lớn rơi
    ngoài cửa sổ nhãn và kéo Precision xuống. Quy tắc này phải ghi vào báo cáo.

Ba biến thể (chọn trước khi xem kết quả, không chỉnh theo nhãn):
  mặc định        stumpy.stump, z-normalized  -> "trần chất lượng" (nhìn cả chuỗi, kể cả tương lai)
  --no-normalize  khoảng cách không chuẩn hoá -> ablation: nhạy với level shift
  --causal        stumpy.stumpi(egress=False) -> điểm của mẫu t chỉ dựa trên quá khứ (chạy được online)

Đầu vào: parquet có cột station_id, param, value, event_time_ms (file đã chèn bất thường của B).
Đầu ra:  results/raw/<run_id>/alerts.jsonl (schema README 5.2, engine="offline", detector="mp",
         seq/ingest_ts_ms/emit_ts_ms = null vì chạy offline), scores.csv, meta.json
Chấm bằng measure/measure_quality.py của B (cùng labels.csv).

  python sota/matrix_profile.py --file data/processed/openaq_hn_injected.parquet
  python sota/matrix_profile.py --file ... --no-normalize --run-id rq2_offline_mpraw_r1
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from common import config, schema  # noqa: E402

COLS = ["station_id", "param", "value", "event_time_ms"]


def mp_scores(x: np.ndarray, m: int, normalize: bool, causal: bool) -> np.ndarray:
    """Điểm cho từng mẫu (NaN nếu chưa đủ dữ liệu). Điểm của đoạn con [i, i+m) gán cho mẫu i+m-1."""
    import stumpy

    n = len(x)
    out = np.full(n, np.nan)
    if causal:
        init = 2 * m                         # cần vài đoạn con trong quá khứ để có láng giềng hợp lệ
        if n <= init:
            return out
        st = stumpy.stumpi(x[:init], m, egress=False, normalize=normalize)
        for t in range(init, n):
            st.update(x[t])
            out[t] = st.P_[-1]               # đoạn con mới nhất so với mọi đoạn con TRƯỚC nó
    else:
        prof = stumpy.stump(x, m, normalize=normalize)
        out[m - 1:] = prof[:, 0].astype(np.float64)
    out[~np.isfinite(out)] = np.nan          # inf: không có láng giềng hợp lệ (vd đoạn hằng số)
    return out


def threshold(scores: np.ndarray, calib_fraction: float, q: float) -> float:
    k = max(1, int(len(scores) * calib_fraction))
    calib = scores[:k]
    calib = calib[np.isfinite(calib)]
    if calib.size == 0:
        return float("inf")
    return float(np.quantile(calib, q))


def pick_alerts(scores: np.ndarray, thr: float, refractory: int) -> np.ndarray:
    idx, skip_until = [], -1
    for i, s in enumerate(scores):
        if i <= skip_until or not np.isfinite(s):
            continue
        if s > thr:
            idx.append(i)
            skip_until = i + refractory
    return np.array(idx, dtype=int)


def main(argv=None):
    cfg = config.load()
    sc = cfg["sota"]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", default=str(ROOT / "data/processed/openaq_hn_injected.parquet"))
    ap.add_argument("--run-id", default=None, help="mặc định rq2_offline_mp[raw|causal]_r1")
    ap.add_argument("--m", type=int, default=int(sc["mp"]["m"]))
    ap.add_argument("--normalize", action=argparse.BooleanOptionalAction, default=bool(sc["mp"]["normalize"]))
    ap.add_argument("--causal", action="store_true")
    ap.add_argument("--refractory", type=int, default=int(sc["mp"]["refractory"]))
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)

    variant = "mp" + ("" if args.normalize else "raw") + ("causal" if args.causal else "")
    run_id = args.run_id or f"rq2_offline_{variant}_r1"
    out = ROOT / "results" / "raw" / run_id
    if (out / "alerts.jsonl").exists() and not args.force:
        sys.exit(f"{out} đã có kết quả; dùng run_id mới (README 10.4) hoặc --force")
    out.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(args.file)
    miss = [c for c in COLS if c not in df.columns]
    if miss:
        sys.exit(f"{args.file} thiếu cột {miss}")
    df = df[COLS].sort_values(["station_id", "param", "event_time_ms"], kind="stable")

    t0 = time.perf_counter()
    all_scores, n_alert, per_series = [], 0, []
    with open(out / "alerts.jsonl", "w", encoding="utf-8") as fa:
        for (st, p), g in df.groupby(["station_id", "param"], sort=True):
            x = g["value"].to_numpy(dtype=np.float64)
            if len(x) < 3 * args.m:
                print(f"[mp] bỏ {st}/{p}: chỉ {len(x)} mẫu", file=sys.stderr)
                continue
            s = mp_scores(x, args.m, args.normalize, args.causal)
            thr = threshold(s, float(sc["calib_fraction"]), float(sc["threshold_quantile"]))
            idx = pick_alerts(s, thr, args.refractory)
            ets = g["event_time_ms"].to_numpy()
            for i in idx:
                ev = {"station_id": st, "param": p, "value": float(x[i]), "event_time_ms": int(ets[i])}
                fa.write(schema.dumps(schema.make_alert(ev, "offline", "mp", float(s[i]), None)) + "\n")
            n_alert += len(idx)
            flag = np.zeros(len(x), dtype=bool)
            flag[idx] = True
            all_scores.append(pd.DataFrame({"station_id": st, "param": p, "event_time_ms": ets,
                                            "value": x, "score": s, "threshold": thr, "alert": flag}))
            per_series.append({"station_id": st, "param": p, "n": int(len(x)), "threshold": thr,
                               "n_alert": int(len(idx))})
            print(f"[mp] {st}/{p}: n={len(x)} ngưỡng={thr:.3f} alert={len(idx)}", file=sys.stderr)
    el = time.perf_counter() - t0
    if all_scores:
        pd.concat(all_scores).to_csv(out / "scores.csv", index=False)
    meta = {"run_id": run_id, "method": "matrix_profile", "variant": variant, "m": args.m,
            "normalize": args.normalize, "causal": args.causal, "refractory": args.refractory,
            "calib_fraction": sc["calib_fraction"], "threshold_quantile": sc["threshold_quantile"],
            "input": args.file, "runtime_s": round(el, 2), "n_alert": n_alert, "series": per_series}
    (out / "meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[mp] {variant}: {n_alert} alert trong {el:.1f}s -> {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
