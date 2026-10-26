"""Gộp các lần lặp của RQ2 thành 1 dòng / (engine, detector) để vẽ hình.

`scripts/run_experiment.sh` chấm chất lượng **từng lần lặp** và ghi
`results/summary/<run_id>_quality.csv`, ví dụ:

    results/summary/rq2_spark_replay_r1_quality.csv
    results/summary/rq2_spark_replay_r2_quality.csv
    results/summary/rq2_spark_replay_r3_quality.csv

`scripts/plot_results.py` (hàm `load_quality`) chỉ vẽ **một** dòng cho mỗi cặp
`(engine, detector)` và bỏ trùng theo thứ tự tên file, nên nếu không gộp thì hình
chỉ phản ánh lần lặp r1. Script này gộp trung bình các lần lặp và ghi ra
`results/summary/rq2_<engine>_<cfg>_quality.csv`; tên này sắp theo bảng chữ cái
**trước** `..._r1_quality.csv` (`q` < `r`) nên `plot_results.py` sẽ dùng bản gộp.

    python scripts/aggregate_quality.py --rq rq2 --engine spark --cfg replay --reps 3
    python scripts/aggregate_quality.py --engine flink --cfg replay --reps 3 \
        --out results/summary/rq2_flink_replay_quality.csv

Cột số (precision, recall, f1, detection_delay_ms_* …) lấy **trung bình** qua các
lần lặp; thêm `<cột>_std` (độ lệch chuẩn mẫu, ddof=1) để báo cáo sai số, thêm
`n_reps` và `runs`. Cột định danh (`engine`, `detector`) và cột boolean
(`has_recv_ts_ms`) giữ nguyên giá trị của lần đầu.
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

try:                              # Windows: tránh UnicodeEncodeError khi in tiếng Việt
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
SUMM = ROOT / "results" / "summary"


def aggregate(files: list[Path]) -> pd.DataFrame:
    """Trung bình các file chất lượng theo (engine, detector)."""
    frames = []
    for p in files:
        df = pd.read_csv(p)
        if df.empty:
            continue
        df["source"] = p.name
        frames.append(df)
    if not frames:
        raise SystemExit("Không có file chất lượng nào để gộp.")
    allr = pd.concat(frames, ignore_index=True)

    keep = [c for c in ("engine", "detector") if c in allr.columns]
    num = [c for c in allr.columns
           if c not in keep + ["source"] and pd.api.types.is_numeric_dtype(allr[c])
           and not pd.api.types.is_bool_dtype(allr[c])]
    fixed = [c for c in allr.columns if c not in num + ["source"] + keep]

    g = allr.groupby(keep, sort=True)
    out = g[num].mean()
    std = g[num].std(ddof=1)
    std.columns = [f"{c}_std" for c in std.columns]
    out = pd.concat([out, std], axis=1)
    if fixed:                                   # has_recv_ts_ms … (giá trị đầu tiên)
        out = pd.concat([out, g[fixed].first()], axis=1)
    out["n_reps"] = g.size()
    out["runs"] = g["source"].apply(lambda s: ";".join(sorted(s)))
    return out.reset_index()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rq", default="rq2", help="tiền tố run_id (mặc định rq2)")
    ap.add_argument("--engine", required=True, help="flink | spark")
    ap.add_argument("--cfg", default="replay", help="cấu hình trong run_id (mặc định replay)")
    ap.add_argument("--reps", type=int, default=None,
                    help="chỉ lấy các lần lặp r1..r<reps> (mặc định: mọi file khớp)")
    ap.add_argument("--out", default=None,
                    help="mặc định results/summary/<rq>_<engine>_<cfg>_quality.csv")
    args = ap.parse_args(argv)

    pat = f"{args.rq}_{args.engine}_{args.cfg}_r*_quality.csv"
    files = sorted(SUMM.glob(pat))
    if args.reps:
        want = {f"{args.rq}_{args.engine}_{args.cfg}_r{i}_quality.csv"
                for i in range(1, args.reps + 1)}
        files = [p for p in files if p.name in want]
    if not files:
        raise SystemExit(f"Không thấy file nào khớp results/summary/{pat}. "
                         f"Chạy run_experiment.sh trước.")
    missing = [p.name for p in files if pd.read_csv(p).empty]

    out = aggregate(files)
    dst = Path(args.out) if args.out else SUMM / f"{args.rq}_{args.engine}_{args.cfg}_quality.csv"
    if not dst.is_absolute():
        dst = ROOT / dst
    dst.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(dst, index=False)

    print(f"[agg] {len(files)} lần lặp: " + ", ".join(p.name for p in files))
    if missing:
        print(f"[agg] CẢNH BÁO: file rỗng (không chấm được gì): {', '.join(missing)}")
    cols = [c for c in ("engine", "detector", "precision", "recall", "f1",
                        "detection_delay_ms_median", "detection_delay_ms_p95", "n_reps")
            if c in out.columns]
    with pd.option_context("display.width", 200, "display.max_columns", 50):
        print(out[cols].to_string(index=False, float_format=lambda v: f"{v:.4f}"))
    print(f"[agg] -> {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
