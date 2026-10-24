"""So khớp output alert giữa bản tham chiếu (offline), Flink và Spark (README mục 9).

Đạt khi ba tập (seq, detector) của các bản ghi kind=="alert" trùng nhau hoàn toàn,
và score lệch nhau dưới 1e-6.

    # chạy bản tham chiếu từ parquet rồi so với 2 file alert của Flink/Spark:
    python measure/compare_outputs.py --input data/processed/equiv_10k.parquet \
        --a-file results/raw/.../alerts.jsonl --b-file results/raw/.../alerts.jsonl

    # hoặc so trực tiếp 3 file alerts.jsonl (nhãn engine do --ref/--a/--b):
    python measure/compare_outputs.py \
        --ref-file data/processed/ref_alerts.jsonl \
        --a-file data/processed/ref_alerts.jsonl \
        --b-file data/processed/ref_alerts.jsonl \
        --ref offline --a offline --b offline
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

TOL = 1e-6


def load_alerts(path):
    """Trả về dict {(seq, detector): score} chỉ lấy bản ghi kind == "alert"."""
    out = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            if r.get("kind", "alert") != "alert":
                continue
            key = (int(r["seq"]), str(r["detector"]))
            out[key] = float(r["score"])
    return out


def compare_pair(name_a, sa, name_b, sb):
    ka, kb = set(sa), set(sb)
    only_a = sorted(ka - kb)
    only_b = sorted(kb - ka)
    common = ka & kb
    bad = [(k, sa[k], sb[k]) for k in sorted(common) if abs(sa[k] - sb[k]) >= TOL]
    max_diff = max((abs(sa[k] - sb[k]) for k in common), default=0.0)
    return {
        "pair": f"{name_a} vs {name_b}",
        "n_a": len(sa),
        "n_b": len(sb),
        "only_a": only_a,
        "only_b": only_b,
        "n_only_a": len(only_a),
        "n_only_b": len(only_b),
        "n_score_mismatch": len(bad),
        "max_score_diff": max_diff,
        "ok": len(only_a) == 0 and len(only_b) == 0 and len(bad) == 0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default="offline", help="nhãn engine của bản tham chiếu")
    ap.add_argument("--a", default="flink", help="nhãn engine của engine A")
    ap.add_argument("--b", default="spark", help="nhãn engine của engine B")
    ap.add_argument("--input", help="parquet để tự chạy bản tham chiếu (khi không truyền --ref-file)")
    ap.add_argument("--ref-file", help="alerts.jsonl của bản tham chiếu")
    ap.add_argument("--a-file", help="alerts.jsonl của engine A")
    ap.add_argument("--b-file", help="alerts.jsonl của engine B")
    args = ap.parse_args()

    # Bản tham chiếu: đọc file, hoặc tự chạy detectors_ref trên parquet.
    if args.ref_file:
        ref = load_alerts(args.ref_file)
        print(f"[ref] đọc {args.ref_file} -> {len(ref)} alert")
    elif args.input:
        import pandas as pd
        from common.detectors_ref import run_reference
        df = pd.read_parquet(args.input)
        if "seq" not in df.columns:
            df = df.reset_index(drop=True)
            df["seq"] = df.index
        df = df.sort_values("seq")
        records, dropped = run_reference(df.to_dict("records"), engine=args.ref, with_probes=False)
        ref = {}
        for r in records:
            if r["kind"] == "alert":
                ref[(int(r["seq"]), str(r["detector"]))] = float(r["score"])
        print(f"[ref] chạy detectors_ref trên {len(df)} sự kiện -> {len(ref)} alert (late_dropped={dropped})")
    else:
        raise SystemExit("Cần --ref-file hoặc --input để có bản tham chiếu.")

    if not args.a_file or not args.b_file:
        raise SystemExit("Cần --a-file và --b-file (alerts.jsonl của Flink và Spark).")

    a = load_alerts(args.a_file)
    b = load_alerts(args.b_file)

    pairs = [
        compare_pair(args.ref, ref, args.a, a),
        compare_pair(args.ref, ref, args.b, b),
        compare_pair(args.a, a, args.b, b),
    ]
    all_ok = all(p["ok"] for p in pairs)

    for p in pairs:
        left, right = p["pair"].split(" vs ")
        print(f"{p['pair']}: n={p['n_a']}/{p['n_b']} "
              f"only_{left}={p['n_only_a']} only_{right}={p['n_only_b']} "
              f"score_mismatch={p['n_score_mismatch']} max_diff={p['max_score_diff']:.2e} "
              f"{'OK' if p['ok'] else 'FAIL'}")
        if not p["ok"]:
            if p["n_only_a"]:
                print(f"   chỉ có ở {left}: {p['only_a'][:10]}")
            if p["n_only_b"]:
                print(f"   chỉ có ở {right}: {p['only_b'][:10]}")

    print("=> TƯƠNG ĐƯƠNG" if all_ok else "=> KHÁC NHAU (kiểm tra lại)")
    raise SystemExit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
