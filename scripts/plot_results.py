"""Vẽ hình cho báo cáo IEEE (README mục 10.5).

    python scripts/plot_results.py                     # vẽ mọi hình có dữ liệu -> results/plots/
    python scripts/plot_results.py --fig quality       # chỉ 1 hình: quality | rq1 | rq3
    python scripts/plot_results.py --check             # kiểm tra code vẽ bằng dữ liệu giả, không ghi đè
    python scripts/plot_results.py --demo              # vẽ bằng dữ liệu giả -> results/plots/demo/

Ba hình:
  1. rq2_quality_comparison  <- results/summary/rq2_offline_quality.csv (+ rq2*.csv nếu có P/R/F1)
  2. rq1_throughput_latency  <- results/summary/rq1.csv   (thông lượng vs p99, sai số = min-max)
  3. rq3_fault_recovery      <- results/raw/rq3_*/latency.csv + meta.json (kill_ts_ms)

Mỗi hình xuất 2 file: <tên>.png (300 DPI) và <tên>.pdf (vector, font nhúng dạng TrueType 42).
Thiếu dữ liệu thì bỏ qua hình đó và in lý do (exit 0); lỗi vẽ thật thì exit 1.
"""
import argparse
import json
import sys
import tempfile
from pathlib import Path

import matplotlib

matplotlib.use("Agg")            # không cần màn hình (chạy trên server/WSL)
import matplotlib.pyplot as plt   # noqa: E402
import numpy as np                # noqa: E402
import pandas as pd               # noqa: E402

try:                              # Windows: tránh UnicodeEncodeError khi in tiếng Việt
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "results" / "plots"
DPI = 300
METHODS = ["zscore", "ewma", "hst", "mp"]          # thứ tự cột trong hình RQ2
METRICS = [("precision", "Precision"), ("recall", "Recall"), ("f1", "F1")]
COLOR_METRIC = {"precision": "#4C72B0", "recall": "#55A868", "f1": "#C44E52"}
COLOR_ENGINE = {"flink": "#1f77b4", "spark": "#d62728"}


# ====================================================================== tiện ích
def style_ieee():
    """Kiểu hình gọn cho bài IEEE 2 cột: font 8 pt, bỏ khung trên/phải, lưới mờ."""
    plt.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 8,
        "axes.labelsize": 8, "axes.titlesize": 9, "axes.linewidth": 0.6,
        "legend.fontsize": 7, "xtick.labelsize": 7, "ytick.labelsize": 7,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.alpha": 0.25, "grid.linewidth": 0.5,
        "axes.axisbelow": True, "savefig.dpi": DPI, "figure.dpi": 120,
        "pdf.fonttype": 42, "ps.fonttype": 42,
    })


def save(fig, outdir: Path, name: str, written: list) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        p = outdir / f"{name}.{ext}"
        fig.savefig(p, bbox_inches="tight", dpi=DPI)
        written.append(p)
    plt.close(fig)


def read_yaml_defaults() -> dict:
    """Đọc ngưỡng cần vẽ từ config.yaml (nguồn duy nhất của tham số, README mục 4)."""
    cfg = {}
    try:
        import yaml
        cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8")) or {}
    except Exception as exc:                      # thiếu pyyaml / thiếu file: dùng mặc định
        print(f"[plot] không đọc được config.yaml ({exc}); dùng ngưỡng mặc định", file=sys.stderr)
    m = cfg.get("measure", {}) or {}
    return {"sustain_p99_ms": float(m.get("sustain_p99_ms", 1000.0)),
            "recovery_window_s": float(m.get("recovery_window_s", 5.0)),
            "recovery_factor": float(m.get("recovery_factor", 2.0))}


def _is_false(v) -> bool:
    """CSV ghi True/False; ô trống thành NaN -> chỉ coi đúng 'False' là False."""
    return str(v).strip().lower() == "false"


def _finite(*vals) -> bool:
    return all(v is not None and np.isfinite(float(v)) for v in vals)


# ================================================================== RQ2: chất lượng
def load_quality(summary_dir: Path):
    """Gộp mọi file chất lượng trong results/summary/ thành 1 bảng.

    Nhận cả bản SOTA offline (rq2_offline_quality.csv) và bản streaming do
    run_experiment.sh sinh ra (results/summary/rq2_<engine>_<cfg>_r<n>_quality.csv).
    File rq2.csv chỉ được dùng nếu thật sự có cột precision/recall/f1.
    """
    need = {"detector", "precision", "recall", "f1"}
    frames = []
    for p in sorted(summary_dir.glob("rq2*quality*.csv")) + [summary_dir / "rq2.csv"]:
        if not p.exists():
            continue
        df = pd.read_csv(p)
        if not need <= set(df.columns) or df.empty:
            continue
        df = df[list(need | ({"engine"} & set(df.columns)))].copy()
        if "engine" not in df.columns:
            df["engine"] = "offline"
        df["source"] = p.name
        frames.append(df)
    if not frames:
        return None
    out = pd.concat(frames, ignore_index=True)
    out["engine"] = out["engine"].fillna("offline").astype(str)
    out["detector"] = out["detector"].astype(str)
    for k, _ in METRICS:
        out[k] = pd.to_numeric(out[k], errors="coerce")
    return out.drop_duplicates(subset=["engine", "detector"], keep="first")


def quality_rows(df) -> list:
    """[(nhãn, precision, recall, f1)]: 4 phương pháp trước (ưu tiên offline), rồi engine streaming."""
    rows, used = [], set()
    off = df[df.engine.str.lower() == "offline"]
    for m in METHODS:
        sel = off[off.detector == m]
        if len(sel):
            r = sel.iloc[0]
            rows.append((m, r.precision, r.recall, r.f1))
            used.add(("offline", m))
    for eng in sorted(set(df.engine.str.lower()) - {"offline"}):
        for m in METHODS:
            sel = df[(df.engine.str.lower() == eng) & (df.detector == m)]
            if len(sel):
                r = sel.iloc[0]
                rows.append((f"{eng}/{m}", r.precision, r.recall, r.f1))
                used.add((eng, m))
    for _, r in df.iterrows():                     # detector lạ (vd hst2, mp_causal): vẫn vẽ
        key = (r.engine.lower(), r.detector)
        if key not in used:
            label = r.detector if key[0] == "offline" else f"{key[0]}/{r.detector}"
            rows.append((label, r.precision, r.recall, r.f1))
    return rows


def plot_quality(rows: list, outdir: Path, written: list) -> list:
    """Cột nhóm: mỗi phương pháp 3 cột Precision / Recall / F1."""
    if not rows:
        return written
    labels = [r[0] for r in rows]
    x = np.arange(len(rows))
    w = 0.26
    fig, ax = plt.subplots(figsize=(3.5, 2.6))
    for i, (key, nice) in enumerate(METRICS):
        vals = [r[i + 1] for r in rows]
        bars = ax.bar(x + (i - 1) * w, vals, w, label=nice,
                      color=COLOR_METRIC[key], edgecolor="black", linewidth=0.3)
        for b, v in zip(bars, vals):
            if _finite(v):
                ax.annotate(f"{float(v):.3f}", (b.get_x() + b.get_width() / 2, float(v)),
                            ha="center", va="bottom", fontsize=5.5, rotation=90,
                            xytext=(0, 1), textcoords="offset points")
    ax.set_xticks(x)
    ax.set_xticklabels([str(l).replace("/", "/\n") for l in labels])
    ax.set_ylim(0, 1.0)
    ax.set_ylabel("Score")
    ax.set_title("Detection quality (RQ2): window [start, end + 1 h]")
    ax.legend(ncol=3, loc="upper center", frameon=False, bbox_to_anchor=(0.5, -0.22))
    save(fig, outdir, "rq2_quality_comparison", written)
    return written


# ======================================================= RQ1: thông lượng vs độ trễ
def load_rq1(summary_dir: Path):
    p = summary_dir / "rq1.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    need = {"engine", "cfg", "throughput_mean_ev_s", "lat_p99_ms"}
    if not need <= set(df.columns) or df.empty:
        return None
    for c in ("throughput_mean_ev_s", "lat_p99_ms", "lat_p50_ms"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def rq1_points(df) -> list:
    """[(engine, cfg, thr, thr_min, thr_max, p99, p99_min, p99_max, n, clock_ok)]."""
    pts = []
    for (eng, cfg), g in df.groupby(["engine", "cfg"], dropna=False):
        g = g.dropna(subset=["throughput_mean_ev_s", "lat_p99_ms"])
        if g.empty:
            continue
        thr = g.throughput_mean_ev_s.to_numpy(dtype=float)
        p99 = g.lat_p99_ms.to_numpy(dtype=float)
        bad = g.clock_ok.map(_is_false).any() if "clock_ok" in g.columns else False
        pts.append({"engine": str(eng), "cfg": str(cfg),
                    "thr": float(thr.mean()), "thr_min": float(thr.min()),
                    "thr_max": float(thr.max()),
                    "p99": float(p99.mean()), "p99_min": float(p99.min()),
                    "p99_max": float(p99.max()),
                    "n": int(len(g)), "clock_ok": not bad})
    return pts


def plot_rq1(points: list, outdir: Path, written: list, sustain_ms: float = 1000.0) -> list:
    """Đường thông lượng (x) vs p99 (y) cho từng engine; thanh sai số = min–max qua các lần lặp."""
    if not points:
        return written
    fig, ax = plt.subplots(figsize=(3.5, 2.8))
    for eng in sorted({p["engine"] for p in points}):
        sel = sorted([p for p in points if p["engine"] == eng], key=lambda p: p["thr"])
        x = np.array([p["thr"] for p in sel])
        y = np.array([p["p99"] for p in sel])
        xerr = np.vstack([x - np.array([p["thr_min"] for p in sel]),
                          np.array([p["thr_max"] for p in sel]) - x])
        yerr = np.vstack([y - np.array([p["p99_min"] for p in sel]),
                          np.array([p["p99_max"] for p in sel]) - y])
        color = COLOR_ENGINE.get(eng, "#7f7f7f")
        n = sum(p["n"] for p in sel)
        ax.errorbar(x, y, xerr=xerr, yerr=yerr, fmt="o-", ms=4, lw=1.0, capsize=2,
                    elinewidth=0.7, color=color, label=f"{eng} (n={n} runs)")
        for p in sel:                              # nhãn cấu hình cạnh mỗi điểm
            ax.annotate(p["cfg"], (p["thr"], p["p99"]), fontsize=5.5, color=color,
                        xytext=(3, 3), textcoords="offset points")
    ax.axhline(sustain_ms, ls="--", lw=0.8, color="#555555")
    ax.annotate(f"p99 = {sustain_ms:g} ms", (0.99, sustain_ms),
                xycoords=("axes fraction", "data"), ha="right", va="bottom",
                fontsize=6, color="#555555")
    ax.set_xlabel("Throughput (events/s)")
    ax.set_ylabel("End-to-end latency p99 (ms)")
    ax.set_title("RQ1: throughput vs tail latency (mean, whiskers = min–max)")
    ax.legend(frameon=False, loc="best")
    save(fig, outdir, "rq1_throughput_latency", written)
    return written


# ============================================================= RQ3: phục hồi lỗi
def load_rq3(raw_dir: Path, win_s: float = 5.0, pre_s: float = 30.0, post_s: float = 150.0) -> list:
    """Mỗi run rq3 -> dict: nhãn, t(s) của từng cửa sổ, p99, số probe, kill_ts_ms, container."""
    out = []
    for d in sorted(raw_dir.glob("rq3_*")):
        lat_p, meta_p = d / "latency.csv", d / "meta.json"
        if not (lat_p.exists() and meta_p.exists()):
            continue
        try:
            meta = json.loads(meta_p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"[plot] bỏ qua {d.name}: meta.json lỗi ({exc})", file=sys.stderr)
            continue
        kill = meta.get("kill_ts_ms")
        lat = pd.read_csv(lat_p)
        if not kill or lat.empty or not {"recv_ts_ms", "ingest_ts_ms"} <= set(lat.columns):
            continue
        lat = lat.dropna(subset=["recv_ts_ms", "ingest_ts_ms"])
        if lat.empty:
            continue
        lat["lat"] = lat.recv_ts_ms.astype("int64") - lat.ingest_ts_ms.astype("int64")
        t = (lat.recv_ts_ms.astype("int64") - int(kill)) / 1000.0
        keep = (t >= -pre_s) & (t <= post_s)
        sel = lat[keep].assign(t=t[keep])
        if sel.empty:
            continue
        sel["w"] = np.floor(sel.t / win_s) * win_s
        g = sel.groupby("w").agg(p99=("lat", lambda s: float(s.quantile(0.99))),
                                 n=("lat", "size")).reset_index()
        recovery = None
        summ_p = d / "summary.json"
        if summ_p.exists():
            try:
                recovery = json.loads(summ_p.read_text(encoding="utf-8")).get("recovery_s")
            except (json.JSONDecodeError, OSError):
                recovery = None
        label = d.name + (f" (recovery {float(recovery):.0f} s)" if recovery is not None else "")
        out.append({"label": label, "t": g.w.to_numpy(dtype=float),
                    "p99": g.p99.to_numpy(dtype=float), "n": g.n.to_numpy(dtype=int),
                    "kill_ts_ms": int(kill), "container": meta.get("killed_container"),
                    "recovery_s": recovery})
    return out


def plot_rq3(series: list, outdir: Path, written: list, win_s: float = 5.0) -> list:
    """Chuỗi thời gian p99 quanh thời điểm kill worker (t = 0)."""
    if not series:
        return written
    fig, ax = plt.subplots(figsize=(7.16, 2.7))
    cmap = plt.get_cmap("tab10")
    for i, s in enumerate(series):
        ax.plot(s["t"], s["p99"], "-o", ms=2.5, lw=1.0, color=cmap(i % 10), label=s["label"])
    ax.axvline(0, ls="--", lw=0.9, color="#333333")
    ax.annotate("kill worker (t = 0)", (0, 1.0), xycoords=("data", "axes fraction"),
                xytext=(4, -10), textcoords="offset points", fontsize=6.5, color="#333333")
    ax.set_xlabel(f"Time relative to kill (s) — p99 per {win_s:g} s window")
    ax.set_ylabel("Latency p99 (ms)")
    ax.set_title("RQ3: tail latency around the worker failure")
    ax.legend(frameon=False, loc="upper right", ncol=2)
    save(fig, outdir, "rq3_fault_recovery", written)
    return written


# ==================================================== dữ liệu giả (dùng cho --demo/--check)
def demo_quality():
    """Bảng chất lượng giả cùng bố cục với rq2_offline_quality.csv."""
    return pd.DataFrame([
        {"engine": "offline", "detector": "zscore", "precision": 0.290, "recall": 0.489, "f1": 0.364},
        {"engine": "offline", "detector": "ewma", "precision": 0.224, "recall": 0.589, "f1": 0.324},
        {"engine": "offline", "detector": "hst", "precision": 0.733, "recall": 0.322, "f1": 0.448},
        {"engine": "offline", "detector": "mp", "precision": 0.049, "recall": 0.089, "f1": 0.064},
    ])


def demo_rq1():
    """3 lần lặp cho mỗi engine x 4 mức tải, đúng cột của results/summary/rq1.csv."""
    rng = np.random.default_rng(42)
    rows = []
    for eng, base in (("flink", 900.0), ("spark", 1400.0)):
        for rate in (1000, 2000, 3000, 4000):
            for rep in (1, 2, 3):
                rows.append({"engine": eng, "cfg": f"rate{rate}", "rep": rep,
                             "throughput_mean_ev_s": rate * (0.92 + 0.05 * rng.random()),
                             "lat_p99_ms": base + 0.35 * rate + 60 * rng.random(),
                             "lat_p50_ms": 50 + 0.05 * rate, "clock_ok": True})
    return pd.DataFrame(rows)


def demo_rq3(win_s: float = 5.0) -> list:
    """Hai run giả (flink/spark) có giai đoạn restart ngay sau kill."""
    rng = np.random.default_rng(7)
    out = []
    for eng, hv in (("flink", 120.0), ("spark", 220.0)):
        t = np.arange(-30.0, 150.0, win_s)
        p99 = np.where(t < 0, 300.0, hv)
        p99 = np.where((t >= 0) & (t < 20), hv + 900, p99)
        p99 = p99 * (0.85 + 0.3 * rng.random(len(t)))
        out.append({"label": f"rq3_{eng}_rate2000_r1 (recovery 20 s)", "t": t, "p99": p99,
                    "n": np.full(len(t), 150), "kill_ts_ms": 0,
                    "container": f"{eng}-worker-1", "recovery_s": 20.0})
    return out


# ====================================================================== điều khiển
def _rel(p: Path) -> str:
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def run_real(which: str, outdir: Path, cfgd: dict) -> list:
    """Vẽ từ dữ liệu THẬT. Hình nào thiếu dữ liệu thì bỏ qua và in lý do."""
    written, missing = [], []
    if which in ("all", "quality"):
        df = load_quality(ROOT / "results" / "summary")
        if df is None:
            missing.append("rq2: chưa có results/summary/rq2_offline_quality.csv (hoặc file "
                           "chất lượng nào có cột precision/recall/f1)")
        else:
            rows = quality_rows(df)
            before = len(written)
            plot_quality(rows, outdir, written)
            print(f"[plot] RQ2: {len(rows)} phương pháp -> "
                  f"{', '.join(_rel(p) for p in written[before:])}")
    if which in ("all", "rq1"):
        df = load_rq1(ROOT / "results" / "summary")
        if df is None:
            missing.append("rq1: chưa có results/summary/rq1.csv")
        else:
            pts = rq1_points(df)
            before = len(written)
            plot_rq1(pts, outdir, written, cfgd["sustain_p99_ms"])
            if len(written) == before:
                missing.append("rq1: rq1.csv không có dòng nào dùng được "
                               "(thông lượng/p99 rỗng)")
            else:
                print(f"[plot] RQ1: {len(pts)} điểm (engine, cấu hình) -> "
                      f"{', '.join(_rel(p) for p in written[before:])}")
    if which in ("all", "rq3"):
        series = load_rq3(ROOT / "results" / "raw", cfgd["recovery_window_s"])
        before = len(written)
        plot_rq3(series, outdir, written, cfgd["recovery_window_s"])
        if len(written) == before:
            missing.append("rq3: chưa có results/raw/rq3_*/{latency.csv, meta.json có kill_ts_ms}")
        else:
            print(f"[plot] RQ3: {len(series)} run -> "
                  f"{', '.join(_rel(p) for p in written[before:])}")
    for m in missing:
        print(f"[plot] BỎ QUA {m}", file=sys.stderr)
    return written


def run_fake(which: str, outdir: Path, cfgd: dict, label: str) -> list:
    """Vẽ bằng dữ liệu giả: kiểm tra toàn bộ hàm vẽ khi chưa có CSV của RQ1/RQ3."""
    written = []
    if which in ("all", "quality"):
        plot_quality(quality_rows(demo_quality()), outdir, written)
    if which in ("all", "rq1"):
        plot_rq1(rq1_points(demo_rq1()), outdir, written, cfgd["sustain_p99_ms"])
    if which in ("all", "rq3"):
        plot_rq3(demo_rq3(cfgd["recovery_window_s"]), outdir, written,
                 cfgd["recovery_window_s"])
    print(f"[plot] {label}: vẽ {len(written)} file trong {_rel(outdir)}")
    return written


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fig", choices=["all", "quality", "rq1", "rq3"], default="all",
                    help="hình cần vẽ (mặc định: tất cả hình có dữ liệu)")
    ap.add_argument("--outdir", default=str(DEFAULT_OUT), help="thư mục đầu ra")
    ap.add_argument("--check", action="store_true",
                    help="vẽ thử bằng dữ liệu giả vào thư mục tạm, KHÔNG ghi results/plots/")
    ap.add_argument("--demo", action="store_true",
                    help="vẽ bằng dữ liệu giả vào <outdir>/demo")
    args = ap.parse_args(argv)

    style_ieee()
    cfgd = read_yaml_defaults()
    outdir = Path(args.outdir)
    if not outdir.is_absolute():
        outdir = ROOT / outdir

    if args.check:
        tmp = Path(tempfile.mkdtemp(prefix="p3_plot_check_"))
        written = run_fake(args.fig, tmp, cfgd, "CHECK")
        expected = 6 if args.fig == "all" else 2
        summ = ROOT / "results" / "summary"
        print(f"[plot] dữ liệu thật có: rq2_offline_quality.csv="
              f"{(summ / 'rq2_offline_quality.csv').exists()}, rq1.csv={(summ / 'rq1.csv').exists()}, "
              f"run rq3={any((ROOT / 'results' / 'raw').glob('rq3_*/meta.json'))}")
        ok = len(written) == expected and all(p.stat().st_size > 0 for p in written)
        print("[plot] CHECK " + ("OK" if ok else f"LỖI: {len(written)}/{expected} file"))
        return 0 if ok else 1

    if args.demo:
        run_fake(args.fig, outdir / "demo", cfgd, "DEMO")
        return 0

    try:
        written = run_real(args.fig, outdir, cfgd)
    except Exception as exc:                       # lỗi vẽ thật -> exit 1 để CI thấy
        print(f"[plot] LỖI khi vẽ: {exc}", file=sys.stderr)
        raise
    if not written:
        print("[plot] không có hình nào được vẽ (thiếu dữ liệu). "
              "Thử: python scripts/plot_results.py --check", file=sys.stderr)
        return 0
    print(f"[plot] xong: {len(written)} file trong {_rel(outdir)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())



