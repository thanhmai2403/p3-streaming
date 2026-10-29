"""Hình cho phần A của báo cáo (RQ1, RQ3). Hình RQ2 vẫn do scripts/plot_results.py (B) vẽ.

    python scripts/plot_a.py            # -> results/plots/{rq1_rates, rq3_timeline, rq3_breakdown}.{png,pdf}

  rq1_rates      2 ô: (a) thông lượng xử lý vs tốc độ gửi (đường chéo = theo kịp),
                      (b) p99 vs tốc độ gửi (trục log, vạch 1 s). Trung vị, râu = min-max.
                      Chỉ lấy cfg rate<N> (bỏ run thử như try5000).
  rq3_timeline   3 ô (Spark | Flink AT_LEAST_ONCE | Flink EXACTLY_ONCE), p99 theo cửa sổ 10 s quanh
                      lúc kill (t = 0), trục log. Cửa sổ 10 s (không phải 5 s) vì EO chỉ commit
                      10 s/lần; cửa sổ trống = không nhận được gì (đường bị ngắt).
  rq3_breakdown  thanh ngang: mất điện + xử lý bù = thời gian phục hồi (trung vị 3 run,
                      chấm = từng run). Đọc results/summary/rq3_detail.csv (chạy rq3_report.py trước).

Kiểu chữ/kích thước theo style_ieee() của plot_results.py. Màu: Flink ALO #2a78d6, Spark #eb6834,
Flink EO #1baf7a (đã kiểm CVD; EO tương phản thấp nên luôn có nhãn chữ đi kèm).
"""
import json
import re
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import plot_results as pr  # noqa: E402

OUT = ROOT / "results" / "plots"
C = {"flink": "#2a78d6", "spark": "#eb6834", "flink_eo": "#1baf7a"}
INK, MUTED = "#222222", "#6b6b6b"
GROUPS = [("spark", "Spark", C["spark"]),
          ("AT_LEAST_ONCE", "Flink at-least-once", C["flink"]),
          ("EXACTLY_ONCE", "Flink exactly-once", C["flink_eo"])]


def _group(run_id: str) -> str:
    if run_id.startswith("rq3_spark"):
        return "spark"
    return "EXACTLY_ONCE" if "eo" in run_id.split("_")[2].lower() else "AT_LEAST_ONCE"


# ================================================================= RQ1
def fig_rq1(written: list):
    p = ROOT / "results" / "summary" / "rq1.csv"
    if not p.exists():
        print("[plot_a] bỏ qua RQ1: chưa có results/summary/rq1.csv")
        return
    df = pd.read_csv(p)
    df["offered"] = df.cfg.str.extract(r"^rate(\d+)$", expand=False).astype(float)
    skipped = sorted(df[df.offered.isna()].cfg.unique())
    if skipped:
        print(f"[plot_a] RQ1: bỏ cfg không phải rate<N>: {skipped}")
    df = df.dropna(subset=["offered"])
    g = df.groupby(["engine", "offered"]).agg(
        thr=("throughput_mean_ev_s", "median"), thr_lo=("throughput_mean_ev_s", "min"),
        thr_hi=("throughput_mean_ev_s", "max"), p99=("lat_p99_ms", "median"),
        p99_lo=("lat_p99_ms", "min"), p99_hi=("lat_p99_ms", "max"), n=("run_id", "size")).reset_index()

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(7.16, 2.6))
    rates = sorted(g.offered.unique())
    lim = max(rates) * 1.12
    a1.plot([0, lim], [0, lim], ls="--", lw=0.8, color=MUTED, label="Theo kịp (xử lý = gửi)")
    for eng in ["flink", "spark"]:
        s = g[g.engine == eng].sort_values("offered")
        if s.empty:
            continue
        lab = f"{eng.capitalize()} (n={int(s.n.sum())} run)"
        a1.errorbar(s.offered, s.thr, yerr=[s.thr - s.thr_lo, s.thr_hi - s.thr], fmt="-o", ms=4.5,
                    lw=1.6, capsize=2.5, elinewidth=0.8, color=C[eng], label=lab)
        a2.errorbar(s.offered, s.p99 / 1000, yerr=[(s.p99 - s.p99_lo) / 1000, (s.p99_hi - s.p99) / 1000],
                    fmt="-o", ms=4.5, lw=1.6, capsize=2.5, elinewidth=0.8, color=C[eng], label=lab)
        last = s.iloc[-1]                       # nhãn trực tiếp ở đầu mút
        a1.annotate(f"{last.thr:,.0f}", (last.offered, last.thr), xytext=(0, 7), textcoords="offset points",
                    fontsize=6.5, color=INK, ha="center", va="bottom")
        a2.annotate(f"{last.p99 / 1000:.0f} s", (last.offered, last.p99 / 1000), xytext=(4, 0),
                    textcoords="offset points", fontsize=6.5, color=INK, va="center")
    a1.set(xlim=(0, lim), ylim=(0, lim), xticks=rates, xlabel="Tốc độ gửi (sự kiện/s)",
           ylabel="Thông lượng xử lý (sự kiện/s)", title="(a) Thông lượng")
    a1.legend(frameon=False, loc="upper left")
    a2.set_yscale("log")
    a2.axhline(1.0, ls="--", lw=0.8, color=MUTED)
    a2.annotate("p99 = 1 s", (max(rates) * 1.15, 1.0), xytext=(0, 3), textcoords="offset points",
                fontsize=6.5, color=MUTED, ha="right")
    a2.set(xticks=rates, xlim=(rates[0] * 0.6, max(rates) * 1.18), xlabel="Tốc độ gửi (sự kiện/s)",
           ylabel="Độ trễ p99 (s, thang log)", title="(b) Độ trễ đuôi")
    for ax in (a1, a2):
        ax.set_xticklabels([f"{int(r):,}" for r in rates])
    fig.tight_layout()
    pr.save(fig, OUT, "rq1_rates", written)
    print(g.assign(p99_s=(g.p99 / 1000).round(2)).drop(columns=["p99", "p99_lo", "p99_hi"]).round(0)
          .to_string(index=False))


# ================================================================= RQ3
def fig_rq3_timeline(written: list, win_s: float = 10.0, pre_s: float = 40.0, post_s: float = 160.0):
    runs = {}
    for d in sorted((ROOT / "results" / "raw").glob("rq3_*")):
        try:
            kill = int(json.loads((d / "meta.json").read_text(encoding="utf-8"))["kill_ts_ms"])
            lat = pd.read_csv(d / "latency.csv")
        except Exception:  # noqa: BLE001
            continue
        lat["lat"] = (lat.recv_ts_ms - lat.ingest_ts_ms) / 1000
        lat["t"] = (lat.recv_ts_ms - kill) / 1000
        lat = lat[(lat.t >= -pre_s) & (lat.t < post_s)]
        bins = np.arange(-pre_s, post_s + win_s, win_s)
        lat["w"] = pd.cut(lat.t, bins, right=False, labels=bins[:-1]).astype(float)
        s = lat.groupby("w").lat.quantile(0.99).reindex(bins[:-1])   # NaN = cửa sổ trống
        runs.setdefault(_group(d.name), []).append((re.search(r"_r(\d+)$", d.name).group(1), s))
    if not runs:
        print("[plot_a] bỏ qua rq3_timeline: chưa có results/raw/rq3_*")
        return
    fig, axes = plt.subplots(1, 3, figsize=(7.16, 2.5), sharey=True)
    styles = ["-", "--", ":"]
    for ax, (key, title, col) in zip(axes, GROUPS):
        for i, (rep, s) in enumerate(runs.get(key, [])):
            x = s.index.to_numpy() + win_s / 2
            ax.plot(x, s.to_numpy(), styles[i % 3], marker="o", ms=2.8, lw=1.4, color=col, label=f"r{rep}")
        ax.axvline(0, lw=0.9, color=INK)
        ax.annotate("kill", (0, 1), xycoords=("data", "axes fraction"), xytext=(3, -9),
                    textcoords="offset points", fontsize=6.5, color=INK)
        ax.set_yscale("log")
        ax.set_title(title, color=INK)
        ax.set_xlabel("Thời gian so với lúc kill (s)")
        ax.set_xlim(-pre_s, post_s)
        ax.legend(frameon=False, loc="upper right", fontsize=6, handlelength=2.2)
    axes[0].set_ylabel(f"p99 mỗi cửa sổ {win_s:g} s (s, log)")
    fig.tight_layout()
    pr.save(fig, OUT, "rq3_timeline", written)


def fig_rq3_breakdown(written: list):
    p = ROOT / "results" / "summary" / "rq3_detail.csv"
    if not p.exists():
        print("[plot_a] bỏ qua rq3_breakdown: chạy scripts/rq3_report.py trước")
        return
    df = pd.read_csv(p)
    df = df[df.note.isna()] if "note" in df else df
    df["grp"] = df.run_id.map(_group)
    df["total"] = df.recovery_s.fillna(df.get("observed_s"))
    fig, ax = plt.subplots(figsize=(3.5, 2.2))
    ylabels = []
    for y, (key, title, col) in enumerate(reversed(GROUPS)):
        s = df[df.grp == key]
        if s.empty:
            continue
        out, tot = float(s.outage_s.median()), float(s.total.median())
        ax.barh(y, out, height=0.55, color="#184f95", edgecolor="white", linewidth=1)
        ax.barh(y, tot - out, left=out, height=0.55, color="#86b6ef", edgecolor="white", linewidth=1)
        ax.scatter(s.total, [y] * len(s), s=14, color=INK, zorder=3,
                   marker="o", facecolors="none", linewidths=0.9)
        cens = int(s.censored.fillna(False).astype(bool).sum())
        ax.annotate(f"{tot:.0f} s" + (f"  ({cens} run >)" if cens else ""), (max(tot, s.total.max()), y),
                    xytext=(5, 0), textcoords="offset points", va="center", fontsize=6.5, color=INK)
        ylabels.append((y, f"{title}\n(n={len(s)})"))
    ax.set_yticks([y for y, _ in ylabels], [t for _, t in ylabels])
    ax.set_xlabel("Giây kể từ lúc kill worker")
    ax.set_xlim(0, df.total.max() * 1.3)
    ax.grid(axis="y", visible=False)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color="#184f95", label="Mất điện (không có output)"),
                       Patch(color="#86b6ef", label="Xử lý bù backlog"),
                       plt.Line2D([], [], ls="", marker="o", ms=4, mfc="none", mec=INK, label="Từng run")],
              frameon=False, fontsize=6, loc="lower center", bbox_to_anchor=(0.4, 1.0), ncol=3,
              handlelength=1.2, columnspacing=0.9)
    fig.tight_layout()
    pr.save(fig, OUT, "rq3_breakdown", written)


def main():
    pr.style_ieee()
    OUT.mkdir(parents=True, exist_ok=True)
    written = []
    fig_rq1(written)
    fig_rq3_timeline(written)
    fig_rq3_breakdown(written)
    for w in written:
        print("->", w)


if __name__ == "__main__":
    main()
