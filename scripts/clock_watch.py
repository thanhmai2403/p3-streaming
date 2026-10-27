"""Theo dõi đồng hồ WSL có bị chỉnh nhảy không: mỗi 0,5 s so CLOCK_REALTIME với CLOCK_MONOTONIC.
Bình thường hiệu (realtime - monotonic) gần như không đổi; đổi đột ngột > 50 ms = đồng hồ bị nhảy.

  python scripts/clock_watch.py --seconds 180 > clock_watch.log &     (chạy song song với một run đo)
"""
import argparse
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=180)
    ap.add_argument("--threshold-ms", type=float, default=50)
    a = ap.parse_args()
    base = time.time() - time.monotonic()
    prev = base
    t_end = time.monotonic() + a.seconds
    jumps, total = 0, 0.0
    print(f"start wall={time.strftime('%H:%M:%S')}", flush=True)
    while time.monotonic() < t_end:
        time.sleep(0.5)
        off = time.time() - time.monotonic()
        d_ms = (off - prev) * 1000
        if abs(d_ms) > a.threshold_ms:
            jumps += 1
            total += d_ms
            print(f"JUMP {d_ms:+.0f} ms lúc {time.strftime('%H:%M:%S')} "
                  f"(t={time.monotonic() - (t_end - a.seconds):.1f}s)", flush=True)
        prev = off
    print(f"done: {jumps} lần nhảy, tổng {total:+.0f} ms, trôi chậm {((prev - base) * 1000 - total):+.0f} ms",
          flush=True)


if __name__ == "__main__":
    main()
