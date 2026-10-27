"""Kiểm tra đồng hồ trước / sau mỗi lần đo (run_experiment.sh gọi qua scripts/check_clock.sh).

Từ 27/10/2026 mọi mốc đo độ trễ (ingest_ts_ms, emit_ts_ms, recv_ts_ms, kill_ts_ms) dùng
CLOCK_MONOTONIC (common.schema.now_ms), vì giờ thật trên WSL2 bị chỉnh nhảy giữa lúc chạy
(đã gặp +66 s, +31 s, -34 s). CLOCK_MONOTONIC không bao giờ nhảy, và DÙNG CHUNG cho mọi tiến trình
khi chúng chạy trên CÙNG một kernel.

Điều kiện bắt buộc (FAIL nếu sai): WSL và mọi container đang chạy có cùng boot_id = cùng kernel.
Thông tin thêm (không FAIL): độ lệch GIỜ THẬT của từng container, và giờ thật có nhảy trong lúc
kiểm tra không. Giờ thật chỉ còn ảnh hưởng event_time_ms của dữ liệu tổng hợp (RQ1/RQ3), không ảnh
hưởng số đo độ trễ.

  python scripts/check_clock.py        (mã thoát 1 nếu có container khác kernel)
"""
import subprocess
import sys
import time

CONTAINERS = ["kafka", "flink-jm", "flink-tm1", "flink-tm2",
              "spark-master", "spark-worker-1", "spark-worker-2"]


def sh(args, timeout=15):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def wall_offset_s():
    """Hiệu (giờ thật - monotonic): đổi đột ngột = giờ thật vừa bị chỉnh."""
    return time.time() - time.monotonic()


def realtime_offset(container, n=5):
    """Độ lệch giờ thật của container so với WSL; RTT đo bằng monotonic nên không bao giờ âm."""
    best = None
    for _ in range(n):
        m0, w0 = time.monotonic(), time.time()
        r = sh(["docker", "exec", container, "date", "-u", "+%s%N"])
        m1 = time.monotonic()
        if r.returncode != 0 or not r.stdout.strip().isdigit():
            return None
        rtt = m1 - m0
        off = int(r.stdout.strip()) / 1e9 - (w0 + rtt / 2)
        if best is None or rtt < best[1]:
            best = (off, rtt)
    return best


def main():
    try:
        local_boot = open("/proc/sys/kernel/random/boot_id").read().strip()
    except OSError:
        print("FAIL: không đọc được /proc (đang chạy trên Windows/Git Bash?). Producer, measure và "
              "run_experiment.sh phải chạy TRONG WSL để dùng chung CLOCK_MONOTONIC với container.")
        sys.exit(1)
    running = set(sh(["docker", "ps", "--format", "{{.Names}}"]).stdout.split())
    off_start = wall_offset_s()
    checked, bad = 0, False
    print("Đồng hồ: độ trễ đo bằng CLOCK_MONOTONIC -> điều kiện là mọi container CÙNG KERNEL với WSL")
    for c in CONTAINERS:
        if c not in running:
            continue
        boot = sh(["docker", "exec", c, "cat", "/proc/sys/kernel/random/boot_id"]).stdout.strip()
        same = boot == local_boot
        checked += 1
        bad |= not same
        ro = realtime_offset(c)
        info = "giờ thật: không đọc được" if ro is None else \
            f"giờ thật lệch {ro[0] * 1000:+8.1f} ms (±{ro[1] * 500:.0f} ms)"
        print(f"  {c:15s} {'cùng kernel' if same else 'KHÁC KERNEL':12s} {'ok' if same else 'FAIL'}   ({info})")
    jump_ms = (wall_offset_s() - off_start) * 1000
    if abs(jump_ms) > 50:
        print(f"  Lưu ý: giờ thật của WSL vừa NHẢY {jump_ms:+.0f} ms trong lúc kiểm tra. Không ảnh hưởng số đo "
              f"độ trễ (đã dùng monotonic), chỉ ảnh hưởng event_time_ms của dữ liệu tổng hợp.")
    if not checked:
        print("Không có container nào đang chạy để kiểm tra (bật Kafka + engine trước).")
        sys.exit(1)
    if bad:
        print("FAIL: có container KHÔNG cùng kernel với WSL -> CLOCK_MONOTONIC không dùng chung được, "
              "số đo độ trễ sẽ sai. Chạy producer/measure trong cùng môi trường Docker với engine.")
        sys.exit(1)
    print("OK")


if __name__ == "__main__":
    main()
