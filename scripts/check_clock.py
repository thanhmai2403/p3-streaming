"""Kiểm tra đồng hồ trước / sau mỗi lần đo.

1) Độ lệch giờ (CLOCK_REALTIME) giữa WSL và từng container Kafka / Flink.
   Mỗi lần hỏi giờ container mất ~100–300 ms (docker exec), nên đo 7 lần và lấy lần có thời gian
   khứ hồi (RTT) ngắn nhất; chỉ báo lệch khi |độ lệch| vượt ngưỡng + RTT/2 (vượt cả sai số đo).
2) Container có chạy CÙNG kernel với WSL không (so boot_id). Cùng kernel thì mọi tiến trình dùng
   chung một đồng hồ, nên "lệch" thật sự chỉ có thể là đồng hồ bị chỉnh nhảy giữa chừng.

  python scripts/check_clock.py            (mã thoát 1 nếu lệch quá ngưỡng)
"""
import subprocess
import sys
import time

TOL_MS = 50
CONTAINERS = ["kafka", "flink-jm", "flink-tm1", "flink-tm2"]


def sh(args, timeout=10):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def boot_id_local():
    return open("/proc/sys/kernel/random/boot_id").read().strip()


def probe(container, n=7):
    best = None
    for _ in range(n):
        t0 = time.time()
        r = sh(["docker", "exec", container, "date", "-u", "+%s%N"])
        t1 = time.time()
        if r.returncode != 0:
            return None
        remote = int(r.stdout.strip()) / 1e9
        rtt = t1 - t0
        off = remote - (t0 + t1) / 2
        if best is None or rtt < best[1]:
            best = (off, rtt)
    return best


def main():
    running = set(sh(["docker", "ps", "--format", "{{.Names}}"]).stdout.split())
    local_boot = boot_id_local()
    bad = False
    checked = 0
    print(f"Đồng hồ so với WSL (ngưỡng {TOL_MS} ms, đã trừ sai số đo):")
    for c in CONTAINERS:
        if c not in running:
            continue
        b = probe(c)
        if b is None:
            print(f"  {c:10s} không đọc được")
            continue
        off_ms, rtt_ms = b[0] * 1000, b[1] * 1000
        same = sh(["docker", "exec", c, "cat", "/proc/sys/kernel/random/boot_id"]).stdout.strip() == local_boot
        checked += 1
        flag = abs(off_ms) > TOL_MS + rtt_ms / 2
        bad |= flag
        print(f"  {c:10s} lệch {off_ms:+7.1f} ms (sai số ±{rtt_ms / 2:.0f} ms)  "
              f"{'cùng kernel' if same else 'KHÁC kernel'}  {'<- LỆCH' if flag else 'ok'}")
    if not checked:
        print("Không có container nào đang chạy để so (docker compose up -d trước).")
        sys.exit(1)
    if bad:
        print("LỆCH QUÁ NGƯỠNG. Sửa: PowerShell -> wsl --shutdown, mở lại Ubuntu + Docker Desktop, kiểm tra lại.")
        sys.exit(1)
    print("OK")


if __name__ == "__main__":
    main()
