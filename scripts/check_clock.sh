#!/usr/bin/env bash
# Kiểm tra đồng hồ trước / sau MỖI lần đo. Chi tiết và lý do: scripts/check_clock.py
# (từ 27/10: độ trễ đo bằng CLOCK_MONOTONIC -> điều kiện là mọi container cùng kernel với WSL).
cd "$(dirname "$0")/.." || exit 1
for PY in .venv/bin/python python3; do command -v "$PY" >/dev/null 2>&1 && break; done
exec "$PY" scripts/check_clock.py "$@"
