#!/usr/bin/env bash
# Kiểm tra đồng hồ trước MỖI lần chạy đo: WSL, container Kafka, container Flink và Windows phải lệch nhau < 50 ms.
# Trên WSL2 đồng hồ có thể bị trôi (nhất là sau khi máy ngủ) rồi tự nhảy giữa chừng -> làm hỏng số đo độ trễ.
# Dùng: bash scripts/check_clock.sh        (lệch quá ngưỡng thì in cách sửa và trả mã lỗi 1)
set -uo pipefail
cd "$(dirname "$0")/.."
ms() { date -u +%s%3N; }
w0=$(ms); k=$(docker compose exec -T kafka date -u +%s%3N 2>/dev/null); w1=$(ms)
f=$(docker compose exec -T flink-jobmanager date -u +%s%3N 2>/dev/null); w2=$(ms)
win=""
if command -v powershell.exe >/dev/null 2>&1; then
  w3=$(ms); win=$(powershell.exe -NoProfile -Command "[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()" 2>/dev/null | tr -d '\r'); w4=$(ms)
fi
bad=0
check() {  # tên, giá_trị, mốc_wsl_trước, mốc_wsl_sau, ngưỡng
  local name=$1 v=$2 a=$3 b=$4 tol=$5
  [ -z "$v" ] && { echo "  $name: không đọc được"; return; }
  local mid=$(( (a + b) / 2 )) d=$(( v - (a + b) / 2 ))
  echo "  $name lệch WSL: ${d} ms (sai số đo ±$(( (b - a) / 2 )) ms)"
  [ ${d#-} -gt "$tol" ] && bad=1
}
echo "Đồng hồ (so với WSL):"
check "kafka   " "$k" "$w0" "$w1" 50
check "flink-jm" "$f" "$w1" "$w2" 50
[ -n "$win" ] && check "windows " "$win" "$w3" "$w4" 1000
if [ $bad = 1 ]; then
  echo "LỆCH QUÁ NGƯỠNG. Sửa: trong PowerShell chạy  wsl --shutdown , mở lại Ubuntu và Docker Desktop, rồi kiểm tra lại."
  echo "(hoặc thử trong WSL:  hwclock -s )"
  exit 1
fi
echo "OK"
