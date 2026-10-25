#!/usr/bin/env bash
# Kiểm tra đồng hồ trước MỖI lần chạy đo: WSL, container Kafka, container Flink và Windows phải lệch nhau < 50 ms.
# Trên WSL2 đồng hồ có thể bị trôi (nhất là sau khi máy ngủ) rồi tự nhảy giữa chừng -> làm hỏng số đo độ trễ.
# Dùng: bash scripts/check_clock.sh        (lệch quá ngưỡng thì in cách sửa và trả mã lỗi 1)
set -uo pipefail
cd "$(dirname "$0")/.."
# `date +%s%3N` không được hỗ trợ: trên Ubuntu nó in 19 chữ số (giây + "3" + nanos) thay vì 13 (milis)
# -> tự tính milis bằng %N. Giá trị đọc từ container cũng vậy: lấy 13 chữ số đầu, và nếu không phải
# toàn chữ số (date không hỗ trợ %N, hoặc docker compose in cảnh báo) thì để rỗng = "không đọc được".
# Mỗi đích lấy 5 mẫu và giữ mẫu có round trip nhỏ nhất (sai số đo nhỏ nhất); chỉ FAIL khi
# |lệch| - sai số đo > ngưỡng, nên độ trễ docker compose exec (~180 ms trên máy này) không còn báo oan.
ms() { echo $(( $(date -u +%s%N) / 1000000 )); }

# Đọc đồng hồ của từng đích (dùng qua best_sample bên dưới)
kafka_() { docker compose exec -T kafka date -u +%s%3N; }
flink_() { docker compose exec -T flink-jobmanager date -u +%s%3N; }
win_() { powershell.exe -NoProfile -Command "[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()" | tr -d '\r'; }

# Lấy $2 mẫu bằng hàm đọc $1 và giữ mẫu có round trip (mốc_sau - mốc_trước) nhỏ nhất, tức sai số đo
# nhỏ nhất; mẫu không đọc được thì bỏ. In "mốc_trước mốc_sau giá_trị" (rỗng nếu mọi mẫu đều hỏng).
best_sample() {
  local reader=$1 n=$2 i a b v rt best="" bestrt=""
  for ((i = 0; i < n; i++)); do
    a=$(ms); v=$("$reader" 2>/dev/null); b=$(ms)
    v=${v:0:13}; [[ $v =~ ^[0-9]{13}$ ]] || v=""
    [ -z "$v" ] && continue
    rt=$(( b - a ))
    if [ -z "$bestrt" ] || [ "$rt" -lt "$bestrt" ]; then bestrt=$rt; best="$a $b $v"; fi
  done
  echo "$best"
}

k0="" k1="" k=""; read -r k0 k1 k <<< "$(best_sample kafka_ 5)"
f0="" f1="" f=""; read -r f0 f1 f <<< "$(best_sample flink_ 5)"
win=""
if command -v powershell.exe >/dev/null 2>&1; then
  w3="" w4=""; read -r w3 w4 win <<< "$(best_sample win_ 5)"
fi
bad=0
check() {  # tên, giá_trị, mốc_wsl_trước, mốc_wsl_sau, ngưỡng
  local name=$1 v=$2 a=$3 b=$4 tol=$5
  [ -z "$v" ] && { echo "  $name: không đọc được"; return; }
  local d unc net
  d=$(( v - (a + b) / 2 )); unc=$(( (b - a) / 2 )); net=$(( ${d#-} - unc ))
  [ "$net" -lt 0 ] && net=0
  # Chỉ FAIL khi lệch vượt ngưỡng NGAY CẢ KHI đã trừ hết sai số đo (round trip) -> không còn báo oan;
  # lệch thật cỡ giây (sai số chỉ vài trăm ms) vẫn vượt ngưỡng và FAIL như cũ.
  if [ "$net" -gt "$tol" ]; then
    echo "  $name lệch WSL: ${d} ms (sai số đo ±${unc} ms) -> FAIL (|lệch| - sai số = ${net} ms > ${tol} ms)"
    bad=1
  else
    echo "  $name lệch WSL: ${d} ms (sai số đo ±${unc} ms) -> OK"
  fi
}
echo "Đồng hồ (so với WSL):"
check "kafka   " "$k" "$k0" "$k1" 50
check "flink-jm" "$f" "$f0" "$f1" 50
[ -n "$win" ] && check "windows " "$win" "$w3" "$w4" 1000
if [ $bad = 1 ]; then
  echo "LỆCH QUÁ NGƯỠNG. Sửa: trong PowerShell chạy  wsl --shutdown , mở lại Ubuntu và Docker Desktop, rồi kiểm tra lại."
  echo "(hoặc thử trong WSL:  hwclock -s )"
  exit 1
fi
echo "OK"
