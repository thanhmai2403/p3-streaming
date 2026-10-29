#!/usr/bin/env bash
# Xóa (nếu có) rồi tạo lại topic `sensor` và `alerts` theo config.yaml (README mục 3).
# Dùng: bash scripts/create_topics.sh
set -euo pipefail
cd "$(dirname "$0")/.."
# Git Bash trên Windows tự dịch '/opt/...' -> 'C:/Program Files/Git/opt/...' (MSYS path conversion) làm
# `docker compose exec ... /opt/kafka/bin/...` sai đường dẫn. Trên WSL biến này không có tác dụng.
# (run_experiment.sh đã đặt sẵn; đặt ở đây để chạy script độc lập cũng đúng.)
export MSYS_NO_PATHCONV=1

# Cùng cách chọn Python như scripts/run_experiment.sh: trên Git Bash của Windows KHÔNG có `python3`
# thật (chỉ có alias Microsoft Store báo "Python was not found" và trả mã lỗi) -> phải dùng Python
# trong .venv của repo (venv Linux: .venv/bin/python, venv Windows: .venv/Scripts/python.exe).
if [ -x ".venv/bin/python" ]; then PY=".venv/bin/python"
elif [ -x ".venv/Scripts/python.exe" ]; then PY=".venv/Scripts/python.exe"
else PY=python3; fi

read -r T_IN T_OUT PARTS RF < <("$PY" - <<'EOF'
import yaml
k = yaml.safe_load(open("config.yaml", encoding="utf-8"))["kafka"]
print(k["topic_in"], k["topic_out"], k["partitions"], k["replication_factor"])
EOF
)
# Python trên Windows in ra CRLF -> cắt '\r' để các giá trị truyền cho docker exec là số sạch ('1' chứ không phải '1\r')
T_IN="${T_IN%$'\r'}"; T_OUT="${T_OUT%$'\r'}"; PARTS="${PARTS%$'\r'}"; RF="${RF%$'\r'}"

kt() { docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 "$@"; }

# Đợi broker trả lời (tối đa ~3 phút). Sau một run quá tải (vd Flink 8000/s), broker có thể bận xóa
# hàng triệu bản ghi của topic cũ và lệnh admin báo "Timed out waiting for a node assignment"
# -> trước đây create_topics.sh chết ngay và cả run bị bỏ (3/24 run RQ1 ngày 27/10).
wait_kafka() {
  local i
  for i in $(seq 1 12); do
    if kt --list >/dev/null 2>&1; then return 0; fi
    echo "Kafka chưa trả lời (lần $i/12), đợi 15 s..." >&2
    sleep 15
  done
  echo "LỖI: Kafka không trả lời sau ~3 phút (xem: docker compose logs --tail 50 kafka)" >&2
  return 1
}
# Chạy lại một lệnh admin tối đa 5 lần (lỗi timeout tạm thời của broker).
retry() {
  local i
  for i in 1 2 3 4 5; do
    if "$@"; then return 0; fi
    echo "Lệnh Kafka lỗi (lần $i/5): $*  -> đợi 10 s rồi thử lại" >&2
    sleep 10
  done
  return 1
}

wait_kafka
for t in "$T_IN" "$T_OUT"; do
  if kt --list | grep -qx "$t"; then
    echo "Xóa topic $t"
    retry kt --delete --if-exists --topic "$t"
  fi
done

# Xóa topic trong Kafka là bất đồng bộ: đợi tới khi biến mất hẳn.
# Lưu ý: KHÔNG viết `[ "$left" = 0 ] && break` — dưới `set -e`, khi điều kiện sai thì cả list trả mã
# 1 và script THOÁT NGAY (đúng lúc topic chưa kịp biến mất), nên phải dùng if/then/fi.
left=""
for _ in $(seq 1 "${DELETE_WAIT_S:-180}"); do
  left=$( (kt --list 2>/dev/null || echo "__kafka_loi__") | grep -cxE "${T_IN}|${T_OUT}|__kafka_loi__" || true)
  if [ "$left" = "0" ]; then break; fi
  sleep 1
done
if [ "${left:-0}" != "0" ]; then
  echo "LỖI: topic ${T_IN}/${T_OUT} vẫn còn sau ${DELETE_WAIT_S:-180} s chờ xóa" >&2
  exit 1
fi

for t in "$T_IN" "$T_OUT"; do
  retry kt --create --if-not-exists --topic "$t" --partitions "$PARTS" --replication-factor "$RF"
done

# Mô tả lại từng topic để xác nhận đã tạo (kafka-topics KHÔNG nhận regex: --topic "a|b" bị coi là
# tên topic theo nghĩa đen -> lỗi "does not exist", nên phải gọi riêng cho từng topic).
for t in "$T_IN" "$T_OUT"; do
  kt --describe --topic "$t" | grep -E "^Topic:"
done
