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

for t in "$T_IN" "$T_OUT"; do
  if kt --list | grep -qx "$t"; then
    echo "Xóa topic $t"
    kt --delete --topic "$t"
  fi
done

# Xóa topic trong Kafka là bất đồng bộ: đợi tới khi biến mất hẳn.
# Lưu ý: KHÔNG viết `[ "$left" = 0 ] && break` — dưới `set -e`, khi điều kiện sai thì cả list trả mã
# 1 và script THOÁT NGAY (đúng lúc topic chưa kịp biến mất), nên phải dùng if/then/fi.
left=""
for _ in $(seq 1 30); do
  left=$(kt --list | grep -cxE "${T_IN}|${T_OUT}" || true)
  if [ "$left" = "0" ]; then break; fi
  sleep 1
done
if [ "${left:-0}" != "0" ]; then
  echo "LỖI: topic ${T_IN}/${T_OUT} vẫn còn sau 30 s chờ xóa" >&2
  exit 1
fi

for t in "$T_IN" "$T_OUT"; do
  kt --create --topic "$t" --partitions "$PARTS" --replication-factor "$RF"
done

# Mô tả lại từng topic để xác nhận đã tạo (kafka-topics KHÔNG nhận regex: --topic "a|b" bị coi là
# tên topic theo nghĩa đen -> lỗi "does not exist", nên phải gọi riêng cho từng topic).
for t in "$T_IN" "$T_OUT"; do
  kt --describe --topic "$t" | grep -E "^Topic:"
done
