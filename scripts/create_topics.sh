#!/usr/bin/env bash
# Xóa (nếu có) rồi tạo lại topic `sensor` và `alerts` theo config.yaml (README mục 3).
# Dùng: bash scripts/create_topics.sh
set -euo pipefail
cd "$(dirname "$0")/.."

read -r T_IN T_OUT PARTS RF < <(python3 - <<'EOF'
import yaml
k = yaml.safe_load(open("config.yaml", encoding="utf-8"))["kafka"]
print(k["topic_in"], k["topic_out"], k["partitions"], k["replication_factor"])
EOF
)

kt() { docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 "$@"; }

for t in "$T_IN" "$T_OUT"; do
  if kt --list | grep -qx "$t"; then
    echo "Xóa topic $t"
    kt --delete --topic "$t"
  fi
done

# Xóa topic trong Kafka là bất đồng bộ: đợi tới khi biến mất hẳn
for _ in $(seq 1 30); do
  left=$(kt --list | grep -cxE "${T_IN}|${T_OUT}" || true)
  [ "$left" = "0" ] && break
  sleep 1
done

for t in "$T_IN" "$T_OUT"; do
  kt --create --topic "$t" --partitions "$PARTS" --replication-factor "$RF"
done

kt --describe --topic "${T_IN}|${T_OUT}" | grep -E "^Topic:"
