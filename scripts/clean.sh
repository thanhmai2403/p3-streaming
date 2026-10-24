#!/usr/bin/env bash
# Dọn dẹp sau thí nghiệm (README mục 10.4): dừng job Spark, xóa checkpoint và
# file tạm. Tùy chọn --topics để xóa luôn topic sensor/alerts.
#
#   bash scripts/clean.sh              # chỉ checkpoint + job
#   bash scripts/clean.sh --topics     # + xóa topic Kafka
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

KAFKA_CT="${KAFKA_CT:-kafka}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-kafka-topics.sh}"
BROKERS="${BROKERS:-kafka:9092}"

echo "[clean] dừng job Spark (nếu đang chạy)..."
docker exec spark-master sh -c 'pkill -f "spark_job/job.py"' >/dev/null 2>&1 || true

echo "[clean] xóa checkpoint spark..."
rm -rf "$REPO_ROOT/checkpoints/spark"
docker exec spark-master rm -rf /checkpoints/spark >/dev/null 2>&1 || true

if [ "${1:-}" = "--topics" ]; then
  echo "[clean] xóa topic sensor + alerts..."
  for topic in sensor alerts; do
    docker exec "$KAFKA_CT" "$KAFKA_TOPICS_BIN" --bootstrap-server "$BROKERS" \
        --delete --topic "$topic" >/dev/null 2>&1 || true
  done
fi

echo "[clean] xong."
