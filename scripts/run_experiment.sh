#!/usr/bin/env bash
# run_experiment.sh – chạy MỘT lần thí nghiệm cho engine Spark (README mục 10.1).
#
# Sáu bước: (1) dừng engine còn lại, (2) xóa/tạo lại topic + xóa checkpoint,
# (3) bật job + measure + docker stats, (4) chờ job RUNNING trên Spark master UI,
# (5) chạy producer: 30s warm-up + 180s đo (rate) hoặc replay tới hết,
# (6) lưu kết quả vào results/raw/<run_id>/ và thêm 1 dòng CSV vào
#     results/summary/<rq>.csv.
#
# Chạy từ Git Bash / WSL. Ví dụ:
#   RUN_ID=rq1_spark_replay_r1 CONFIG=replay bash scripts/run_experiment.sh
#   RUN_ID=rq2_spark_rate_r1  CONFIG=rate   bash scripts/run_experiment.sh
#   RUN_ID=rq3_spark_rate_r1  CONFIG=rate RQ3_KILL_WORKER_S=60 bash scripts/run_experiment.sh
#
# Yêu cầu trước khi chạy:
#   - docker compose up -d --build   (Spark master + 2 worker trong docker-compose.yml)
#   - Kafka của Thành viên A đang chạy và thấy nhau qua network p3-net:
#       docker network connect p3-net <kafka-container>
#   - Producer: mặc định chạy trong Docker (PRODUCER_IMAGE), hoặc PRODUCER_MODE=host
#     để chạy producer.py ngay trên máy (mode replay, do B viết).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
COMPOSE=(docker compose -f "$REPO_ROOT/docker-compose.yml")

# ---- tham số (đều override được bằng biến môi trường) ----
RUN_ID="${RUN_ID:-rq2_spark_replay_r1}"
CONFIG="${CONFIG:-replay}"                     # replay | rate
ENGINE="${ENGINE:-spark}"
MASTER_URL="${MASTER_URL:-spark://spark-master:7077}"
BROKERS="${BROKERS:-kafka:9092}"               # trong mạng docker
HOST_BROKERS="${HOST_BROKERS:-localhost:29092}"
SPARK_MASTER_UI="${SPARK_MASTER_UI:-http://localhost:8080}"
OUT="${OUT:-$REPO_ROOT/results/raw/$RUN_ID}"
WARMUP_S="${WARMUP_S:-30}"
MEASURE_S="${MEASURE_S:-180}"
WAIT_RUNNING_S="${WAIT_RUNNING_S:-180}"
STOP_AFTER_IDLE_S="${STOP_AFTER_IDLE_S:-60}"
DRAIN_S="${DRAIN_S:-30}"
KAFKA_CT="${KAFKA_CT:-kafka}"
KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-kafka-topics.sh}"
OTHER_ENGINE_CTS="${OTHER_ENGINE_CTS:-flink-jobmanager flink-taskmanager}"
PRODUCER_MODE="${PRODUCER_MODE:-docker}"      # docker | host
PRODUCER_IMAGE="${PRODUCER_IMAGE:-p3-producer:latest}"
PRODUCER_CMD="${PRODUCER_CMD:-}"              # nếu đặt: chạy nguyên lệnh này (docker run --network p3-net)
PRODUCER_FILE_HOST="${PRODUCER_FILE_HOST:-$REPO_ROOT/data/processed/openaq_hn_injected.parquet}"
PRODUCER_ARGS_REPLAY="${PRODUCER_ARGS_REPLAY:---speed 500 --late-fraction 0.05 --late-shift 1 4 --seed 42}"
RQ3_KILL_WORKER_S="${RQ3_KILL_WORKER_S:-}"    # vd 60 -> kill ở giây 60 của cửa sổ đo
RQ3_KILL_WORKER_CT="${RQ3_KILL_WORKER_CT:-spark-worker-1}"
RQ3_RESTART_AFTER_S="${RQ3_RESTART_AFTER_S:-10}"

PY="$REPO_ROOT/.venv/Scripts/python.exe"
[ -x "$PY" ] || PY=python

log() { echo "[run] $*" "$(date '+%H:%M:%S')"; }
now_ms() { "$PY" -c "import time; print(time.time_ns()//1_000_000)"; }
sleep_ms() { "$PY" -c "import time,sys; time.sleep(max(0,float(sys.argv[1])/1000))" "$1"; }

mkdir -p "$OUT"
log "RUN_ID=$RUN_ID CONFIG=$CONFIG ENGINE=$ENGINE -> $OUT"

# ---------------------------------------------------------------- (1) engine còn lại
log "[1/6] Dừng engine còn lại (Flink của A, bỏ qua nếu không chạy)..."
for ct in $OTHER_ENGINE_CTS; do docker stop "$ct" >/dev/null 2>&1 || true; done
# đảm bảo Kafka thấy được trên mạng p3-net (idempotent)
docker network connect p3-net "$KAFKA_CT" >/dev/null 2>&1 || true

# ------------------------------------------------- (2) reset topic + checkpoint
log "[2/6] Xóa & tạo lại topic sensor/alerts; xóa checkpoint..."
for topic in sensor alerts; do
  docker exec "$KAFKA_CT" "$KAFKA_TOPICS_BIN" --bootstrap-server "$BROKERS" \
      --delete --topic "$topic" >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do
    if ! docker exec "$KAFKA_CT" "$KAFKA_TOPICS_BIN" --bootstrap-server "$BROKERS" \
         --list 2>/dev/null | grep -qx "$topic"; then break; fi
    sleep 1
  done
  docker exec "$KAFKA_CT" "$KAFKA_TOPICS_BIN" --bootstrap-server "$BROKERS" \
      --create --topic "$topic" --partitions 6 --replication-factor 1
done
rm -rf "$REPO_ROOT/checkpoints/spark/$RUN_ID"
docker exec spark-master rm -rf "/checkpoints/spark/$RUN_ID" >/dev/null 2>&1 || true

# --------------------------------- (3) job + measure + docker stats
log "[3/6] Khởi động job Spark + measure + docker stats..."
docker exec -d spark-master sh -c \
  "python3 /opt/p3/spark_job/job.py --run-id '$RUN_ID' --master '$MASTER_URL' \
   --bootstrap-servers '$BROKERS' > /tmp/p3_engine.log 2>&1"

"$PY" measure/measure_latency.py \
    --bootstrap-servers "$HOST_BROKERS" \
    --group "measure_$RUN_ID" \
    --output-dir "$OUT/measure" \
    --warmup-s "$WARMUP_S" --measure-s "$MEASURE_S" \
    --stop-after-idle-s "$STOP_AFTER_IDLE_S" \
    ${RQ3_KILL_WORKER_S:+--kill-offset-s "$RQ3_KILL_WORKER_S"} \
    > "$OUT/measure.log" 2>&1 &
MEASURE_PID=$!

docker stats --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}}" \
    spark-master spark-worker-1 spark-worker-2 > "$OUT/docker_stats.csv" 2>&1 &
STATS_PID=$!

cleanup() {
  log "dọn dẹp tiến trình nền..."
  [ -n "${MEASURE_PID:-}" ] && kill "$MEASURE_PID" 2>/dev/null || true
  [ -n "${STATS_PID:-}" ] && kill "$STATS_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

# ----------------------------------------------------- (4) chờ job RUNNING
log "[4/6] Chờ job RUNNING (tối đa ${WAIT_RUNNING_S}s)..."
state=""
for _ in $(seq 1 $((WAIT_RUNNING_S / 5))); do
  state="$(curl -s "${SPARK_MASTER_UI}/api/v1/applications" | "$PY" -c "
import json, sys
try:
    apps = json.load(sys.stdin)
except Exception:
    print('ERR'); raise SystemExit
apps = sorted(apps, key=lambda a: a.get('startTime', 0), reverse=True)
print(apps[0]['state'] if apps else 'NONE')" 2>/dev/null || echo ERR)"
  [ "$state" = "RUNNING" ] && break
  sleep 5
done
if [ "$state" != "RUNNING" ]; then
  log "LỖI: job không RUNNING (state=$state). Xem engine log:"
  docker exec spark-master sh -c 'tail -n 50 /tmp/p3_engine.log' 2>/dev/null || true
  exit 1
fi
log "job RUNNING."

# -------------------------------------------------- (5) producer warm-up + đo
log "[5/6] Chờ bản ghi đầu tiên (T0)..."
T0_MS=""
for _ in $(seq 1 120); do
  if [ -f "$OUT/measure/t0.txt" ]; then T0_MS="$(cat "$OUT/measure/t0.txt")"; break; fi
  sleep 1
done
if [ -z "$T0_MS" ]; then
  log "LỖI: không có bản ghi nào sau 120s chờ (job đã RUNNING?)."
  exit 1
fi
log "T0=$T0_MS"

# RQ3: lên lịch kill worker ở đúng T0 + warmup + offset
if [ -n "$RQ3_KILL_WORKER_S" ]; then
  KILL_MS=$((T0_MS + (WARMUP_S + RQ3_KILL_WORKER_S) * 1000))
  log "RQ3: sẽ kill $RQ3_KILL_WORKER_CT ở T0+$((WARMUP_S + RQ3_KILL_WORKER_S))s, restart sau ${RQ3_RESTART_AFTER_S}s"
  (
    NOW="$(now_ms)"
    sleep_ms $((KILL_MS - NOW))
    log "RQ3: kill worker!"
    "${COMPOSE[@]}" stop "$RQ3_KILL_WORKER_CT"
    sleep "$RQ3_RESTART_AFTER_S"
    log "RQ3: restart worker!"
    "${COMPOSE[@]}" start "$RQ3_KILL_WORKER_CT"
  ) &
  KILLER_PID=$!
fi

if [ "$CONFIG" = "rate" ]; then
  # --mode rate: phần của A (chưa có trong repo khi B viết script này).
  # PRODUCER_CMD hoặc image của A phải tự chạy đúng WARMUP_S + MEASURE_S giây.
  if [ -n "$PRODUCER_CMD" ]; then
    docker run --rm --network p3-net $PRODUCER_CMD > "$OUT/producer.log" 2>&1
  else
    docker run --rm --network p3-net \
      -e P3_BROKERS="$BROKERS" \
      -e P3_DURATION=$((WARMUP_S + MEASURE_S)) \
      "$PRODUCER_IMAGE" > "$OUT/producer.log" 2>&1
  fi
else
  # replay: bắn hết dữ liệu rồi thoát (tốc độ do PRODUCER_ARGS_REPLAY quyết định)
  if [ "$PRODUCER_MODE" = "host" ]; then
    "$PY" producer/producer.py --mode replay --file "$PRODUCER_FILE_HOST" \
        --bootstrap-servers "$HOST_BROKERS" $PRODUCER_ARGS_REPLAY \
        > "$OUT/producer.log" 2>&1
  elif [ -n "$PRODUCER_CMD" ]; then
    docker run --rm --network p3-net $PRODUCER_CMD > "$OUT/producer.log" 2>&1
  else
    docker run --rm --network p3-net \
      -e P3_BROKERS="$BROKERS" \
      -e P3_ARGS="$PRODUCER_ARGS_REPLAY" \
      "$PRODUCER_IMAGE" > "$OUT/producer.log" 2>&1
  fi
fi
if [ -n "${KILLER_PID:-}" ]; then wait "$KILLER_PID" 2>/dev/null || true; fi
log "producer xong; chờ drain ${DRAIN_S}s..."
sleep "$DRAIN_S"

# ---------------------------------------------------------- (6) thu thập kết quả
log "[6/6] Dừng job, thu log, ghi CSV tổng hợp..."
docker exec spark-master sh -c 'pkill -f "spark_job/job.py"' >/dev/null 2>&1 || true
docker cp spark-master:/tmp/p3_engine.log "$OUT/engine.log" >/dev/null 2>&1 || true
kill "$MEASURE_PID" 2>/dev/null || true
wait "$MEASURE_PID" 2>/dev/null || true
kill "$STATS_PID" 2>/dev/null || true

RQC="${RUN_ID%%_*}"
SUMMARY_CSV="$REPO_ROOT/results/summary/$RQC.csv"
"$PY" - "$OUT" "$RUN_ID" "$ENGINE" "$CONFIG" "$SUMMARY_CSV" "$WARMUP_S" "$MEASURE_S" <<'PY'
import csv, json, os, re, sys
out, run_id, engine, config, csv_path, warmup_s, measure_s = sys.argv[1:8]

def load_json(p, default=None):
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

s = load_json(os.path.join(out, "measure", "summary.json"), {}) or {}
kinds = s.get("kinds", {})
alert = kinds.get("alert", {})
probe = kinds.get("probe", {})
rq3 = s.get("rq3") or {}

def tail(p):
    try:
        with open(p, encoding="utf-8", errors="replace") as f:
            return f.read()
    except Exception:
        return ""

eng_log = tail(os.path.join(out, "engine.log"))
m = re.findall(r"late_dropped tổng cộng: (\d+)", eng_log)
late_dropped = int(m[-1]) if m else None

prod_log = tail(os.path.join(out, "producer.log"))
m = re.findall(r"đã gửi xong (\d+) sự kiện trong ([\d.]+)s", prod_log)
n_input, prod_dur = (int(m[-1][0]), float(m[-1][1])) if m else (None, None)
throughput = round(n_input / prod_dur, 2) if n_input and prod_dur else None

row = {
    "run_id": run_id, "engine": engine, "config": config,
    "warmup_s": warmup_s, "measure_s": measure_s,
    "n_input_events": n_input, "producer_duration_s": prod_dur,
    "throughput_eps": throughput, "late_dropped": late_dropped,
    "alert_n": alert.get("count"), "alert_p50_ms": alert.get("p50_ms"),
    "alert_p90_ms": alert.get("p90_ms"), "alert_p99_ms": alert.get("p99_ms"),
    "alert_max_ms": alert.get("max_ms"),
    "probe_n": probe.get("count"), "probe_p50_ms": probe.get("p50_ms"),
    "probe_p90_ms": probe.get("p90_ms"), "probe_p99_ms": probe.get("p99_ms"),
    "probe_max_ms": probe.get("max_ms"),
    "rq3_pre_kill_p99_ms": rq3.get("pre_kill_p99_ms"),
    "rq3_recovery_s": rq3.get("recovery_s"),
}
fields = list(row)
os.makedirs(os.path.dirname(csv_path), exist_ok=True)
exists = os.path.exists(csv_path)
with open(csv_path, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    if not exists:
        w.writeheader()
    w.writerow(row)
print("CSV dòng:", json.dumps(row, ensure_ascii=False, default=str))
PY

log "XONG. Kết quả trong: $OUT"
log "  engine.log, producer.log, measure.log, docker_stats.csv"
log "  measure/latency_windows.csv, measure/summary.json"
log "  +1 dòng -> $SUMMARY_CSV"
