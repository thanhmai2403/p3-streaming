#!/usr/bin/env bash
# run_experiment.sh – chạy MỘT lần thí nghiệm theo đúng luồng của A (docs/handover_A.md, Ưu tiên 1).
#
#   bash scripts/run_experiment.sh <rq> <engine> <config> <lần>
#
#   rq     : rq0 (cổng tương đương) | rq1 (thông lượng/độ trễ) | rq2 (chất lượng, replay OpenAQ)
#            | rq3 (chịu lỗi, kill 1 worker)
#   engine : flink | spark
#   config : nhãn cấu hình, đưa thẳng vào run_id (README 10.4: <rq>_<engine>_<config>_r<lần>)
#            vd rate2000, rate4000, replay, equiv
#   lần    : số nguyên (1, 2, 3...)
#
# Ví dụ:
#   bash scripts/run_experiment.sh rq1 flink rate2000 1
#   bash scripts/run_experiment.sh rq1 spark rate4000 2
#   bash scripts/run_experiment.sh rq0 flink equiv 1
#   bash scripts/run_experiment.sh rq2 spark replay 1
#   bash scripts/run_experiment.sh rq3 spark rate2000 1              # kill ở giây 60
#   DELIVERY=EXACTLY_ONCE bash scripts/run_experiment.sh rq3 flink rate2000 4
#
# 7 bước (handover_A mục 4, Ưu tiên 1):
#   1. `bash scripts/check_clock.sh` – lệch thì dừng ngay (số đo độ trễ sẽ vô nghĩa).
#   2. Dừng engine còn lại, `bash scripts/create_topics.sh`, xóa checkpoint của run_id này.
#   3. Submit job: Flink `flink run -d -py /opt/p3/flink_job/job.py`; Spark `spark-submit` qua
#      spark-master (standalone, 2 worker × 2 core = 4 core, giống Flink).
#   4. `measure/measure_perf.py collect --docker-stats` chạy nền (gắn recv_ts_ms).
#   5. `producer/producer.py`: rq1/rq3 `--mode rate`; rq0 replay equiv_10k; rq2 replay OpenAQ
#      + `--late-fraction 0.05 --late-shift 1 4`.
#   6. rq3: ở giây KILL_AT_S giết 1 worker (`docker kill`) rồi start lại, ghi `kill_ts_ms`
#      (epoch ms) vào results/raw/<run_id>/meta.json -> summarize tự tính thời gian phục hồi.
#   7. Hủy job, `measure_perf.py summarize` (+ `measure_quality.py` nếu rq2), rồi
#      `check_clock.sh` lần cuối.
#
# Biến môi trường hay dùng: RATE, DURATION_S, PRODUCERS, KILL_AT_S, RESTART_AFTER_S, KILL_CT,
# DELIVERY=EXACTLY_ONCE, REPLAY_FILE, REPLAY_SPEED, MAX_SECONDS, FORCE=1 (cho phép chạy lại
# run_id đã có kết quả), WARMUP_S, DRAIN_S.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
COMPOSE=(docker compose -f "$REPO_ROOT/docker-compose.yml")

usage() { sed -n '2,45p' "${BASH_SOURCE[0]}" | sed 's/^#\ \{0,1\}//'; }

if [ $# -ne 4 ]; then
  usage
  echo "" >&2
  echo "Thiếu tham số: cần đúng 4 (nhận $#)" >&2
  exit 2
fi
RQ="$1"; ENGINE="$2"; CONFIG="$3"; REP="$4"

case "$RQ" in
  rq0|rq1|rq2|rq3) ;;
  *) echo "rq phải là rq0|rq1|rq2|rq3 (nhận '$RQ')" >&2; exit 2 ;;
esac
case "$ENGINE" in
  flink|spark) ;;
  *) echo "engine phải là flink|spark (nhận '$ENGINE')" >&2; exit 2 ;;
esac
[[ "$CONFIG" =~ ^[a-z0-9]+([_-][a-z0-9]+)*$ ]] || {
  echo "config chỉ gồm a-z, 0-9, '_' hoặc '-' (nhận '$CONFIG')" >&2; exit 2; }
[[ "$REP" =~ ^[0-9]+$ ]] || { echo "lần phải là số nguyên (nhận '$REP')" >&2; exit 2; }

RUN_ID="${RQ}_${ENGINE}_${CONFIG}_r${REP}"
OUT="$REPO_ROOT/results/raw/$RUN_ID"

# ---------- tham số (đều override được bằng biến môi trường) ----------
RATE="${RATE:-}"
if [ -z "$RATE" ] && [[ "$CONFIG" =~ ^rate([0-9]+) ]]; then RATE="${BASH_REMATCH[1]}"; fi
RATE="${RATE:-2000}"                            # sự kiện/giây (tổng của mọi producer)
DURATION_S="${DURATION_S:-210}"                 # 30 s warm-up + 180 s đo (config.yaml: measure)
PRODUCERS="${PRODUCERS:-1}"                     # chia tải bằng --producer-id/--n-producers
WARMUP_S="${WARMUP_S:-30}"
DRAIN_S="${DRAIN_S:-20}"                        # chờ alert cuối cùng
WAIT_RUNNING_S="${WAIT_RUNNING_S:-180}"
HOST_BROKERS="${HOST_BROKERS:-localhost:29092}" # producer/measure chạy trên host
IN_BROKERS="${IN_BROKERS:-kafka:9092}"          # job trong mạng docker
REPLAY_FILE="${REPLAY_FILE:-data/processed/openaq_hn_injected.parquet}"
REPLAY_SPEED="${REPLAY_SPEED:-}"                 # rỗng: để producer.py tự quyết (rq2 không đặt)
EQUIV_SPEED="${EQUIV_SPEED:-2000}"              # rq0: --speed khi phát lại equiv_10k
KILL_AT_S="${KILL_AT_S:-60}"
RESTART_AFTER_S="${RESTART_AFTER_S:-10}"
DELIVERY="${DELIVERY:-AT_LEAST_ONCE}"           # EXACTLY_ONCE: chỉ Flink có cờ này
LABELS="${LABELS:-data/processed/labels.csv}"
SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3}"
MASTER_URL="${MASTER_URL:-spark://spark-master:7077}"

case "$ENGINE" in
  flink)
    KILL_CT="${KILL_CT:-flink-tm1}"
    OTHER_SVCS=(spark-master spark-worker-1 spark-worker-2)
    MY_SVCS=(kafka flink-jobmanager flink-taskmanager-1 flink-taskmanager-2)
    ;;
  spark)
    KILL_CT="${KILL_CT:-spark-worker-1}"
    OTHER_SVCS=(flink-jobmanager flink-taskmanager-1 flink-taskmanager-2)
    MY_SVCS=(kafka spark-master spark-worker-1 spark-worker-2)
    ;;
esac

if [ -x "$REPO_ROOT/.venv/bin/python" ]; then PY="$REPO_ROOT/.venv/bin/python"
elif [ -x "$REPO_ROOT/.venv/Scripts/python.exe" ]; then PY="$REPO_ROOT/.venv/Scripts/python.exe"
else PY=python3; fi

DC() { "${COMPOSE[@]}" "$@"; }
log() { echo "[run] $(date '+%H:%M:%S') $*"; }
die() { echo "[run] LỖI: $*" >&2; exit 1; }

# ---------- kiểm tra môi trường ----------
command -v docker >/dev/null 2>&1 || die "không có docker CLI trong PATH"
docker compose version >/dev/null 2>&1 || die "không chạy được 'docker compose'"
[ -f "$REPO_ROOT/config.yaml" ] || die "thiếu config.yaml"
[ -f "$LABELS" ] || die "thiếu $LABELS (cần cho measure_quality.py)"
if [ -e "$OUT/latency.csv" ] && [ "${FORCE:-0}" != "1" ]; then
  die "run_id $RUN_ID đã có kết quả trong $OUT (README 10.4: không ghi đè).
     Dùng số lần r mới, hoặc FORCE=1 nếu thật sự muốn chạy lại."
fi
mkdir -p "$OUT"

if [ -z "${MAX_SECONDS:-}" ]; then
  case "$RQ" in
    rq1|rq3) MAX_SECONDS=$((DURATION_S + DRAIN_S + 120)) ;;
    *)       MAX_SECONDS=1200 ;;   # replay: chạy tới hết file
  esac
fi

log "RUN_ID=$RUN_ID  rq=$RQ engine=$ENGINE config=$CONFIG rate=$RATE dur=${DURATION_S}s -> $OUT"

# ---------------------------------------------------------------- (1) đồng hồ
log "[1/7] Kiểm tra đồng hồ trước khi đo (bắt buộc, lệch > 50 ms là dừng)"
if ! bash scripts/check_clock.sh; then
  die "đồng hồ lệch quá ngưỡng -> KHÔNG đo.
     Sửa: trong PowerShell chạy 'wsl --shutdown', mở lại Ubuntu + Docker Desktop, rồi chạy lại."
fi

# ------------------------------------------- (2) dừng engine kia, reset topic
log "[2/7] Dừng engine còn lại (${OTHER_SVCS[*]}), tạo lại topic, xóa checkpoint $RUN_ID"
DC stop "${OTHER_SVCS[@]}" >/dev/null 2>&1 || true
DC up -d "${MY_SVCS[@]}" >&2
bash scripts/create_topics.sh

# Checkpoint: Flink ghi /checkpoints/flink/<run_id>, Spark ghi /checkpoints/spark/<run_id>.
# Volume là named volume dùng chung cho master + các worker -> xóa 1 lần là đủ.
if [ "$ENGINE" = flink ]; then
  DC exec -T flink-jobmanager rm -rf "/checkpoints/flink/$RUN_ID" >/dev/null 2>&1 || true
else
  DC exec -T spark-master rm -rf "/checkpoints/spark/$RUN_ID" >/dev/null 2>&1 || true
fi

# ------------------------------------------------------------- (3) submit job
log "[3/7] Submit job $ENGINE ($RUN_ID)"
if [ "$ENGINE" = flink ]; then
  FLINK_ARGS=(run -d -py /opt/p3/flink_job/job.py --run-id "$RUN_ID")
  if [ "$DELIVERY" = "EXACTLY_ONCE" ]; then FLINK_ARGS+=(--delivery EXACTLY_ONCE); fi
  DC exec -T flink-jobmanager flink "${FLINK_ARGS[@]}" 2>&1 | tee "$OUT/submit.log"
else
  SPARK_SUBMIT=(/opt/spark/bin/spark-submit
    --master "$MASTER_URL"
    --deploy-mode client
    --packages "$SPARK_PACKAGES"
    --driver-memory 600m
    --conf spark.executor.memory=2400m
    --conf spark.executor.cores=2
    --conf spark.cores.max=4
    --conf spark.sql.shuffle.partitions=4
    /opt/p3/spark_job/job.py
    --run-id "$RUN_ID"
    --bootstrap-servers "$IN_BROKERS"
    --checkpoint "/checkpoints/spark/$RUN_ID")
  # -d: trả về ngay; log driver nằm trong container (/opt/p3 là read-only nên ghi /tmp)
  DC exec -d spark-master sh -c "$(printf '%q ' "${SPARK_SUBMIT[@]}")> /tmp/p3_driver_$RUN_ID.log 2>&1"
fi

job_running() {
  # Không dùng `... | grep -q` vì `pipefail` + SIGPIPE (141) có thể làm hàm trả sai.
  if [ "$ENGINE" = flink ]; then
    local out
    out=$(DC exec -T flink-jobmanager flink list -r 2>/dev/null || true)
    [[ "$out" =~ [0-9a-f]{32} ]]
  else
    DC exec -T spark-master grep -q 'checkpoint=' "/tmp/p3_driver_$RUN_ID.log" 2>/dev/null
  fi
}

log "      chờ job nhận việc (tối đa ${WAIT_RUNNING_S}s)..."
ready=0
for ((i = 0; i < WAIT_RUNNING_S; i += 3)); do
  if job_running; then ready=1; break; fi
  sleep 3
done
if [ "$ready" != "1" ]; then
  if [ "$ENGINE" = spark ]; then
    DC exec -T spark-master tail -n 40 "/tmp/p3_driver_$RUN_ID.log" 2>/dev/null || true
  fi
  die "job $ENGINE không chạy sau ${WAIT_RUNNING_S}s (xem $OUT/submit.log)"
fi
log "      job đã chạy"

# ------------------------------------------------------- (4) measure (nền)
log "[4/7] measure_perf.py collect --docker-stats (nền, tối đa ${MAX_SECONDS}s)"
MEASURE_FORCE=()
if [ "${FORCE:-0}" = "1" ]; then MEASURE_FORCE=(--force); fi
"$PY" measure/measure_perf.py collect --run-id "$RUN_ID" --docker-stats \
  --bootstrap "$HOST_BROKERS" --max-seconds "$MAX_SECONDS" \
  ${MEASURE_FORCE[@]+"${MEASURE_FORCE[@]}"} > "$OUT/measure.log" 2>&1 &
MEASURE_PID=$!
sleep 2

# ------------------------------------------------------------- (5) producer
log "[5/7] producer: $PRODUCERS tiến trình (tổng $RATE sự kiện/giây, ${DURATION_S}s)"
PER_RATE=$("$PY" -c "print($RATE / $PRODUCERS)")

prod_args_for() {
  local k=$1
  PROD_ARGS=(--bootstrap-servers "$HOST_BROKERS" --producer-id "$k" --n-producers "$PRODUCERS")
  case "$RQ" in
    rq0)
      PROD_ARGS+=(--mode replay --file data/processed/equiv_10k.parquet --speed "$EQUIV_SPEED")
      ;;
    rq2)
      PROD_ARGS+=(--mode replay --file "$REPLAY_FILE" --late-fraction 0.05 --late-shift 1 4)
      if [ -n "$REPLAY_SPEED" ]; then PROD_ARGS+=(--speed "$REPLAY_SPEED"); fi
      ;;
    *)
      PROD_ARGS+=(--mode rate --rate "$PER_RATE" --duration "$DURATION_S")
      ;;
  esac
  PROD_ARGS+=(--out-dir "$OUT")
}

# ------------------------------------ (6) rq3: lên lịch giết 1 worker ở giây KILL_AT_S
KILLER_PID=""
if [ "$RQ" = "rq3" ]; then
  log "[6/7] rq3: sẽ giết container '$KILL_CT' ở giây $KILL_AT_S, start lại sau ${RESTART_AFTER_S}s"
  (
    sleep "$KILL_AT_S"
    KILL_MS=$("$PY" -c 'import time; print(time.time_ns() // 1_000_000)')
    docker kill "$KILL_CT" >/dev/null 2>&1 || true
    KILL_MS="$KILL_MS" KILL_CT="$KILL_CT" RESTART_AFTER_S="$RESTART_AFTER_S" \
      "$PY" - "$OUT" <<'PY'
import json, os, sys
from pathlib import Path
p = Path(sys.argv[1]) / "meta.json"
meta = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
meta.update({"kill_ts_ms": int(os.environ["KILL_MS"]),
             "killed_container": os.environ["KILL_CT"],
             "restart_after_s": float(os.environ["RESTART_AFTER_S"])})
p.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
PY
    echo "[run] rq3: đã kill $KILL_CT ở $KILL_MS ms (ghi vào meta.json)"
    sleep "$RESTART_AFTER_S"
    docker start "$KILL_CT" >/dev/null 2>&1 || true
    echo "[run] rq3: đã start lại $KILL_CT"
  ) &
  KILLER_PID=$!
else
  log "[6/7] $RQ: không kill worker (chỉ rq3)"
fi

PROD_PIDS=()
for ((k = 0; k < PRODUCERS; k++)); do
  prod_args_for "$k"
  "$PY" producer/producer.py "${PROD_ARGS[@]}" > "$OUT/producer_$k.log" 2>&1 &
  PROD_PIDS+=($!)
done

prod_fail=0
for pid in "${PROD_PIDS[@]}"; do wait "$pid" || prod_fail=1; done
[ "$prod_fail" = "0" ] || log "CẢNH BÁO: có producer lỗi (xem $OUT/producer_*.log)"
if [ -n "$KILLER_PID" ]; then wait "$KILLER_PID" 2>/dev/null || true; fi
log "producer xong; chờ drain ${DRAIN_S}s"
sleep "$DRAIN_S"

# ------------------------------------------- (7) hủy job + tổng hợp kết quả
log "[7/7] Hủy job, tổng hợp kết quả"
if [ "$ENGINE" = flink ]; then
  for jid in $(DC exec -T flink-jobmanager flink list -r 2>/dev/null \
               | grep -oE '[0-9a-f]{32}' | sort -u); do
    DC exec -T flink-jobmanager flink cancel "$jid" >/dev/null 2>&1 || true
  done
  DC logs --no-color --tail 3000 flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 \
    > "$OUT/engine.log" 2>&1 || true
else
  DC exec -T spark-master sh -c "pkill -f 'spark_job/job.py'" >/dev/null 2>&1 || true
  docker cp "spark-master:/tmp/p3_driver_$RUN_ID.log" "$OUT/engine.log" >/dev/null 2>&1 || true
fi

# collect đã tự summarize trong finally; gọi lại cho chắc (idempotent, đọc file đã lưu)
kill -TERM "$MEASURE_PID" 2>/dev/null || true
wait "$MEASURE_PID" 2>/dev/null || true
"$PY" measure/measure_perf.py summarize --run-id "$RUN_ID" \
  || log "CẢNH BÁO: summarize lỗi (xem $OUT/measure.log)"

if [ "$RQ" = "rq2" ]; then
  log "      measure_quality.py: chấm $OUT/alerts.jsonl bằng $LABELS"
  "$PY" measure/measure_quality.py --labels "$LABELS" --alerts "$OUT/alerts.jsonl" \
    --out "$REPO_ROOT/results/summary/${RUN_ID}_quality.csv" \
    || log "CẢNH BÁO: measure_quality.py lỗi"
fi

log "      kiểm tra lại đồng hồ sau khi đo"
clock_ok=0
if bash scripts/check_clock.sh; then clock_ok=1; fi

log "XONG $RUN_ID -> $OUT"
log "  engine.log, producer_*.log, measure.log, latency.csv, alerts.jsonl, summary.json"
if [ "$clock_ok" != "1" ]; then
  log "CẢNH BÁO: đồng hồ lệch ở CUỐI run -> KHÔNG dùng số liệu độ trễ của run này, chạy lại với số r mới"
  exit 1
fi
