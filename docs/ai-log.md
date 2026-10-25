# Nhật ký làm việc với AI (README mục 11)

## 2026-10-23 – [B] – Dữ liệu OpenAQ: resample theo giờ + chèn 90 bất thường + detectors_ref.py
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): Làm chuẩn dữ liệu OpenAQ Hà Nội đúng README 7.1/7.2: (a) resample trạm tần suất cao về 1 mẫu/giờ (floor `event_time_ms` về đầu giờ rồi lấy trung bình), (b) chọn 3 trạm đủ dữ liệu để mỗi trạm chèn đủ ~30 bất thường (seed 42, min_gap 48), (c) xóa location thừa trong `data/raw/`, (d) hoàn thiện `common/detectors_ref.py` (thêm `--with-probes`, `--engine`, fix `UnicodeEncodeError` trên Windows).
- Đã kiểm tra bằng: chạy lại toàn bộ pipeline và tự viết script xác minh (clean 19.568 dòng, median_gap 60 phút cả 3 trạm, `labels.csv` 90 bất thường = 30/trạm, `ref_alerts.jsonl` 759 + `ref_alerts_full.jsonl` 1.356 khớp đúng); tất cả PASS.
- Đã sửa gì so với AI: thay `hn_01` từ `2161306` (chỉ 1.525 mẫu, ~9 tháng) sang `2161292` (3.439 mẫu, đủ 12 tháng); thêm bước resample floor+mean vào `clean()`; đặt lại `ref_alerts*.jsonl` về `data/processed/` (vị trí bản gốc), không để ở `results/`.

## 2026-10-23 – [B] – SOTA Half-Space Trees (sota/half_space_trees.py)
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): Viết theo README 10.3: pipeline `MinMaxScaler() | HalfSpaceTrees(n_trees=25, height=15, window_size=250, seed=42)`, chạy độc lập theo từng key (station_id, param), `score_one` trước rồi `learn_one`, ngưỡng phân vị 99% của điểm số trên 20% dữ liệu đầu (không dùng nhãn), warm-up 24 mẫu, xuất `alerts.jsonl` đúng schema 5.2 (`engine="offline"`, `detector="hst"`).
- Đã kiểm tra bằng: chạy trên `openaq_hn_injected.parquet` → 116 alert (hn_01: 57, hn_02: 33, hn_03: 26); chấm `measure_quality.py` → Precision 0.7328, Recall 0.3222, F1 0.4476 (phát hiện 29/90 bất thường).
- Đã sửa gì so với AI: xác nhận chiều điểm số của `HalfSpaceTrees.score_one` (cao = bất thường) từ docstring `river`; xác nhận `learn_one` lần đầu build cây chậm ~10.7s/trạm do `height=15` (2^16−1 node/cây × 25 cây) — chi phí cố hữu của thông số README.

## 2026-10-23 – [B] – measure/compare_outputs.py
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): Viết theo README 9: so tập (seq, detector) của các bản ghi `kind=="alert"` giữa tham chiếu (`--ref`), Flink (`--a`) và Spark (`--b`); đạt khi ba tập trùng nhau hoàn toàn và `score` lệch nhau < 1e-6.
- Đã kiểm tra bằng: so `ref_alerts.jsonl` với chính nó → 759/759 TƯƠNG ĐƯƠNG; chạy `--input equiv_10k.parquet` (tự sinh 759 alert tham chiếu bằng `detectors_ref`) khớp `ref_alerts.jsonl` → TƯƠNG ĐƯƠNG, max_diff 0.00e+00.
- Đã sửa gì so với AI: thêm `--ref-file/--a-file/--b-file` để so trực tiếp 3 file alerts.jsonl (ngoài chế độ `--input` tự chạy bản tham chiếu).

## 2026-10-23 – [B] – producer/producer.py (chế độ --mode replay)
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): Viết theo README 8: đọc parquet, gửi theo thứ tự thời gian với `--speed` sự kiện/giây; `--late-fraction` giữ trễ 5% sự kiện (seed cố định) rồi gửi sau `--late-shift a b` sự kiện kế tiếp cùng trạm; `seq = producer_id * 10^12 + đếm` và `ingest_ts_ms` gán NGAY LÚC GỬI; chỉ gửi trạm có `chỉ_số_trạm % n_producers == producer_id`; cấu hình kafka `linger.ms=5, batch.size=65536, acks=1, compression.type=none`; cờ `--dry-run --output`.
- Đã kiểm tra bằng: dry-run → 19.568 sự kiện, `seq` tăng dần 0..n-1, 974 sự kiện trễ (4.98%); sharding 3 producer → producer 0: hn_03, producer 1: hn_01, producer 2: hn_02 (tổng 19.568, không trùng).
- Đã sửa gì so với AI: sửa lỗi chèn `main()` vào giữa dict `build_message` khi tách file; dùng `np.random.default_rng(seed)` để late-fraction tái lập được.

## 2026-10-24 – [B] – Ngày 2 (hoàn thiện): config dùng chung, Spark job applyInPandasWithState, Docker, đo độ trễ, run_experiment.sh
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): Theo README mục 4/5/6/9/10.1 và PLAN.md Ngày 2: (a) `config.yaml` + `common/config.py` làm nguồn duy nhất của mọi tham số; (b) `common/schema.py` (schema 5.1/5.2); (c) gắn `common/detectors_ref.py` đọc tham số từ `config.yaml`; (d) `spark_job/job.py`: Kafka `sensor` -> `from_json` -> `groupBy(station_id, param)` -> `applyInPandasWithState` (z-score cửa sổ 24 + EWMA + bỏ trễ 2h, `sort_values("seq")` trong micro-batch, probe cho `seq % 100 == 0`, KHÔNG watermark, `GroupStateTimeout.NoTimeout`, `outputMode("append")`) -> sink Kafka `alerts` với `key=station_id`, `value=to_json(struct(*), ignoreNullFields=false)`; (e) `docker/Dockerfile.spark` (FROM `apache/spark:4.1.3` + pandas/pyarrow/pyyaml) + `docker-compose.yml` (master + 2 worker × 2 core/4 GB, bind-mount `checkpoints`); (f) `measure/measure_latency.py` (cửa sổ 5s, p50/p90/p99, T0 = recv bản ghi đầu, RQ3: recovery = cửa sổ đầu sau kill có p99 ≤ 2× p99 trước kill); (g) `scripts/run_experiment.sh` (6 bước) + `scripts/clean.sh`.
- Đã kiểm tra bằng:
  - `py_compile` toàn bộ file mới PASS; `detectors_ref.py` trên `equiv_10k.parquet` -> 759 alert, byte-identical với `ref_alerts.jsonl` đã có.
  - Kiểm thử tương đương: `job.py --test-parquet` -> `spark_alerts.jsonl` 759/759 alert, `max_diff = 0.00e+00`, `compare_outputs.py` => TƯƠNG ĐƯƠNG.
  - Xác minh API pyspark 4.1.3 thực tế (đã cài trong venv): `applyInPandasWithState(func, outputStructType, stateStructType, outputMode: str, timeoutConf: str)` nằm trên `GroupedData`; `GroupStateTimeout.NoTimeout` là CHUỖI (không phải callable); `state.get`/`state.exists` là property và `state.get` trả TUPLE (không phải Row); `state.update(tuple)`.
  - Docker: tag `apache/spark:4.1.3` tồn tại (amd64 + arm64); `rocksdbjni-9.8.4.jar` có sẵn trong phân phối Spark 4.1.3 -> KHÔNG cần `--packages` cho RocksDB, chỉ cần `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3` cho Kafka connector (đã cấu hình trong Dockerfile).
- Đã sửa gì so với AI:
  - `GroupStateTimeout.NoTimeout()` -> `GroupStateTimeout.NoTimeout` (pyspark 4.1.3 định nghĩa là chuỗi, gọi `()` gây `TypeError: 'str' object is not callable`).
  - `_row_to_state` đọc tuple theo chỉ số thay vì thuộc tính Row (`state.get` trả tuple đúng thứ tự STATE_STRUCT).
  - `_copy_state` mô phỏng đúng vòng lưu/đọc state của Spark (`_row_to_state(_state_to_row(st))`); sửa lỗi double-encode JSON làm `buf` thành deque ký tự (`TypeError: unsupported operand type(s) for +: 'int' and 'str'`).
  - Ghi nhận hạn chế: Spark 4.1.3 (Hadoop 3.4.1) KHÔNG chạy được trên Java 25 local (`UnsupportedOperationException: getSubject is not supported` khi tạo checkpoint FileSystem) -> chạy local phải dùng JDK 17/21; trong Docker (Java 17) không ảnh hưởng.

## 2026-10-24 – [B] – Smoke test streaming local (Spark + pandas UDF + checkpoint recovery) PASS end-to-end
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): Xác nhận `spark_job/job.py` chạy streaming `applyInPandasWithState` thành công local (không cần Kafka) trước khi build Docker: nguồn `rate` → groupBy → pandas UDF stateful (z-score + EWMA + probe) → sink, gồm 2 pha: pha 1 checkpoint mới, pha 2 restart từ checkpoint cũ (khôi phục state + offset).
- Kết quả (scripts/_smoke_local.py, ghi mốc vào `smoke_result.txt`): **PASS: pha1=244 pha2=129** — pha 1: 244 bản ghi (162 alert: 81 zscore + 81 ewma, 82 probe); pha 2 khởi động lại từ checkpoint parquet thành công, tiếp tục xử lý +129 bản ghi với state đã khôi phục.
- Phát hiện/sửa trong quá trình:
  1. **pandas 3.1.0 + pyarrow 26 không tương thích pyspark 4.1.3**: `TypeError: 'float' object cannot be interpreted as an integer` trong `pyarrow.lib.RecordBatch.slice` (worker pandas UDF). Cặp chuẩn: **pandas 2.2.3 + pyarrow 19.0.0** (khớp Dockerfile). Đã hạ venv local về đúng cặp này.
  2. **Bug thật trong `spark_job/job.py`**: `None if r.ingest_ts_ms is None else int(r.ingest_ts_ms)` crash vì null long trong pandas UDF đến dạng NaN float (`ValueError: cannot convert float NaN to integer`). Đã thêm helper `_int_or_none(v)` (xử lý None + NaN) và dùng cho `ingest_ts_ms`.
  3. **Spark 4.x không cho restart query từ checkpoint với memory sink**: `AnalysisException: This query does not support recovering from checkpoint location. Delete checkpoints/_smoke/offsets to start over.` → smoke test dùng **parquet file sink** cho cả 2 pha (file sink hỗ trợ recovery); kết quả pha 2 đếm theo delta rows.
  4. **pyspark 4.1 đã bỏ `DataFrame.applyInPandas`** (`[ATTRIBUTE_NOT_SUPPORTED]`); thay bằng `mapInPandas` (nhận iterator pandas DF). `applyInPandasWithState` trên `GroupedData` vẫn hoạt động bình thường (đã xác minh).
  5. Môi trường local: JDK 21 (`_fix_java`), winutils + hadoop.dll (cdarlint/winutils hadoop-3.3.6), `PYSPARK_PYTHON=sys.executable`; chạy nền bằng PowerShell `Start-Process -WindowStyle Hidden` + redirect stdout/stderr riêng (chạy chung console tool dễ bị Ctrl+C giết giữa chừng).
- Đã kiểm tra bằng: đọc `smoke_result.txt` (BẮT ĐẦU/sanity/status từng 5s/KẾT từng pha/PASS); sanity pandas UDF tĩnh OK; batches đều 300–500 rows; không còn lỗi UDF trong toàn bộ 2 pha.
- Chưa làm (bước tiếp): build Docker (`docker compose -f docker-compose.yml build` khi có Docker CLI); chạy `run_experiment.sh` với `RUN_ID=rq1_spark_replay_r1 CONFIG=replay` khi có Kafka.

## 2026-10-24 – [B] – Kiểm tra tương đương THẬT: applyInPandasWithState (Spark) vs bản tham chiếu offline — TƯƠNG ĐƯƠNG
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): Không dùng dữ liệu tổng hợp — chạy `spark_job/job.py` qua `applyInPandasWithState` THẬT trên `data/processed/equiv_10k.parquet` (10.000 sự kiện thật) → ghi `data/processed/spark_alerts.jsonl`, rồi `measure/compare_outputs.py` so với `data/processed/ref_alerts.jsonl`. Không được đổi logic/ngưỡng detector để "vừa khít".
- Kết quả: **TƯƠNG ĐƯƠNG HOÀN HẢO** — `offline vs spark: n=759/759, only=0/0, score_mismatch=0, max_diff=0.00e+00` (cả 3 cặp OK, exit 0).
  - Tổng alert: ref 759 = spark 759; zscore 348 = 348; ewma 411 = 411; seq chung 759/759; lệch score 0; max score diff 0.000000e+00.
  - `spark_alerts.jsonl` gồm 859 bản ghi: 759 alert + 100 probe (seq % 100 == 0, đúng cơ chế probe của job thật); late_dropped = 0.
- Cách làm (thêm chế độ `--test-parquet-spark` vào `spark_job/job.py`, giữ lại làm công cụ kiểm thử):
  1. `equiv_10k.parquet` chỉ có 4 cột `station_id, param, value, event_time_ms` (KHÔNG có seq/ingest_ts_ms; bản tham chiếu tạo `seq = thứ tự dòng vật lý trong file`, `ingest_ts_ms = null`). Spark tái tạo đúng: batch-read (file 121KB, 1 row group → 1 partition → giữ thứ tự dòng) → `seq = monotonically_increasing_id()`, `ingest_ts_ms = NULL` → ghi parquet tạm.
  2. Đọc lại bằng `spark.readStream` (file source) với **schema tường minh** — file source streaming KHÔNG tự infer schema (`IllegalArgumentException: Schema must be specified...`).
  3. `trigger(once=True)` → xử lý hết rồi dừng; `groupBy("station_id","param").applyInPandasWithState(...)` y hệt `build_query`; sink JSON file (append) → gộp `part-*.json` thành `spark_alerts.jsonl`.
- Phát hiện/điểm chú ý:
  1. `pyarrow 19.0.0` KHÔNG đọc được `equiv_10k.parquet` (`OSError: Repetition level histogram size mismatch`) — file vẫn hợp lệ (JVM parquet-mr đọc bình thường; pyarrow 26 đọc được). Hệ quả: chế độ offline `--test-parquet` (dùng `pd.read_parquet`) hiện vướng lỗi này với cặp pandas 2.2.3 + pyarrow 19; chế độ mới `--test-parquet-spark` không bị ảnh hưởng (đọc bằng JVM).
  2. JDK phải là bản OpenJDK/Temurin chuẩn: `C:\Users\M\jdk-21` (Temurin 21.0.12.1) chạy OK; JBR của Android Studio thiếu module `jdk.incubator.vector` → `PySparkRuntimeError: [JAVA_GATEWAY_EXITED]`.
  3. Process chạy nền bằng `Start-Process` từ phiên console của công cụ bị kill khi mở lệnh foreground mới → với job dài nên chạy foreground trong 1 lệnh duy nhất + redirect log.
  4. Shutdown hook Spark báo lỗi xóa thư mục temp/LOCK trên Windows — vô hại (chỉ là dọn temp).
- Đã sửa gì so với AI: không đổi detector/ngưỡng; chỉ thêm hạ tầng chạy (chế độ `--test-parquet-spark`, schema tường minh, tổng hợp seq/ingest_ts_ms đúng cách bản tham chiếu đã làm).


## 2026-10-25 – [B] – Hợp nhất nhánh A (Flink + hạ tầng) vào `b/data`; sửa 2 file parquet cho pyarrow 19.0.0; thêm Spark vào docker-compose
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): Hợp nhất `origin/main` của A (Flink 2.2.1, Kafka 4.2.2, `--mode rate`, `measure_perf.py`, `matrix_profile.py`, `tests/`, `docs/handover_A.md`) vào nhánh của B mà **không mất** code Spark/`--mode replay`; làm `equiv_10k.parquet` và `openaq_hn_injected.parquet` đọc được bằng pandas 2.2.3 + pyarrow 19.0.0; thêm `spark-master` + 2 worker vào `docker-compose.yml` với tài nguyên y hệt Flink; stage các file Ưu tiên 0 (chưa commit/push).
- Phát hiện quan trọng khi merge:
  1. Hai nhánh **không cùng lịch sử** (`fatal: refusing to merge unrelated histories`) — A upload lại repo chứ không fork từ commit của B.
  2. Commit `b907f6a` của A ("khung thư mục theo README mục 2") đã **ghi đè mọi file thành 0 byte**, sau đó A chỉ điền lại phần của A. Vì vậy fast-forward sẽ **xóa sạch** `spark_job/job.py`, `common/detectors_ref.py`, `measure/*`, `sota/half_space_trees.py`, `docker/Dockerfile.spark`, `docs/ai-log.md`, `PLAN.md`…
  → Merge bằng `git merge --allow-unrelated-histories --no-ff --no-commit origin/main`, rồi soát từng file: chỉ 7 file xung đột (`.gitignore`, `README.md`, `common/config.py`, `common/schema.py`, `config.yaml`, `docker-compose.yml`, `producer/producer.py`); các file B còn lại giữ nguyên byte-for-byte.
- Cách hợp nhất file dùng chung:
  1. `config.yaml` + `README.md`: lấy bản của A (đã có `recv_ts_ms`, `flink.python_execution_mode: thread`, `flink.assign_timestamps: false`, `sota.mp.refractory`, `spark.shuffle_partitions: 4`, `spark.checkpoint_dir: /checkpoints/spark`).
  2. `docker-compose.yml`: lấy bản của A (Kafka + Flink) rồi thêm `spark-master`/`spark-worker-1`/`spark-worker-2` với giới hạn y hệt phía Flink (0,5 core/1200m và 2 core/3500m → tổng 4 core), volume `spark-checkpoints`.
  3. `common/schema.py`: giữ API của A (`make_seq`/`split_seq`, `make_event`, `make_alert`/`make_probe`, `validate_output`, `dumps`, `now_ms`) và thêm lại phần của B (`spark_event_struct`, `spark_record_struct`, `make_record`, `serialize_*`, `parse_record`, bí danh `SENSOR_VERSION`).
  4. `common/config.py`: giữ `load()`/`get()` của A, thêm `load_config()`/`resolve_config_path()` của B, kèm bảng `LEGACY_KEYS` dịch tên khóa phẳng cũ (`window` → `detector.zscore.window`, `kafka.docker_brokers` → `kafka.bootstrap_internal`, …) để code cũ không vỡ.
  5. `producer/producer.py`: lấy bản của A (`Sender`, `--mode rate`, `Pacer`, `sent.csv`) và đưa lại chuẩn replay của B: chia trạm theo `chỉ_số_trạm % n_producers` (`station_index`/`select_stations`), nhận `--bootstrap-servers` (tên cũ của B), `--dry-run --output` ghi JSON từng sự kiện.
  6. `spark_job/job.py` + `common/detectors_ref.py`: đổi khóa cấu hình cũ sang cấu trúc mới của A (`spark.shuffle_partitions`, `spark.checkpoint_dir`, `kafka.bootstrap_internal`, `detector.*`, `late.tolerance_ms`, `probe.every`).
- Sửa parquet: `pd.read_parquet` báo `OSError: Repetition level histogram size mismatch` với **cả 2** file (pyarrow 19.0.0). Cách sửa: đọc bằng JVM (Spark `local[2]` + `coalesce(1)` để giữ nguyên thứ tự dòng vật lý) rồi **ghi lại bằng chính pyarrow 19.0.0** (`to_parquet(engine="pyarrow", index=False)`), `assert_frame_equal` khớp 100% rồi mới `os.replace` file gốc.
- Đã kiểm tra bằng:
  - `common/detectors_ref.py` trên `equiv_10k.parquet` (đã sửa) → **759 alert, 0 bỏ vì trễ**; `measure/compare_outputs.py` → `n=759/759, score_mismatch=0, max_diff=0.00e+00` → **TƯƠNG ĐƯƠNG** (exit 0).
  - `spark_job/job.py --test-parquet-spark` (applyInPandasWithState THẬT, RocksDB) → `spark_alerts.jsonl`; `compare_outputs.py` ref vs spark → **759/759, max_diff=0.00e+00**.
  - `docker-compose.yml` parse YAML OK: spark-master 0.5 CPU/1200m, mỗi worker 2 CPU/3500m (tổng 4 core) — y hệt `flink-jm`/`flink-tm`.
  - `spark_job/job.py`: `spark.sql.shuffle.partitions` = `spark.shuffle_partitions` = **4**; checkpoint `/checkpoints/spark/<run_id>`; **không** gọi `withWatermark`; `sort_values("seq", kind="stable")` trong pandas UDF.
- Chưa làm / lưu ý:
  - Việc thống nhất `scripts/run_experiment.sh` (dùng `measure_perf.py`, network mặc định `p3`) **đã xử lý** ở mục 2026-10-25 bên dưới.
  - Chưa build/chạy Docker trong phiên này (môi trường không có Docker CLI).

## 2026-10-25 – [B] – Hoàn thiện Ưu tiên 0 & 1 sau merge: measure_quality v2, run_experiment.sh 7 bước, chấm MP của A
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn):
  1. `measure/measure_quality.py` đúng yêu cầu của A (handover mục 2.1/2.3, Ưu tiên 1): cửa sổ `[start_ms, end_ms + 1 giờ]`, gộp theo `(engine, detector)`, detection delay lấy từ `recv_ts_ms`, **chịu được `seq`/`ingest_ts_ms`/`emit_ts_ms = null` và thiếu `recv_ts_ms`** (output `matrix_profile.py`, `half_space_trees.py`) mà không crash.
  2. `scripts/run_experiment.sh <rq> <engine> <config> <lần>` theo đúng 7 bước trong handover, dùng `measure_perf.py` + network mặc định của compose.
  3. Xoá `requirements-b.txt`, chạy 4 test không cần Docker của A, lấy thông số CPU/RAM của máy B, commit + push.
- `measure/measure_quality.py` (v2):
  - `load_alerts()`: bỏ bản ghi hỏng / thiếu `station_id|param|event_time_ms` (đếm rồi in ra stderr), chuẩn hoá `station_id`/`param` về str, thiếu `recv_ts_ms` thì điền NaN.
  - **Sửa lỗi cũ:** `drop_duplicates(["engine","detector","seq"])` gộp *toàn bộ* alert của Matrix Profile thành **1 dòng** vì MP để `seq = null` (pandas coi NaN bằng nhau). Nay dedupe theo `(engine, detector, trạm, param, event_time_ms, round(score, 9))` — không phụ thuộc `seq`.
  - Detection delay = `recv_ts_ms − ingest_ts_ms` của alert **sớm nhất đo được** trong cửa sổ. `ingest_ts_ms` đã có sẵn trong bản ghi alert nên **không cần `sent.csv`** nữa (`--sent` chỉ còn là nguồn dự phòng; `sent.csv` của producer là thống kê theo giây nên tự bỏ qua).
  - Cột kết quả: `detection_delay_ms_{median,mean,p95}`, `detection_delay_n`, `detection_delay_hours_event_median`, `has_recv_ts_ms`; thiếu giờ máy thì NaN (CSV để trống) nhưng P/R/F1 vẫn tính; `--window-hours` mặc định 1.0.
- `scripts/run_experiment.sh` (viết lại toàn bộ):
  - Cú pháp `bash scripts/run_experiment.sh <rq0|rq1|rq2|rq3> <flink|spark> <config> <lần>`; `run_id = <rq>_<engine>_<config>_r<lần>` khớp regex của `measure_perf.parse_run_id`; từ chối chạy lại run_id đã có kết quả (trừ `FORCE=1`).
  - 7 bước đúng handover: (1) `check_clock.sh` — lệch thì `exit 1`; (2) `docker compose stop` engine kia + `create_topics.sh` + xoá `/checkpoints/{flink,spark}/<run_id>`; (3) submit — Flink `flink run -d -py /opt/p3/flink_job/job.py --run-id <id>` (thêm `--delivery EXACTLY_ONCE` khi `DELIVERY=EXACTLY_ONCE`), Spark `spark-submit --master spark://spark-master:7077 --conf spark.cores.max=4 --conf spark.executor.cores=2 --conf spark.executor.memory=2400m --conf spark.sql.shuffle.partitions=4` (driver log `/tmp/p3_driver_<run_id>.log` trong container) rồi chờ job chạy; (4) `measure_perf.py collect --docker-stats` chạy nền; (5) producer: `rq0` replay `equiv_10k --speed 2000`, `rq1/rq3` `--mode rate --rate <R> --duration 210`, `rq2` replay OpenAQ `--late-fraction 0.05 --late-shift 1 4`, chia `--producer-id/--n-producers` khi `PRODUCERS>1`; (6) rq3 giết `flink-tm1`/`spark-worker-1` ở giây `KILL_AT_S=60` rồi `docker start`, ghi `kill_ts_ms` + `killed_container` vào `meta.json`; (7) hủy job (`flink cancel` / `pkill spark_job/job.py`), `summarize`, `measure_quality.py` nếu rq2, `check_clock.sh` lần cuối.
  - Bỏ hẳn `measure/measure_latency.py` và `docker network connect p3-net` khỏi luồng chạy (giữ `measure_latency.py` trong repo làm tham khảo).
  - `bash -n` trên bản LF: **OK**. (Các file `.sh` trong working tree là CRLF do `core.autocrlf=true`; blob trong git là LF.)
- Chấm lại chất lượng trên `data/processed/labels.csv` (90 nhãn, cửa sổ `[start, end+1h]`, cùng một bộ chấm):

  | engine | detector | alert | trong cửa sổ | phát hiện | P | R | F1 |
  | --- | --- | --- | --- | --- | --- | --- | --- |
  | offline | hst | 116 | 85 | 29/90 | 0.7328 | 0.3222 | 0.4476 |
  | offline | zscore (ref) | 348 | 101 | 44/90 | 0.2902 | 0.4889 | 0.3642 |
  | offline | ewma (ref) | 411 | 92 | 53/90 | 0.2238 | 0.5889 | 0.3244 |
  | offline | mp (`matrix_profile.py` của A) | 162 | 8 | 8/90 | 0.0494 | 0.0889 | 0.0635 |

  - HST ra đúng số cũ (0.7328 / 0.3222 / 0.4476) ⇒ merge không làm lệch cách chấm; MP không còn bị gộp thành 1 dòng.
  - MP: 92,1 s cho 3 trạm, alert rải khắp chuỗi (trung vị 123 h trước nhãn; chỉ 19/90 nhãn có alert trong 24 h trước `start`) ⇒ kém do **ngưỡng phân vị 99 % lấy trên 20 % dữ liệu đầu + refractory = m**, không phải do bộ chấm. Cần A xem lại tham số (calib rộng hơn, hạ phân vị, hoặc so với `--causal`).
- Cài thêm `stumpy==1.14.1` (+ `numba 0.68.0`, `llvmlite 0.50.0`) — đã ghim trong `requirements.txt` nhưng venv thiếu nên `sota/matrix_profile.py` không chạy được trước đó.
- Test của A (không cần Docker), `PYTHONPATH=. python -m tests.<tên>`:
  - `test_detector`: `events=1200 alerts=95 (zscore=45, ewma=50)`, `max|score diff| = 2.80e-12` → **PASS**.
  - `test_producer`: rate 3 producer × 2999 sự kiện (mục tiêu 3000), replay lùi 118/1200 (~10 %) → **PASS**.
  - `test_measure`: p50 66 ms / p99 13 795 ms, `dup_seq=100`, `missing_seq=1`, `recovery_s=25.0`, cảnh báo đồng hồ đúng → **PASS**.
  - `test_mp`: 3 biến thể Matrix Profile chạy được trên chuỗi giả → **PASS**.
- Thông số máy B (để chốt máy đo):
  - Windows 11 Home, **AMD Ryzen 5 5600U — 6 core vật lý / 12 luồng**, **13,84 GB RAM**.
  - WSL2 Ubuntu: **nproc = 8**, `MemTotal = 9 154 240 kB ≈ 8,73 GiB` (chưa có `.wslconfig` riêng); Docker Desktop đang Stop.
  - **Cảnh báo:** compose hiện tại cần Kafka 2 CPU/3 GB + engine 4 CPU/(1,2 + 2×3,5 GB) = **~11,2 GB**, vượt RAM WSL (8,73 GB). Trước khi chạy RQ1/RQ3 phải tăng `memory=` trong `%UserProfile%\.wslconfig` (máy có 13,84 GB → đặt 10–11 GB) hoặc giảm Kafka xuống ~1,5–2 GB. CPU đủ (8 vCPU ≥ 6 core cần dùng).
- Đã sửa gì so với AI: chấm cả 4 bộ alert trong **một** lần chạy (bảng trên) thay vì tin kết quả cũ; phát hiện và sửa lỗi dedupe `seq = null`; đổi detection delay sang hiệu `recv_ts_ms − ingest_ts_ms` để không phụ thuộc `sent.csv`; thêm kiểm tra `docker compose version`, chờ job RUNNING và `bash -n` trước khi commit.
- Chưa làm: chưa build/`docker compose up` (môi trường này không có Docker CLI) nên phần submit Spark/Flink trong `run_experiment.sh` mới chỉ được kiểm tra cú pháp, chưa chạy thật.

## 2026-10-25 – [B] – README 7.1 chi tiết, `docs/handover_B.md`, `scripts/plot_results.py`, bản thảo báo cáo IEEE của B
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): 4 hạng mục tiếp theo của B sau khi xong Ưu tiên 0/1:
  1. Điền rõ quy tắc tiền xử lý OpenAQ Hà Nội vào **README 7.1** (3 trạm + location id, 19 568 dòng, `median_gap = 60 phút`, quy tắc `floor` về đầu giờ UTC + `mean`, ≥ 1 mẫu/giờ, không nội suy; `seed = 42`, `min_gap = 48` giờ, 30 sự kiện/trạm → 90 nhãn).
  2. Viết **`docs/handover_B.md`** theo bố cục bảng biểu giống `handover_A.md` (tóm tắt, file đã làm, bảng chấm chất lượng + phân tích MP, phát hiện kỹ thuật, trả lời 5 câu hỏi mở ở Mục 7 của handover_A, lệnh nhanh).
  3. Viết **`scripts/plot_results.py`**: 3 hình IEEE (RQ2 cột nhóm P/R/F1; RQ1 thông lượng vs p99 kèm min–max; RQ3 p99 quanh lúc kill worker), xuất `.png` 300 DPI + `.pdf` vào `results/plots/`, có `--check` / `--demo` để kiểm tra hàm vẽ khi chưa có CSV của RQ1/RQ3.
  4. Viết **`docs/report_B_draft.md`** (Mục 0 Tóm tắt, Mục 4 phương pháp + dữ liệu, Mục 5 thiết kế thí nghiệm + metric, Mục 8 giới hạn, Mục 9 kết luận), rồi cập nhật `ai-log.md`, commit và push `b/data`.
- `README.md`:
  - **7.1 viết lại** thành 6 bước làm sạch có số dòng thật từ `data/processed/cleaning_report.json` (455 512 → 97 118 pm25 → bỏ 57 giá trị âm → 97 061 → 19 568 mẫu/giờ) + bảng 3 trạm.
  - **Đo lại tần suất gốc** từ `data/raw/locationid=*/year=*/month=*/*.csv.gz` thay vì suy đoán: `hn_01` (2161292) **60 phút**, `hn_02` (4946813) **5 phút**, `hn_03` (6123215) **60 phút** ⇒ chỉ `hn_02` thực sự bị gộp 12:1; `hn_01`/`hn_03` đi qua bước resample mà không đổi giá trị.
  - Ghi rõ `floor` là về **mốc đầu giờ UTC** (= 07:00 giờ Việt Nam) để A không hiểu nhầm là đầu giờ địa phương.
  - **7.2** nói rõ 30/trạm → 90 nhãn, `min_gap = 48` mẫu **= 48 giờ**, `skip_first = 48` (tránh vùng warm-up 24 mẫu).
  - **10.4/10.5**: thay dòng `figures/` bằng `results/plots/` + bảng mô tả 3 hình và cách gọi `scripts/plot_results.py`.
- `scripts/plot_results.py` (mới, ~460 dòng):
  - `load_quality()` gộp **mọi** file `results/summary/rq2*quality*.csv` (+ `rq2.csv` nếu có cột P/R/F1), nên sau này run streaming tự sinh `rq2_<engine>_<cfg>_r<n>_quality.csv` là hình tự có thêm cột `flink/zscore`…; `quality_rows()` xếp 4 phương pháp trước (ưu tiên `offline`), detector lạ vẫn được vẽ.
  - `plot_rq1()`: gom theo `(engine, cfg)`, trung bình + thanh sai số **min–max** cả hai trục, nhãn cấu hình cạnh điểm, đường ngưỡng `measure.sustain_p99_ms` đọc từ `config.yaml`.
  - `load_rq3()`: đọc `results/raw/rq3_*/latency.csv` + `meta.json`, tính `lat = recv − ingest`, cửa sổ **5 s** (`measure.recovery_window_s`) trong `[kill − 30 s, kill + 150 s]`, lấy `recovery_s` từ `summary.json` để in trong legend; `plot_rq3()` kẻ đường `t = 0` = lúc kill.
  - `--check` vẽ cả 3 hình bằng dữ liệu giả vào **thư mục tạm** rồi kiểm tra đủ 6 file (PNG+PDF) và in trạng thái dữ liệu thật; `--demo` vẽ vào `results/plots/demo/`. Cùng một hàm vẽ dùng cho cả 3 chế độ (không có nhánh code riêng cho demo).
  - Thiếu dữ liệu thì **bỏ qua + in lý do** (exit 0), lỗi vẽ thật thì raise (exit 1).
- `docs/handover_B.md` (mới): 6 mục; **trả lời từng câu** trong 5 câu hỏi mở của A (máy đo, mức tải RQ1, README 7.1, đồng ý/không đồng ý 6 thay đổi ở mục 2, ai chạy RQ1/RQ3 phía Flink + nháp Thảo luận). Có bảng 4 phương pháp với số thật và 4 lập luận vì sao MP thấp + gợi ý tinh chỉnh cho A.
- `docs/report_B_draft.md` (mới): Mục 0/4/5/8/9 + danh sách tham khảo nháp; công thức z-score, EWMA (kiểm tra trước – cập nhật sau), quy tắc trễ, HST, Matrix Profile; bảng công bằng Flink↔Spark, định nghĩa 7 metric, ma trận RQ0–RQ3, 9 mục giới hạn.
- Đã kiểm tra bằng:
  - `python scripts/plot_results.py --check` → `CHECK OK` (6/6 file, dữ liệu giả), in đúng trạng thái dữ liệu thật (`rq2_offline_quality.csv=True, rq1.csv=True, run rq3=False`).
  - `python scripts/plot_results.py` → **4 file thật**: `results/plots/rq2_quality_comparison.{png,pdf}` (4 phương pháp) và `results/plots/rq1_throughput_latency.{png,pdf}` (1 điểm `flink/try5000`); RQ3 bị bỏ qua kèm thông báo (chưa có run nào có `kill_ts_ms`).
  - Đọc lại PDF: font DejaVuSans nhúng dạng CID (fonttype 42), MediaBox 3,5 in ⇒ đúng khổ 1 cột IEEE; PNG 300 DPI.
  - Số liệu trong README/handover/report đều đối chiếu lại file thật: `cleaning_report.json`, `stations.csv`, `labels.csv`, `results/summary/rq2_offline_quality.csv`, `rq2_offline_mp_r1/meta.json`.
- Đã sửa gì so với AI / phát hiện thêm:
  - venv **thiếu `matplotlib`** dù đã ghim `matplotlib==3.11.2` trong `requirements.txt` → cài đúng bản ghim (kéo theo contourpy/cycler/fonttools/kiwisolver/packaging/pillow/pyparsing). Ai clone repo cần `pip install -r requirements.txt` **đủ**, không chỉ các gói của Spark.
  - Bảng markdown bị vỡ khi ô chứa ký tự `|` (`MinMaxScaler() | HalfSpaceTrees(...)`) → đổi thành `→`.
  - Khi chỉ có 1 engine trong `rq1.csv`, hình RQ1 hiện **1 điểm** — đúng như thiết kế (không bịa dữ liệu Spark), nhưng phải chờ đợt đo Spark mới có hình so sánh thật.
- Chưa làm: vẫn **không có Docker CLI** trong phiên này nên chưa build/`docker compose up`; chưa có RQ1/RQ3 thật ⇒ hình `rq1_throughput_latency` mới 1 điểm và `rq3_fault_recovery` chưa sinh (đã kiểm tra bằng `--demo`/`--check`).



