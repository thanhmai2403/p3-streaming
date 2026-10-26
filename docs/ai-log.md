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
- Chưa làm (bước tiếp): build Docker (`docker compose -f docker-compose.yml build` khi có Docker CLI); chạy `run_experiment.sh` với `RUN_ID=rq1_spark_replay_r1 CONFIG=replay` khi có Kafka. *(nay đã khắc phục: Docker + Kafka chạy được trên máy B — xem `docs/handover_B.md` mục 3.8)*

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
  - Chưa build/chạy Docker trong phiên này (môi trường không có Docker CLI). *(nay đã khắc phục: Docker chạy được trên máy B — xem `docs/handover_B.md` mục 3.8)*

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
  - 7 bước đúng handover: (1) `check_clock.sh` — lệch thì `exit 1`; (2) `docker compose stop` engine kia + `create_topics.sh` + xoá `/checkpoints/{flink,spark}/<run_id>`; (3) submit — Flink `flink run -d -py /opt/p3/flink_job/job.py --run-id <id>` (thêm `--delivery EXACTLY_ONCE` khi `DELIVERY=EXACTLY_ONCE`), Spark `spark-submit --master spark://spark-master:7077 --driver-memory $SPARK_DRIVER_MEMORY --conf spark.cores.max=4 --conf spark.executor.cores=2 --conf spark.executor.memory=$SPARK_EXECUTOR_MEMORY --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.shuffle.partitions=4` (hai biến RAM mặc định 600m/2400m, lowmem đặt 512m/1024m; connector Kafka nhúng sẵn trong image; driver log `/tmp/p3_driver_<run_id>.log` trong container) rồi chờ job chạy; (4) `measure_perf.py collect --docker-stats` chạy nền; (5) producer: `rq0` replay `equiv_10k --speed 2000`, `rq1/rq3` `--mode rate --rate <R> --duration 210`, `rq2` replay OpenAQ `--late-fraction 0.05 --late-shift 1 4`, chia `--producer-id/--n-producers` khi `PRODUCERS>1`; (6) rq3 giết `flink-tm1`/`spark-worker-1` ở giây `KILL_AT_S=60` rồi `docker start`, ghi `kill_ts_ms` + `killed_container` vào `meta.json`; (7) hủy job (`flink cancel` / `pkill spark_job/job.py`), `summarize`, `measure_quality.py` nếu rq2, `check_clock.sh` lần cuối.
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
  - WSL2 Ubuntu: **nproc = 8**, `MemTotal = 9 154 240 kB ≈ 8,73 GiB` (`.wslconfig`: `memory=9GB`, `processors=8`, `swap=4GB`); Docker Desktop đã chạy (WSL integration bật).
  - **Cảnh báo:** compose hiện tại cần Kafka 2 CPU/3 GB + engine 4 CPU/(1,2 + 2×3,5 GB) = **~11,2 GB**, vượt RAM WSL (8,73 GB). Máy B đã chốt `memory=9GB` trong `%UserProfile%\.wslconfig` (không nâng lên 10–11 GB) và **không** làm máy đo RQ1/RQ3; chỉ chạy RQ0/RQ2 bằng `docker-compose.lowmem.yml` (Kafka 1,5 GB, tổng ~6,9 GB). CPU đủ (8 vCPU ≥ 6 core cần dùng).
- Đã sửa gì so với AI: chấm cả 4 bộ alert trong **một** lần chạy (bảng trên) thay vì tin kết quả cũ; phát hiện và sửa lỗi dedupe `seq = null`; đổi detection delay sang hiệu `recv_ts_ms − ingest_ts_ms` để không phụ thuộc `sent.csv`; thêm kiểm tra `docker compose version`, chờ job RUNNING và `bash -n` trước khi commit.
- Chưa làm: chưa build/`docker compose up` (môi trường này không có Docker CLI) nên phần submit Spark/Flink trong `run_experiment.sh` mới chỉ được kiểm tra cú pháp, chưa chạy thật. *(nay đã khắc phục: Docker chạy được trên máy B — xem `docs/handover_B.md` mục 3.8)*

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
- Chưa làm: vẫn **không có Docker CLI** trong phiên này nên chưa build/`docker compose up`; chưa có RQ1/RQ3 thật ⇒ hình `rq1_throughput_latency` mới 1 điểm và `rq3_fault_recovery` chưa sinh (đã kiểm tra bằng `--demo`/`--check`). *(nay đã khắc phục: Docker chạy được trên máy B — xem `docs/handover_B.md` mục 3.8)*

## 2026-10-25 – [B] – Nhúng connector Kafka vào image Spark, siết `run_experiment.sh`, chốt cổng tương đương Docker `rq0_spark_equiv_r1` (759/759)
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): (1) nhúng connector Kafka vào image Spark để `run_experiment.sh` không cần `--packages`; (2) script fail-fast khi job lỗi và chờ drain theo **offset** thay vì `sleep` mù; (3) chạy thật `rq0 spark equiv 1` qua Kafka trên máy B rồi đối chiếu với bản tham chiếu; (4) sửa các số RAM lỗi thời trong `docs/` và `docker-compose.lowmem.yml`; (5) xử lý triệt để lỗi CRLF khi chạy script trên Git Bash.
- `docker/Dockerfile.spark`: tải trực tiếp 4 jar từ Maven Central (`spark-sql-kafka-0-10_2.13-4.1.3`, `spark-token-provider-kafka-0-10_2.13-4.1.3`, `kafka-clients-3.9.1`, `commons-pool2-2.12.1`) → chép vào `/opt/spark/jars` (chỉ jar **chưa có**), `chown 185:185`, kiểm tra bằng `test -f` + `ls -l` ngay trong layer.
  - Đã thử cách "chuẩn" `spark-shell --packages … --conf spark.jars.ivy=/tmp/.ivy2`: Ivy resolve **đủ 11 jar** nhưng `spark-shell` trả **exit 1** khi nhận `:quit` từ stdin ⇒ build fail ⇒ chuyển sang tải trực tiếp (comment trong Dockerfile ghi lại lý do để người sau không thử lại).
- `scripts/run_experiment.sh`:
  - Bỏ `--packages` (giữ `--conf spark.jars.ivy=/tmp/.ivy2`); điều kiện "job đã chạy" đổi thành **driver log có `Committed offsets for batch`** (tức đã commit micro-batch đầu), thêm `job_failed()` bắt `Traceback|INVALID_|Exception|NoClassDefFoundError` → in 30 dòng cuối driver log rồi `die` (trước đây chờ đủ 300 s mới báo lỗi).
  - Drain theo **offset**: đọc tổng offset cuối của topic `alerts` (`kafka-get-offsets.sh --time -1`) mỗi 5 s, dừng khi đứng yên 20 s, tối thiểu `DRAIN_S`, tối đa `DRAIN_MAX_S` (mặc định 300). Trước đây `sleep DRAIN_S` cố định nên run ngắn vẫn chờ thừa, run dài thì cắt mất alert.
  - Hỗ trợ `COMPOSE_FILE` (`:` trên WSL, `;` trên Git Bash) + `unset COMPOSE_FILE` trước khi gọi docker — trước đây script hard-code `-f docker-compose.yml` nên `docker-compose.lowmem.yml` **không có tác dụng**.
  - `export MSYS_NO_PATHCONV=1` và `REPO_ROOT` dạng `pwd -W` (`C:/...`) cho Git Bash trên Windows (nếu không: `/opt/...` bị dịch thành `C:/Program Files/Git/opt/...`, còn `docker.exe`/`python.exe` chỉ nhận đường dẫn `C:/...`).
  - **CRLF (triệt để)**: mọi giá trị lấy bằng `$(...)` từ `python.exe`/`powershell.exe` đều qua `| tr -d '\r'` — `PER_RATE`, `KILL_MS` trong `run_experiment.sh`; `scripts/create_topics.sh` (`T_IN`/`T_OUT`/`PARTS`/`RF`) và `scripts/check_clock.sh` (đọc đồng hồ Windows) đã làm từ trước. Lỗi thật đã gặp trên Git Bash: `--rate "2000.0\r"` và `Cannot parse argument '1\n' of replication-factor`.
- `docker-compose.yml`: thêm `command:` (`spark-class … deploy.master.Master` / `… deploy.worker.Worker`) cho `spark-master` + 2 worker — image `apache/spark:4.1.3` có `ENTRYPOINT=/opt/entrypoint.sh`, `CMD=null`; thiếu `command` thì entrypoint `exec` rỗng ⇒ container **exit 0 ngay**, cluster không lên. **Cấu hình tài nguyên giữ nguyên chuẩn**: master `cpus: 0.5` / `mem_limit: 1200m`, mỗi worker `cpus: 2` / `mem_limit: 3500m` + `SPARK_WORKER_MEMORY=3072m` (bản lowmem nằm riêng trong `docker-compose.lowmem.yml`).
- `docker-compose.lowmem.yml` (mới): Kafka 1,5 GB, Flink ~5,4 GB, Spark ~6,9 GB (master `mem_limit 1300m`, driver **512m**); sửa các comment còn ghi 384m/1000m.
- `docs/handover_B.md`, `docs/report_B_draft.md`: sửa số RAM lỗi thời (WSL **đã chốt `memory=9GB`**, không nâng 10–11 GB; `SPARK_DRIVER_MEMORY=512m`; máy B **không** phải máy đo RQ1/RQ3).
- Kết quả chạy thật trên máy B (`docker-compose.lowmem.yml` + `SPARK_EXECUTOR_MEMORY=1024m SPARK_DRIVER_MEMORY=512m`):
  - `bash scripts/run_experiment.sh rq0 spark equiv 1` → `RUN_EXIT=0`; producer replay 10 000 sự kiện trong 6,5 s (0 lỗi Kafka); job commit micro-batch đầu sau ~40 s; `meta.json`: `n_alert=759`, `n_probe=100`, `n_bad=0`; `summary.json`: `missing_seq=0`, `dup_seq=0`, `clock_ok=true`.
  - `python measure/compare_outputs.py --ref data/processed/ref_alerts.jsonl --a data/processed/ref_alerts.jsonl --b results/raw/rq0_spark_equiv_r1/alerts.jsonl` → **n=759/759, only=0/0, score_mismatch=0, max_diff=0.00e+00 ⇒ TƯƠNG ĐƯƠNG** — cổng tương đương **qua Kafka thật** của Spark đã qua; bằng chứng lưu trong `results/raw/rq0_spark_equiv_r1/` và `results/summary/rq0.csv`.
- Đã kiểm tra bằng: `bash -n` cho `run_experiment.sh`, `create_topics.sh`, `check_clock.sh`; `wc -l results/raw/rq0_spark_equiv_r1/alerts.jsonl` = 759; `git ls-files --eol` xác nhận mọi `scripts/*.sh` là LF trong working tree; đọc lại `git diff` từng file trước khi commit.
- Lưu ý khi đọc `summary.json` của run này: đây là run **tương đương**, `n_probe_window=0` nên `lat_p50/p95/p99 = NaN`, `throughput = 0` — **không dùng** số hiệu năng của `rq0`; độ trễ/thông lượng chỉ lấy từ `rq1`/`rq3`.
- Chưa làm: chưa chạy `rq0 flink equiv 1` (máy B chưa có jar connector Flink Kafka `docker/jars/flink-sql-connector-kafka-5.0.0-2.2.jar` — tệp này bị `.gitignore`, phải tải riêng); chưa có RQ1/RQ3.


## 2026-10-26 – [B] – RQ2 Spark ×3 qua Kafka; cổng nhiều micro-batch + dữ liệu trễ; sửa 4 lỗi ẩn của `create_topics.sh`
- Công cụ: Cline (Claude)
- Yêu cầu chính (rút gọn): (1) chạy `rq2 spark replay` đủ **3 lần lặp** trên máy B rồi chấm chất lượng, vẽ lại hình và cập nhật tài liệu; (2) hoàn thành việc 3 mục 6 handover (Spark với **nhiều micro-batch** + **dữ liệu trễ**); (3) làm chuỗi script chạy được trên **Git Bash/Windows** mà không phải sửa tay từng bước.
- Kết quả chạy thật `bash scripts/run_experiment.sh rq2 spark replay {1,2,3}` (`docker-compose.lowmem.yml`, `SPARK_EXECUTOR_MEMORY=1024m`, `SPARK_DRIVER_MEMORY=512m`, `REPLAY_SPEED=2000`):
  - Cả 3 run `EXIT=0`; `clock_ok=true` ở cả lần kiểm tra **trước và sau** khi đo; `dup_seq=0`; `n_probe_window=188`; `missing_seq=8/196` (probe rơi đúng vào sự kiện bị bỏ trễ, cùng bản chất `late_dropped=544`).
  - Chất lượng (`results/summary/rq2_spark_replay_r<n>_quality.csv`): z-score P 0,2815 / R 0,8556 / **F1 0,4236**; EWMA P 0,2032 / R 1,0000 / **F1 0,3377** — **giống tuyệt đối ở cả 3 lần** (luồng sự kiện + quy tắc trễ là xác định), chỉ độ trễ đổi (p50 theo run: 27 740 / 25 584 / 30 987 ms; delay trung vị mỗi run 25,6–31,0 s).
  - So với bản offline cùng bộ chấm: F1 z-score 0,4306 → **0,4236** (−0,0070; 77 thay vì 79 alert đúng), EWMA 0,3398 → 0,3377. Nguyên nhân xác định: **544 sự kiện bị bỏ vì trễ** làm chuỗi trạng thái lệch ở đúng các vị trí đó ⇒ ghi thành kết luận "giá phải trả của quy tắc không chờ", không phải sai số.
  - `sent_rate_ev_s = null`, `sustainable = false`, `median_last30_ms = NaN` trong `results/summary/rq2.csv`: producer gửi xong ở giây ~13 còn cửa sổ đo bắt đầu sau warm-up 30 s ⇒ 30 s cuối **không còn sự kiện**. Đã ghi cảnh báo "chỉ dùng `lat_*`/chất lượng ở RQ2, thông lượng–bền vững lấy từ RQ1" trong `handover_B` 2.2b và `report_B_draft` 5.5 + giới hạn 8.10.
- `scripts/aggregate_quality.py` (**mới**): gộp `<rq>_<engine>_<cfg>_r*_quality.csv` thành 1 dòng / (engine, detector) = trung bình các lần, thêm `<cột>_std`, `n_reps`, `runs`; ghi `results/summary/rq2_spark_replay_quality.csv`. Lý do phải có: `plot_results.py::load_quality` bỏ trùng theo `(engine, detector)` với `keep="first"` theo thứ tự ABC, nên nếu chỉ có file từng lần thì hình chỉ phản ánh **r1**; tên file gộp (`..._quality.csv`, `q` < `r`) đứng trước `..._r1_quality.csv` nên hình vẽ **trung bình 3 lần**.
- **Bốn lỗi ẩn của `scripts/create_topics.sh`** (đều làm `run_experiment.sh` dừng ở bước **[2/7]**):
  1. Gọi thẳng `python3`: Git Bash trên Windows chỉ có **alias Microsoft Store** ⇒ `Python was not found…` + exit 1 ⇒ `set -e` dừng script. Nay chọn Python như `run_experiment.sh` (`.venv/Scripts/python.exe` → `.venv/bin/python` → `python3`).
  2. `[ "$left" = "0" ] && break` dưới `set -e`: khi Kafka **chưa** xoá xong topic (xoá bất đồng bộ) thì cả pipeline trả mã 1 ⇒ script thoát **im lặng**. Nay `if … then break; fi` và báo lỗi rõ nếu quá 30 s.
  3. Thiếu `export MSYS_NO_PATHCONV=1`: khi gọi trực tiếp, `/opt/kafka/bin/...` bị dịch thành `C:/Program Files/Git/opt/...`.
  4. `kafka-topics.sh --describe --topic "sensor|alerts"`: `--describe` **không** nhận regex ⇒ báo "topic does not exist"; nay `--describe` riêng từng topic.
- `scripts/run_experiment.sh`: `REPLAY_SPEED` mặc định **2000** (producer `--mode replay` bắt buộc có `--speed`, trước đây thiếu nên bước 5 lỗi argparse).
- `spark_job/job.py`: `--test-parquet-spark` nhận **thư mục** nhiều `part-*.parquet` (mỗi file một khối `seq` liên tiếp) + `--max-files-per-trigger`; `tests/test_spark_multibatch_late.py` (mới) dùng hai tham số này để tạo **20 micro-batch**.
- Đã kiểm tra bằng:
  - `python scripts/aggregate_quality.py --engine spark --cfg replay --reps 3` → bảng trung bình 3 lần (z-score F1 0,4236, delay trung vị 28 652 ms; EWMA F1 0,3377, delay 28 940 ms), `AGG_EXIT=0`.
  - `python scripts/plot_results.py` → `[plot] RQ2: 6 phương pháp` (hst, mp, zscore, ewma offline + spark/zscore + spark/ewma), 4 file PNG/PDF trong `results/plots/`; `python scripts/plot_results.py --check` → **CHECK OK**.
  - `bash scripts/create_topics.sh` → exit 0, `--describe` xác nhận **6 partition** cho cả `sensor` và `alerts`.
  - `docker run … p3-spark:4.1.3 python3 -m tests.test_spark_multibatch_late` → 20 micro-batch, `late_dropped=544` = tham chiếu, 1325 alert + 188 probe, `only=0/0`, `score_mismatch=0`, `max_diff=0.00e+00`, in `SPARK_MULTIBATCH_LATE_OK`; bằng chứng `results/raw/rq0_spark_multibatch_late/`.
- Đã sửa gì so với AI:
  - Giữ **cùng `REPLAY_SPEED=2000`** cho cả 3 lần (không đổi giữa chừng) để 3 lần so sánh được với nhau; **không** chỉnh tay file trong `results/raw/` (đúng README 10.4) — sai thì chạy lại với số `r` mới.
  - **Không** thêm cột chất lượng vào `rq2.csv` (file đó là hiệu năng, do `measure_perf.py` ghi); chất lượng để ở `*_quality.csv` + file gộp riêng.
  - `scripts/aggregate_quality.py`: loại cột **bool** (`has_recv_ts_ms`) khỏi phần lấy trung bình (bản đầu biến nó thành `1.0` + cột `_std` vô nghĩa).
  - Ghi lại **cảnh báo burst** thay vì chỉ báo con số đẹp: 19 568 sự kiện/11 s ⇒ độ trễ RQ2 là thời gian xả hàng đợi, muốn đo độ trễ ổn định phải giảm `REPLAY_SPEED` (~110 sự kiện/giây cho 180 s).
- Cập nhật tài liệu: `handover_B` §1.3 (thêm `scripts/aggregate_quality.py`), **§2.2b mới** (bảng RQ2 streaming + cách đọc số), §3.8 (4 lỗi `create_topics.sh`), §5 (lệnh gộp chất lượng), §6 việc 4 (RQ2 Spark xong, còn Flink); `report_B_draft` **§5.5 mới** + giới hạn **8.10**; hình `results/plots/rq2_quality_comparison.{png,pdf}` vẽ lại với 6 nhóm cột.
- Chưa làm: `rq2 flink replay` (cần jar connector Flink Kafka bị `.gitignore`), RQ1/RQ3 của Spark; chưa chạy lại RQ2 với `REPLAY_SPEED` thấp để đo độ trễ ổn định.


### Bổ sung cùng ngày (26/10/2026) — xác nhận **state store thật** của bài test nhiều micro-batch

- Yêu cầu chính: mục 6.3 handover_B ghi rõ "Spark Structured Streaming (`applyInPandasWithState` thật với **RocksDB state store**)", nhưng lần chạy trước chỉ dựa vào `build_spark()` (tự fallback provider mặc định khi thiếu native lib) và **bằng chứng không ghi lại đã dùng provider nào** ⇒ chưa chứng minh được phần "trên RocksDB".
- `spark_job/job.py::run_test_parquet_spark`: đọc `spark.conf.get("spark.sql.streaming.stateStore.providerClass", "default")` **sau** `build_spark()`, in ra và trả thêm `state_store` (`rocksdb`/`default`) + `state_store_provider` trong `info` ⇒ `check.json` có bằng chứng, không chỉ log.
- `tests/test_spark_multibatch_late.py`: in `state_store` ở dòng kết quả, ghi vào `check.json`, đưa vào điều kiện `ok` và thêm **assert cứng** `state_store == "rocksdb"` (chạy ngoài image `p3-spark` không có native lib sẽ **fail rõ ràng** thay vì lặng lẽ pass với provider mặc định — đúng mục 3.8).
- Chạy lại thật (`docker run … p3-spark:4.1.3 python3 -m tests.test_spark_multibatch_late`): `state store: RocksDB (org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider)`; 20 file → **20 micro-batch**; 1325 alert + 188 probe; `late_dropped = 544` = tham chiếu; `only_ref = 0`, `only_spark = 0`, `score_mismatch = 0`, `max_diff = 0.00e+00`; in `SPARK_MULTIBATCH_LATE_OK`; `check.json` có `state_store = rocksdb`, `ok = true`.
- Thư mục tạm `checkpoints/_multibatch_check` và `checkpoints/_equiv_check` đã bị xoá (trong `finally` của `run_test_parquet_spark` + cuối test); `checkpoints/` chỉ còn `_smoke` của bài smoke streaming trước đó (đã gitignore).


