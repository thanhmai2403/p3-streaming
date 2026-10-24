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

