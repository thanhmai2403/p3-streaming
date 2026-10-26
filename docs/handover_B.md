# Bàn giao của Mẫn (Spark + dữ liệu), ngày 25/10/2026

Người viết: B (Mẫn). Người nhận: A (Mai).
Repo: https://github.com/thanhmai2403/p3-streaming — nhánh `b/data`, PR **#1** (`b/data` → `main`).

**Tóm tắt (cập nhật 25/10/2026, sau khi chạy thật trên máy B — commit `493e6da`):**
- Đã xong **Ưu tiên 0, Ưu tiên 1** (handover_A mục 4) và thêm SOTA **Half-Space Trees**. PR **#1** (`b/data` → `main`).
- **Tương đương qua file đã qua**: Spark `applyInPandasWithState` khớp **759/759** alert với bản tham chiếu (`max_diff = 0.00e+00`), so theo tập (`seq`, `detector`) đúng README mục 9. **Tương đương qua Kafka thật: ĐÃ QUA CỔNG** — `rq0_spark_equiv_r1` chạy trên Docker thật (Kafka + Spark standalone 2 worker) với **`COMPOSE_FILE=docker-compose.yml:docker-compose.lowmem.yml`, `SPARK_EXECUTOR_MEMORY=1024m`, `SPARK_DRIVER_MEMORY=512m`** (đúng ràng buộc `mem_limit` 1300m của `spark-master`, mục 3.5) ra **PASS `759/759` alert**, `max_diff = 0.00e+00`, `probe = 100`, `bad = 0`, `dup_seq = 0`, `missing_seq = 0`, `clock_ok = true`; bằng chứng: `results/raw/rq0_spark_equiv_r1/{alerts.jsonl,latency.csv,meta.json,summary.json,measure.log,sent.csv,docker_stats.csv}` + dòng `rq0_spark_equiv_r1` trong `results/summary/rq0.csv`. Còn lại: chưa so với Flink.
- **Nhiều micro-batch + dữ liệu trễ: ĐÃ KIỂM TRA (26/10/2026)** — `python -m tests.test_spark_multibatch_late`: 19 568 sự kiện cắt thành **20 file** (mỗi file 978–979 sự kiện liên tiếp theo `seq`) đọc với `maxFilesPerTrigger=1` → **20 micro-batch** thật (state phải lưu/đọc lại từ RocksDB giữa các batch), phát lại đúng thứ tự của producer với `--late-fraction 0.05 --late-shift 1 4` → **1325 alert + 188 probe**, `late_dropped = 544` **bằng đúng** bản tham chiếu, `only_ref = 0`, `only_spark = 0`, `score_mismatch = 0`, `max_diff = 0.00e+00`, `state_store = rocksdb` (`RocksDBStateStoreProvider`). Bằng chứng: `results/raw/rq0_spark_multibatch_late/{ref_alerts.jsonl, spark_alerts.jsonl, check.json}`.
- **Bảng chất lượng đã chấm lại trên toàn bộ 19 568 mẫu** (bản cũ chấm baseline trên `equiv_10k` nên Recall bị thấp giả). F1: `hst` 0,4476 ≈ `zscore` 0,4306 > `ewma` 0,3398 > `mp` 0,0635. HST và z-score chênh chỉ 0,017 với 90 nhãn, một lần chạy, nên **chưa kết luận hơn kém**.
- **Máy B không đủ làm máy đo RQ1/RQ3** (mục 3.5): RAM máy 14,176 MB, WSL tối đa 8,7 GB, cấu hình đo cần ~11,2 GB. Đề xuất máy A là máy đo (mục 4.1). Máy B chạy `rq0`, `rq2` với `docker-compose.lowmem.yml`.
- Docker đã chạy được trên máy B (WSL integration bật qua `settings-store.json`). Chưa có số liệu RQ1/RQ3 của Spark.

---

## 1. B đã hoàn thành

### 1.1 Dữ liệu

| File | Nội dung |
| --- | --- |
| `data/download_openaq.py` | Tải PM2.5 12 tháng (`2025-10-23` → `2026-10-22`) từ S3 OpenAQ theo `locationid/year/month`; làm sạch + **resample về 1 mẫu/giờ** (`floor` về đầu giờ UTC rồi `mean`); ghi `cleaning_report.json` |
| `data/processed/openaq_hn_injected.parquet` | **19 568** mẫu/giờ của 3 trạm (`hn_01` 3 439, `hn_02` 8 055, `hn_03` 8 074), đã chèn 90 bất thường; 4 cột `station_id, param, value, event_time_ms`. Đã ghi lại bằng `pyarrow 19.0.0` để `pd.read_parquet` của A đọc được (mục 3.2) |
| `data/processed/labels.csv` | **90 nhãn**: `level_shift` 33, `spike` 32, `drift` 25; đúng 30/trạm, `seed = 42`, `min_gap = 48` mẫu (= 48 giờ) |
| `data/processed/equiv_10k.parquet` | 10 000 sự kiện đầu theo thời gian (đã `git add -f` vì `.gitignore` chặn `*.parquet`) |
| `data/processed/stations.csv`, `cleaning_report.json` | Bảng tra `hn_01..03` → `location_id` (2161292 / 4946813 / 6123215) và số dòng bị bỏ ở từng bước |
| `data/processed/ref_alerts.jsonl`, `ref_alerts_full.jsonl` | Output bản tham chiếu: **759** alert (`equiv_10k`) và **1 356** alert (toàn bộ dữ liệu, có probe) |
| `data/inject_anomalies.py` | Chèn `spike` (+U(4,6)·σ, 1 mẫu), `level_shift` (+3σ, 6–24 mẫu), `drift` (dốc 0→3σ, 24–48 mẫu) |

Quy tắc gộp giờ đã được ghi chi tiết vào **README 7.1** (commit này), gồm bảng tần suất gốc: `hn_01` 60 phút, `hn_02` **5 phút** (→ gộp 12:1), `hn_03` 60 phút.

### 1.2 Spark

| File | Nội dung |
| --- | --- |
| `spark_job/job.py` | `readStream kafka` → `from_json` → `groupBy(station_id, param)` → **`applyInPandasWithState`** (z-score cửa sổ 24 + EWMA + bỏ trễ 2 h) → `writeStream kafka`. `sort_values("seq", kind="stable")` trong mỗi nhóm, `GroupStateTimeout.NoTimeout`, **không** `withWatermark`, `outputMode("append")`, probe `seq % 100 == 0`, key bản ghi ra = `station_id` |
| `spark_job/job.py` (chế độ test) | `--test-parquet` (chạy đúng hàm xử lý, không cần Spark), `--test-parquet-spark` (**applyInPandasWithState THẬT** trên file/thư mục parquet nguồn, `trigger(once=True)` hoặc `--max-files-per-trigger N` để tạo nhiều micro-batch, sink JSON; in + ghi lại **state store thực tế** — `state_store = rocksdb` khi có native lib) — công cụ của cổng tương đương |
| `docker/Dockerfile.spark` | `apache/spark:4.1.3` (Java 17, Python 3) + `pandas==2.2.3` + `pyarrow==19.0.0` + `pyyaml`; bake `common/`, `spark_job/`, `config.yaml` vào `/opt/p3` |
| `docker-compose.yml` (phần B) | `spark-master` **0,5 CPU / 1 200 m** + `spark-worker-1/2` **2 CPU / 3 500 m mỗi cái** → **tổng 4 core**, y hệt `flink-jobmanager` + 2 `flink-taskmanager`; volume `spark-checkpoints`; `SPARK_WORKER_CORES=2`, `SPARK_WORKER_MEMORY=3072m`; Kafka connector `spark-sql-kafka-0-10_2.13:4.1.3` |
| `spark.checkpoint_dir` | `/checkpoints/spark/<run_id>`, `spark.sql.shuffle.partitions = 4` (đọc từ `config.yaml`) |

### 1.3 Đo lường, SOTA và script

| File | Nội dung |
| --- | --- |
| `common/detectors_ref.py` | Bản tham chiếu Python thuần cho z-score/EWMA/quy tắc trễ; CLI `--with-probes`, `--engine`; đọc tham số từ `config.yaml` |
| `measure/compare_outputs.py` | So **tập (`seq`, `detector`)** + `score` lệch < 1e-6 giữa tham chiếu / Flink / Spark; có `--input` (tự chạy bản tham chiếu) và `--ref-file/--a-file/--b-file` |
| `measure/measure_quality.py` (v2) | Cửa sổ `[start_ms, end_ms + 1 h]`, gộp theo `(engine, detector)`, detection delay = `recv_ts_ms − ingest_ts_ms`; chịu được `seq`/`ingest_ts_ms`/`emit_ts_ms = null` và thiếu `recv_ts_ms` |
| `sota/half_space_trees.py` | Pipeline `MinMaxScaler` → `HalfSpaceTrees(n_trees=25, height=15, window_size=250, seed=42)`, `score_one` trước `learn_one`, ngưỡng phân vị 99 % trên 20 % đầu, warm-up 24, `engine="offline"`, `detector="hst"` |
| `producer/producer.py` (`--mode replay`) | Phát lại parquet theo `--speed`, `--late-fraction` / `--late-shift a b`, chia trạm theo `--producer-id/--n-producers`, `seq = producer_id·10^12 + đếm`, `ingest_ts_ms` gán **lúc gửi**, ghi `sent.csv`, có `--dry-run` |
| `scripts/run_experiment.sh` | Đúng **7 bước** của handover_A mục 4 (check_clock → dừng engine kia + tạo topic + xoá checkpoint → submit → measure nền → producer → kill worker ở giây 60 cho rq3 → cancel + summarize + chấm chất lượng + check_clock lần cuối). `run_id = <rq>_<engine>_<config>_r<n>` |
| `scripts/plot_results.py` | 3 hình cho báo cáo (`rq2_quality_comparison`, `rq1_throughput_latency`, `rq3_fault_recovery`), PNG 300 DPI + PDF, có `--check`/`--demo` |
| `scripts/aggregate_quality.py` | Gộp các lần lặp `<rq>_<engine>_<cfg>_r*_quality.csv` thành **1 dòng / (engine, detector)** = trung bình các lần, kèm `<cột>_std`, `n_reps`, `runs`; ghi `results/summary/<rq>_<engine>_<cfg>_quality.csv` để `plot_results.py` vẽ trung bình thay vì chỉ lần r1 (nó bỏ trùng theo thứ tự tên file) |

---

## 2. Kết quả kiểm thử và bảng chấm chất lượng

### 2.1 Kiểm thử (chạy được **không cần Docker**, `PYTHONPATH=. python -m tests.<tên>`)

| Test | Kết quả | Số liệu |
| --- | --- | --- |
| `test_detector` | **PASS** | `events=1200 alerts=95 (zscore=45, ewma=50)`, `max abs(score diff) = 2.80e-12` |
| `test_producer` | **PASS** | rate: 3 producer × 2 999 sự kiện (mục tiêu 3 000); replay lùi 118/1 200 (~10 %) |
| `test_measure` | **PASS** | p50 66 ms / p99 13 795 ms, `dup_seq=100`, `missing_seq=1`, `recovery_s=25.0`, cảnh báo đồng hồ đúng |
| `test_mp` | **PASS** | 3 biến thể Matrix Profile (offline, `--no-normalize`, `--causal`) chạy được trên chuỗi giả |
| `compare_outputs.py` (offline vs offline) | **TƯƠNG ĐƯƠNG** | `n=759/759, only=0/0, score_mismatch=0, max_diff=0.00e+00` |
| `spark_job/job.py --test-parquet-spark` → `compare_outputs.py` | **TƯƠNG ĐƯƠNG** | ref 759 = spark 759 (zscore 348, ewma 411), `max_diff=0.00e+00`, `late_dropped=0` |
| `test_spark_multibatch_late` | **PASS** | 20 file (`maxFilesPerTrigger=1`) → **20 micro-batch**, `state_store = rocksdb` (`RocksDBStateStoreProvider`), 1325 alert + 188 probe, `late_dropped = 544` = tham chiếu, `only=0/0`, `score_mismatch=0`, `max_diff=0.00e+00` (cần Java 17 của image `p3-spark`, xem mục 3.8) |
| Smoke streaming local (2 pha, checkpoint recovery) | **PASS** | pha 1: 244 bản ghi (162 alert + 82 probe); pha 2 restart từ checkpoint: +129 bản ghi |

### 2.2 Bảng chấm SOTA / baseline — `results/summary/rq2_offline_quality.csv`

Cùng một bộ chấm (`measure/measure_quality.py`, cửa sổ `[start_ms, end_ms + 1 h]`, `labels.csv` 90 nhãn). **Mọi phương pháp chạy trên cùng toàn bộ 19 568 mẫu** (`openaq_hn_injected.parquet`; `ref_alerts_full.jsonl` cho z-score và EWMA):

| engine | detector | alert | alert trong cửa sổ | phát hiện | Precision | Recall | F1 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `offline` | `hst` (SOTA của B) | 116 | 85 | 29/90 | **0,7328** | 0,3222 | **0,4476** |
| `offline` | `zscore` (bản tham chiếu) | 589 | 168 | 79/90 | 0,2852 | 0,8778 | 0,4306 |
| `offline` | `ewma` (bản tham chiếu) | 767 | 157 | 90/90 | 0,2047 | **1,0000** | 0,3398 |
| `offline` | `mp` (SOTA của A) | 162 | 8 | 8/90 | 0,0494 | 0,0889 | 0,0635 |

Đọc bảng:
- **HST** báo ít nhưng đúng (P cao nhất), bỏ sót nhiều (R thấp). **z-score** cân bằng. **EWMA** bắt đủ 90/90 nhưng báo thừa nhiều nhất.
- F1 của HST và z-score chỉ chênh 0,017; với 90 nhãn và một lần chạy xác định, nên nói **tương đương F1, khác hồ sơ đánh đổi**, không nói HST tốt hơn.
- Precision đếm theo **alert**, không theo sự kiện: bất thường dài (drift 24-48 mẫu) có thể sinh nhiều alert liên tiếp nên P của detector "bật liên tục" bị thổi lên. MP có bỏ 24 mẫu sau mỗi alert (`sota.mp.refractory`) nên điều kiện không hoàn toàn giống nhau.
- Cả 4 dòng đều `has_recv_ts_ms = False` nên cột `detection_delay_*` để trống (SOTA offline). Dòng **streaming** (có `recv_ts_ms`) nằm ở **mục 2.2b** dưới đây.
- Lưu ý khi chấm: **không** chấm baseline trên `equiv_10k.parquet` (chỉ 10 000 mẫu đầu) với đủ 90 nhãn — nhãn nằm ngoài phần đó bị tính là bỏ sót. `equiv_10k` chỉ dùng để so Flink với Spark.

### 2.2b RQ2 qua Kafka trên Spark — 3 lần lặp (`rq2_spark_replay_r1/r2/r3`)

Chạy `bash scripts/run_experiment.sh rq2 spark replay <n>` trên máy B với `docker-compose.lowmem.yml`
(`SPARK_EXECUTOR_MEMORY=1024m`, `SPARK_DRIVER_MEMORY=512m`), `REPLAY_SPEED=2000` = **2 000 sự kiện/giây**
⇒ 19 568 sự kiện vào Kafka trong **~11 s**, 5 % sự kiện bị làm trễ 1–4 vị trí (README 6.3), 6 partition,
không kill worker. Cùng bộ chấm với mục 2.2 (cửa sổ `[start_ms, end_ms + 1 h]`, `labels.csv` 90 nhãn).

| engine | detector | alert | alert trong cửa sổ | phát hiện | Precision | Recall | F1 | delay trung vị (ms) | delay p95 (ms) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `spark` | `zscore` | 572 | 161 | 77/90 | 0,2815 | 0,8556 | **0,4236** | 28 652 | 32 597 |
| `spark` | `ewma` | 753 | 153 | 90/90 | 0,2032 | **1,0000** | 0,3377 | 28 940 | 32 595 |

Số trong bảng là **trung bình 3 lần lặp** (`scripts/aggregate_quality.py` → `results/summary/rq2_spark_replay_quality.csv`;
từng lần ở `results/summary/rq2_spark_replay_r<n>_quality.csv`).

Đọc bảng:
- **Chất lượng lặp lại tuyệt đối**: cả 3 lần cho **cùng** P/R/F1 (chỉ độ trễ đổi: trung vị p50 của run = 27 740 / 25 584 / 30 987 ms). Luồng sự kiện và quy tắc trễ là **xác định**, nên 3 lần lặp ở RQ2 dùng để lấy **phân bố độ trễ**, không phải để lấy trung bình chất lượng.
- So với bản offline cùng bộ chấm (mục 2.2): z-score F1 0,4306 → **0,4236** (−0,0070, 77 thay vì 79 alert đúng), EWMA 0,3398 → **0,3377**. Chênh lệch **có nguyên nhân xác định**: 544 sự kiện bị bỏ vì trễ (quy tắc README 6.3, không chờ) làm chuỗi trạng thái (z-score cửa sổ 24, EWMA) lệch đi ở đúng những chỗ đó ⇒ mất 2 alert z-score. Đây là **hệ quả của quy tắc trễ**, không phải sai số đo.
- `dup_seq = 0` cả 3 lần (không nhân bản ở `AT_LEAST_ONCE` với tải này); `missing_seq = 8/196` (**4,1 %** probe). Probe nào **rơi đúng vào sự kiện bị bỏ trễ** thì không bao giờ tới sink — cùng bản chất `late_dropped`, không phải mất dữ liệu ngoài dự kiến. Đây cũng là lý do **không** dùng `missing_seq` của RQ2 làm chỉ số tính đúng đắn (chỉ dùng cho RQ0/RQ3).
- `clock_ok = true` cả 3 lần, kiểm tra đồng hồ trước **và** sau khi đo ⇒ số độ trễ dùng được.
- **Cảnh báo khi đọc `results/summary/rq2.csv`**: `sent_rate_ev_s` trống và `sustainable = false` **không** có nghĩa "Spark không bền vững". Producer gửi xong ở giây ~13, còn cửa sổ đo bắt đầu sau warm-up 30 s ⇒ **30 s cuối không còn sự kiện** (`median_last30_ms = NaN` ⇒ `drift_ok = False` ⇒ `sustainable = False`). Thông lượng và tính bền vững **chỉ lấy từ RQ1**; ở RQ2 chỉ dùng `lat_*`, `dup_seq`, chất lượng.
- Độ trễ 25–31 s (trung vị mỗi run) là **thời gian xả hàng đợi**, không phải độ trễ trạng thái ổn định: 19 568 sự kiện dồn vào Kafka trong ~11 s ⇒ ~640–750 sự kiện/giây thực tế, vượt năng lực 2 worker × 2 core ở cấu hình lowmem. Muốn đo độ trễ ổn định cho RQ2 thì **giảm `REPLAY_SPEED`** để replay trải hết cửa sổ 180 s (A quyết; 3 lần hiện tại dùng cùng 2 000 nên so sánh được với nhau).
- Bằng chứng: `results/raw/rq2_spark_replay_r{1,2,3}/` (`alerts.jsonl`, `latency.csv`, `summary.json`, `meta.json`, `sent.csv`, `docker_stats.csv`, `timeseries_1s.csv`), `results/summary/rq2.csv` (3 dòng), `results/summary/rq2_spark_replay_r{1,2,3}_quality.csv`, `results/summary/rq2_spark_replay_quality.csv`, hình `results/plots/rq2_quality_comparison.{png,pdf}` — nay **6 nhóm cột**: `hst`, `mp`, `zscore`, `ewma` (offline) + `spark/zscore`, `spark/ewma`.

### 2.3 Vì sao Matrix Profile của A ra F1 = 0,0635 (A quyết)

Đây **không phải** lỗi bộ chấm:

1. **Số alert không ít** (162) nhưng chỉ **8** rơi vào cửa sổ nhãn: alert nằm ở chỗ khác chứ không bị mất.
2. **Lệch thời điểm**: trung vị khoảng cách từ alert tới `start_ms` nhãn là 123 giờ trước nhãn; chỉ 19/90 nhãn có alert trong 24 giờ trước `start_ms`. MP đang báo ở các *discord tự nhiên* của PM2.5.
3. **Ngưỡng** lấy phân vị 99% trên 20% dữ liệu đầu (`sota.calib_fraction = 0,2`, `sota.threshold_quantile = 0,99`), có thể không bám được phân bố ở mùa khác.
4. `refractory = 24` chỉ bỏ 24 mẫu sau mỗi alert, ngắn hơn đoạn drift (tới 48 mẫu).

Gợi ý (A quyết, B không đổi tham số của A): thử `--causal` (stumpi) và `--no-normalize` để so sánh. **Không chỉnh ngưỡng dựa trên `labels.csv`** để tăng F1 (như vậy là học thuộc đề); nếu đổi quy tắc ngưỡng thì áp dụng cùng cách cho HST và ghi vào Giới hạn.

---

## 3. Phát hiện kỹ thuật quan trọng phía B

### 3.1 Cặp `pandas==2.2.3` + `pyarrow==19.0.0` cho PySpark 4.1.3

PySpark 4.1.3 + **`pyarrow 26`** làm chết pandas UDF của Spark (`applyInPandasWithState`):

```
TypeError: 'float' object cannot be interpreted as an integer
  at pyarrow.lib.RecordBatch.slice (worker pandas UDF)
```

Đã kiểm chứng riêng: `pandas 3.1.0 + pyarrow 26` lỗi, `pandas 2.3.3 + pyarrow 26` lỗi, `pandas 2.3.3 + pyarrow 21` và `pandas 3.1.0 + pyarrow 21` chạy đúng ⇒ **thủ phạm là pyarrow 26**, không phải pandas 3. Nhóm chọn cặp `pandas 2.2.3 + pyarrow 19.0.0` (khớp `docker/Dockerfile.spark`). **Đừng nâng cấp hai gói này khi chưa thử lại UDF.** Cần thêm: worker phải dùng đúng Python có pyarrow (`PYSPARK_PYTHON`), nếu không báo `No module named 'pyarrow'`.

### 3.2 `pyarrow 19` không đọc được parquet do JVM (Spark) ghi

`pd.read_parquet` báo `OSError: Repetition level histogram size mismatch` với `equiv_10k.parquet` và `openaq_hn_injected.parquet` (file do Spark/pyarrow bản mới ghi). Cách sửa đã dùng: đọc bằng **JVM** (`spark.read.parquet`, `coalesce(1)` để giữ nguyên thứ tự dòng vật lý), ghi lại bằng chính `pyarrow 19.0.0`, `assert_frame_equal` khớp 100 % rồi mới `os.replace`. **A cần biết:** nếu A ghi lại 2 file này bằng pyarrow ≥ 26 thì `pd.read_parquet` trên máy B lại hỏng; quy ước là **file parquet chia sẻ phải ghi bằng pyarrow 19.0.0**.

### 3.3 Lỗi `drop_duplicates` khi `seq = null` (đã sửa trong `measure_quality.py`)

Bản `measure_quality.py` đầu tiên dedupe theo `["engine", "detector", "seq"]`. Output của `matrix_profile.py`/`half_space_trees.py` có `seq = null`, và pandas coi **mọi NaN là bằng nhau** ⇒ **162 alert MP bị gộp còn 1 dòng**, F1 tụt về gần 0 và số alert trong bảng sai. Nay dedupe theo `(engine, detector, station_id, param, event_time_ms, round(score, 9))` — không phụ thuộc `seq`. Nếu A viết script chấm riêng thì tránh dedupe theo cột có thể null.

### 3.4 PySpark 4.1.3: 5 điểm API dễ sai (đã xác minh trên bản cài thật)

| Điểm | Thực tế |
| --- | --- |
| `GroupStateTimeout.NoTimeout` | Là **chuỗi**, không phải callable → viết `GroupStateTimeout.NoTimeout`, **không** `NoTimeout()` |
| `state.get` | Là **property** trả **tuple** đúng thứ tự `STATE_STRUCT` (không phải `Row`) → đọc theo chỉ số |
| `state.update` | Nhận **tuple**, không nhận `Row` |
| `DataFrame.applyInPandas` | **Đã bỏ** ở 4.1 → dùng `mapInPandas` |
| Restart từ checkpoint | **Không** hỗ trợ với memory sink (`This query does not support recovering from checkpoint location`) → test recovery phải dùng file sink (parquet/JSON) |

Thêm: file source streaming **không tự suy schema** (`Schema must be specified...`) → phải truyền `schema=` tường minh (đúng như `--test-parquet-spark` làm).

### 3.5 Thông số máy B và vì sao **không** làm máy đo

| | Máy B |
| --- | --- |
| CPU | AMD Ryzen 5 5600U (theo Cline), WSL `nproc = 12` (chưa giới hạn) |
| RAM | **14 176 MB** (`systeminfo`) |
| OS | Windows 11 Home + WSL2 Ubuntu |
| Giới hạn WSL (`.wslconfig`) | `memory=9GB`, `processors=8`, `swap=4GB` → `MemTotal` ≈ **8,7 GiB** |
| Docker | Docker Desktop + Engine + Compose v2 (giao diện không mở được, dùng qua terminal trong WSL) |

Cấu hình đo trong `docker-compose.yml` (Kafka 3 GB + engine 1,2 + 2×3,5 GB) cần **~11,2 GB** cho RAM container. WSL máy B đã chốt `memory=9GB` (không nâng lên 10-11 GB, vì khi đó Windows, Docker Desktop và VS Code chỉ còn ~3 GB; RAM bị ép làm container bị kill hoặc làm méo độ trễ). Vì vậy **máy B không làm máy đo RQ1/RQ3**.

Máy B dùng `docker-compose.lowmem.yml` (Kafka 1,5 GB, mỗi engine ~5,4 GB, tổng ~6,9 GB) **chỉ cho `rq0` (tương đương) và `rq2` (chất lượng)**. Số liệu chạy với override này **không dùng để so sánh hiệu năng** (RQ1/RQ3). Khi chạy Spark với override phải đặt `SPARK_EXECUTOR_MEMORY=1024m SPARK_DRIVER_MEMORY=512m` (driver 512m trong `mem_limit` 1300m của `spark-master`, `SPARK_DAEMON_MEMORY=256m`) và `COMPOSE_FILE=docker-compose.yml:docker-compose.lowmem.yml` (trên Git Bash của Windows dùng dấu `;`: `"docker-compose.yml;docker-compose.lowmem.yml"`). Cấu hình này **đã kiểm chứng bằng hai lần chạy thật**: `rq0_spark_equiv_r1` (PASS 759/759) và `tests/test_spark_multibatch_late` (PASS, mục 2.1).

### 3.6 Ba bẫy bash khi viết `run_experiment.sh`

1. **`pipefail` + SIGPIPE**: `flink list -r | grep -q ...` làm `grep` thoát sớm, `flink` nhận SIGPIPE (141), `pipefail` khiến cả pipeline trả 141 ⇒ hàm "job đang chạy" trả **sai**. Nay dùng `out=$(... || true)` rồi `[[ $out =~ [0-9a-f]{32} ]]`.
2. **CRLF**: repo đặt `core.autocrlf=true` nên file `.sh` trong working tree là CRLF; blob trong git là LF. Trong WSL phải chạy bản từ `git show` hoặc bật `git config core.autocrlf input` cho repo. Kiểm tra: `git show HEAD:scripts/run_experiment.sh | file -`.
3. **`docker compose stop`** engine kia phải chạy **trước** `up -d` engine mình, nếu không 2 engine cùng chiếm 4 core ⇒ mọi so sánh vô nghĩa (script đã làm đúng thứ tự này).

### 3.7 `measure_quality.py`: định nghĩa detection delay đang khác README 10.2

README 10.2: delay = `recv_ts_ms` của alert đầu tiên trong cửa sổ − `ingest_ts_ms` của **mẫu bất thường đầu tiên**.
Bản hiện tại dùng `ingest_ts_ms` **chép sẵn trong chính alert** (mẫu bị báo), nên không phải mẫu bất thường đầu tiên. Khi so Flink với Spark thì chênh lệch giữa hai engine vẫn đúng (cùng một cách tính), nhưng đây là độ trễ xử lý alert, không phải toàn bộ độ trễ phát hiện. **Phải ghi rõ điều này trong báo cáo**, hoặc thêm file `sent_events.csv` (`station_id,event_time_ms,ingest_ts_ms`) từ producer replay để tính đúng định nghĩa. `sent.csv` hiện tại là thống kê theo giây, không dùng được cho mục đích này.

### 3.8 Lỗi môi trường gặp khi chạy lần đầu trên máy B (A có thể gặp lại)

| Lỗi | Nguyên nhân và cách xử |
| --- | --- |
| Container Spark thoát ngay `Exited (0)` | Image `apache/spark` không có `CMD`; compose phải khai báo `command` chạy `spark-class ...Master` / `...Worker` ở nền trước (đã sửa trong `docker-compose.yml`) |
| `spark-submit --packages` báo `/nonexistent/.ivy2.5.2/cache` | Người dùng `spark` có HOME không tồn tại: thêm `--conf spark.jars.ivy=/tmp/.ivy2` |
| `check_clock.sh` báo lệch hàng nghìn tỉ ms | `date +%s%3N` trên Ubuntu mới in 19 chữ số (nano giây); dùng `$(( $(date -u +%s%N) / 1000000 ))`. Script đã sửa |
| `check_clock.sh` báo lệch ~100 ms dù đồng hồ đúng | Sai số đo (`docker compose exec` chậm) lớn hơn độ lệch; đã sửa tiêu chí: FAIL chỉ khi `|lệch| − sai số > ngưỡng`, lấy 5 mẫu tốt nhất |
| `pip install` `pandas==2.2.3` biên dịch lỗi | Python hệ thống của Ubuntu là 3.14, chưa có bản cài sẵn: tạo venv Python 3.11 bằng `uv venv --python 3.11` |
| `docker` không có trong Ubuntu | Bật WSL integration trong Docker Desktop (hoặc `EnableIntegrationWithDefaultWslDistro` + `IntegratedWslDistros` trong `settings-store.json`) rồi `wsl --shutdown` |
| Chạy `pyspark` trên **host Windows** (venv) báo `java.lang.UnsupportedOperationException: getSubject is not supported` khi đọc parquet | JDK của máy B là **Java 25**; Spark 4.1.3 (Hadoop 3.4.x) vẫn gọi `Subject.getSubject` — API đã bị **bỏ từ Java 24**. Không sửa được bằng cấu hình: chạy Spark **trong image `p3-spark:4.1.3` (Java 17)**, lệnh ở mục 5 (bước 1b). Các test *không cần Spark* (`test_detector`, `test_producer`, `test_measure`, `test_mp`) vẫn chạy trên host bình thường |
| `bash scripts/create_topics.sh` báo `Python was not found; run without arguments to install from the Microsoft Store…` rồi `run_experiment.sh` dừng ở bước **[2/7]** | Git Bash của Windows **không có `python3` thật**, chỉ có alias Microsoft Store trả mã lỗi; script cũ gọi thẳng `python3`. Đã sửa: chọn Python như `run_experiment.sh` (`.venv/Scripts/python.exe` → `.venv/bin/python` → `python3`) (mục 3.6 dòng 4) |
| `create_topics.sh` in `Xóa topic sensor` rồi **thoát im lặng** (exit 1), `run_experiment.sh` dừng ở bước **[2/7]** dù topic đã xóa | `set -e` + `[ "$left" = "0" ] && break`: khi topic **chưa** biến mất (Kafka xóa bất đồng bộ) thì cả list trả mã 1 ⇒ script thoát ngay. Đã sửa thành `if … then break; fi` và báo lỗi rõ nếu sau 30 s topic vẫn còn |
| `create_topics.sh` báo `…/opt/kafka/bin/kafka-topics.sh: No such file or directory` hoặc đường dẫn thành `C:/Program Files/Git/opt/...` | MSYS path conversion của Git Bash: đã `export MSYS_NO_PATHCONV=1` ngay trong script (trước đây chỉ có khi gọi qua `run_experiment.sh`) |
| `kafka-topics.sh --describe --topic "sensor\|alerts"` báo topic không tồn tại | `kafka-topics.sh` **không** nhận regex ở `--describe` (coi `sensor\|alerts` là tên topic theo nghĩa đen). Đã sửa: gọi `--describe` riêng cho từng topic |
| Trong container `p3-spark`, `python3 -m tests....` báo `ModuleNotFoundError: No module named 'pyspark'` | Image **không** `pip install pyspark`: pyspark nằm ở `/opt/spark/python` và compose chỉ đặt `PYTHONPATH=/opt/p3`. Phải tự thêm `/opt/spark/python`, `/opt/spark/python/lib/pyspark.zip`, `/opt/spark/python/lib/py4j-0.10.9.9-src.zip` (lệnh mục 5) hoặc chạy qua `/opt/spark/bin/spark-submit` |

---

## 4. Trả lời 5 câu hỏi mở ở Mục 7 của `handover_A.md`

### 4.1 Máy nào là máy đo?

**Đề xuất: máy A (Mai), nếu máy A có đủ RAM.** Máy B không đủ (mục 3.5). A vui lòng gửi `nproc` và `free -h` chạy trong WSL của máy A (cần ~12 GB RAM cho WSL, 8 core trở lên) để chốt.

**Quy tắc bắt buộc** (ghi vào báo cáo): cả Flink và Spark chạy **cùng một máy, cùng một phiên Docker, cùng giới hạn CPU/RAM** cho RQ1/RQ3. Máy B chạy `rq0` (tương đương), `rq2` (chất lượng; không phụ thuộc máy đo), phát triển và viết báo cáo, với `docker-compose.lowmem.yml`.

### 4.2 Chốt các mức tải RQ1

**Đồng ý khung 1 000 / 2 000 / 3 000 / 4 000 sự kiện/giây**, kèm 2 điều kiện:

1. **Chạy 1 vòng dò ngưỡng trước** (1 000 → 5 000, mỗi mức 60 s, không cần 3 lần lặp) để tìm mức mà p99 vượt 1 000 ms. Nếu **4 000 đã vượt** thì hạ khung xuống `500 / 1 000 / 2 000 / 3 000`; nếu **4 000 vẫn dưới** thì giữ khung và thêm `5 000` làm điểm bão hòa.
2. **Chỉ dùng run có `clock_ok = true`.** Dữ liệu `try5000` hiện có cho thấy vấn đề: producer gửi ~4 600 ev/s (`sent_rate`) nhưng thông lượng đo được chỉ **1 528–1 666 ev/s** ⇒ engine đã bão hòa/ùn ở mức dưới 5 000, và `r4` có `clock_ok = False` (19 probe có `emit < ingest`) nên **cả 4 run này không dùng được**, đúng như A kết luận.

### 4.3 Quy tắc gộp dữ liệu về giờ (README 7.1)

Đã ghi chi tiết trong commit này. Tóm tắt để A đối chiếu:

- 3 trạm: `hn_01` = location **2161292** (thay cho 2161306 chỉ có 1 525 mẫu ≈ 9 tháng), `hn_02` = 4946813, `hn_03` = 6123215. Tổng **19 568** dòng/giờ; `median_gap = 60 phút` ở **cả 3** trạm.
- Tần suất gốc: `hn_01` **60 phút**, `hn_02` **5 phút**, `hn_03` **60 phút**.
- Với trạm có tần suất > 1 mẫu/giờ: `floor` `event_time_ms` về **mốc đầu giờ UTC** (bội số 3 600 000; = 07:00 giờ Việt Nam) rồi lấy **trung bình cộng** các mẫu hợp lệ trong giờ đó. Yêu cầu **≥ 1 mẫu/giờ**; **không nội suy** khoảng trống.
- Làm sạch trước đó: bỏ `value` null (0 dòng) và `< 0` (57 dòng), bỏ timestamp trùng (0 dòng).

### 4.4 B có đồng ý các thay đổi ở mục 2 của `handover_A.md` không?

| Mục | Ý kiến của B |
| --- | --- |
| 2.1 `recv_ts_ms` do `measure_perf.py` gắn | **Đồng ý.** `measure_quality.py` dùng `recv_ts_ms − ingest_ts_ms`; thiếu `recv_ts_ms` thì cột delay = NaN, P/R/F1 vẫn tính |
| 2.2 Flink thread mode + `assign_timestamps: false` | **Đồng ý**, không ảnh hưởng Spark. **Nhưng** đây là cấu hình *không chuẩn* của Flink ⇒ phải ghi vào **Giới hạn**: kết quả Flink là "PyFlink ở thread mode", không đại diện Flink Java/event-time. Nếu còn thời gian, chạy **1 ablation** `assign_timestamps: true` + `python_execution_mode: process` để chứng minh kết luận không phụ thuộc cấu hình |
| 2.3 SOTA có `seq`/`ingest_ts_ms`/`emit_ts_ms = null` | **Đồng ý.** Đã sửa scorer để chịu được null (mục 3.3); MP vẫn bỏ 24 mẫu sau alert (`sota.mp.refractory`) |
| 2.4 `config.yaml` thêm `producer`, `measure`, `sota`, `flink.*` | **Đồng ý.** Spark và các script của B đọc đủ các khóa này; `common/config.py` có bảng `LEGACY_KEYS` để code cũ không vỡ |
| 2.5 Chế độ `replay` của A | **Đã thay bằng bản của B** (chia trạm theo `station_index % n_producers`, `--bootstrap-servers` là bí danh của `--bootstrap`, `--dry-run --output`), giữ nguyên ngữ nghĩa `--late-shift a b` của A. Lý do: bản của A không chia trạm được nên khi chạy nhiều producer, `seq` của một trạm có thể không tăng theo thứ tự đến |
| 2.6 Tên run `rq0_<engine>_equiv_r<n>` | **Đồng ý**, `parse_run_id` của A nhận đúng |

### 4.5 Phân việc khi máy A là máy đo

- **A** chạy RQ1/RQ3 cho **cả hai engine** bằng `scripts/run_experiment.sh` (hỗ trợ `flink` và `spark`, cùng 7 bước), dùng các mức tải A chốt và thứ tự `rq0` → `rq2` → `rq1` → `rq3`.
- **B** chạy `rq0` và `rq2` trên máy B (không cần đúng hạn mức tài nguyên), Half-Space Trees, và viết nháp Thảo luận/Giới hạn.
- Nháp phần Thảo luận/Giới hạn của B trong **`docs/report_B_draft.md`** (mục 8 và 9): kết quả "Flink có thông lượng cao nhất" của ShuffleBench là với **code Java**; với Python, PyFlink trả giá **cầu nối Java–Python ở từng bản ghi**, còn PySpark `applyInPandasWithState` xử lý theo **lô pandas**.

---

## 5. Lệnh nhanh phía Spark & đo chất lượng

```bash
# 0) chuẩn bị (chỉ 1 lần)
docker compose build spark-master spark-worker-1 spark-worker-2
docker compose up -d kafka spark-master spark-worker-1 spark-worker-2
bash scripts/create_topics.sh
bash scripts/check_clock.sh                     # phải OK mới đo

# 1) cổng tương đương (không cần Kafka, chạy trên host)
python spark_job/job.py --test-parquet-spark data/processed/equiv_10k.parquet \
    --output data/processed/spark_alerts.jsonl
python measure/compare_outputs.py --ref-file data/processed/ref_alerts.jsonl \
    --a-file data/processed/ref_alerts.jsonl --b-file data/processed/spark_alerts.jsonl \
    --ref offline --a offline --b spark
# -> "TƯƠNG ĐƯƠNG: n=759/759, max_diff=0.00e+00"

# 1b) nhiều micro-batch + dữ liệu trễ (BẮT BUỘC chạy trong image p3-spark: JDK host là Java 25,
#     Spark 4.1.3 không chạy được - xem mục 3.8). Bind-mount :rw vì test ghi results/ + checkpoints/.
docker run --rm -v "$PWD:/opt/p3" -w /opt/p3 \
  -e PYTHONPATH=/opt/p3:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
  p3-spark:4.1.3 python3 -m tests.test_spark_multibatch_late
# -> "20 micro-batch: 1513 bản ghi (1325 alert + 188 probe), late_dropped=544" + "SPARK_MULTIBATCH_LATE_OK"

# 2) một lần chạy thí nghiệm (script lo cả 7 bước)
bash scripts/run_experiment.sh rq0 spark equiv 1        # tương đương qua Kafka
bash scripts/run_experiment.sh rq1 spark rate2000 1     # đổi cấu hình theo mức A chốt
bash scripts/run_experiment.sh rq2 spark replay 1       # replay OpenAQ + late 5%
bash scripts/run_experiment.sh rq3 spark rate2000 1     # kill spark-worker-1 ở giây 60

# 3) SOTA Half-Space Trees + chấm chất lượng (không cần Docker)
python sota/half_space_trees.py --input data/processed/openaq_hn_injected.parquet \
    --output results/sota_hst/alerts.jsonl
python measure/measure_quality.py --labels data/processed/labels.csv \
    --alerts results/raw/rq2_offline_mp_r1/alerts.jsonl results/sota_hst/alerts.jsonl \
    --out results/summary/rq2_offline_quality.csv

# 3b) gộp 3 lần lặp RQ2 về 1 dòng/(engine, detector) để hình vẽ TRUNG BÌNH 3 lần
python scripts/aggregate_quality.py --engine spark --cfg replay --reps 3
python scripts/aggregate_quality.py --engine flink --cfg replay --reps 3

# 4) hình cho báo cáo
python scripts/plot_results.py                # -> results/plots/*.png, *.pdf
python scripts/plot_results.py --check        # kiểm tra code vẽ, không ghi đè

# 5) xem kết quả
python -c "import pandas as pd;print(pd.read_csv('results/summary/rq1.csv').to_string())"
docker compose exec spark-master ls /checkpoints/spark/<run_id>   # checkpoint RocksDB
```

---

## 6. Việc B làm tiếp (theo thứ tự)

1. ~~Chạy `rq0 spark equiv 1` qua Kafka thật~~ **ĐÃ XONG (25/10/2026)**: `rq0_spark_equiv_r1` với `docker-compose.lowmem.yml` (`COMPOSE_FILE=docker-compose.yml:docker-compose.lowmem.yml`, `SPARK_EXECUTOR_MEMORY=1024m`, `SPARK_DRIVER_MEMORY=512m`) → `TƯƠNG ĐƯƠNG 759/759`, `max_diff = 0.00e+00`, `probe = 100` (bằng chứng: `results/raw/rq0_spark_equiv_r1/`, `results/summary/rq0.csv`).
2. Chạy `rq0 flink equiv 1` (hoặc nhờ A) và so **ba bên**: tham chiếu = Flink = Spark.
3. ~~Kiểm tra Spark với **nhiều micro-batch** (`maxFilesPerTrigger=1`, ~20 file) và với **dữ liệu trễ** (`late_dropped > 0`, bằng với bản tham chiếu)~~ **ĐÃ XONG (26/10/2026)**: `tests/test_spark_multibatch_late` → 20 micro-batch trên **RocksDB state store** (`state_store = rocksdb`, `RocksDBStateStoreProvider`), `late_dropped = 544` = tham chiếu, 1325 alert + 188 probe khớp tuyệt đối (`only=0/0`, `score_mismatch=0`, `max_diff=0.00e+00`); bằng chứng `results/raw/rq0_spark_multibatch_late/check.json` (có trường `state_store`).
4. ~~Chạy `rq2 spark replay 1` và `rq2 flink replay 1` trên máy B (điền `results/summary/rq2.csv`)~~ **SPARK ĐÃ XONG (26/10/2026)**: `rq2_spark_replay_r1/r2/r3`, cả 3 `EXIT=0`, `clock_ok=true`; z-score F1 **0,4236**, EWMA F1 **0,3377** (giống nhau tuyệt đối ở 3 lần), delay trung vị 25,6–31,0 s; bằng chứng `results/raw/rq2_spark_replay_r{1,2,3}/`, `results/summary/rq2.csv`, `results/summary/rq2_spark_replay_quality.csv`, hình `results/plots/rq2_quality_comparison.{png,pdf}`; chi tiết + cảnh báo đọc số ở **mục 2.2b**. **Còn lại: `rq2 flink replay <n>`** (cần jar connector Flink Kafka, xem mục 6 dòng cuối).
5. Trả lời A về máy đo; nếu máy A đo thì B gửi A `scripts/run_experiment.sh` mới nhất và `docker-compose.lowmem.yml` chỉ để tham khảo.
6. Chạy `scripts/plot_results.py` sau mỗi đợt; viết tiếp `docs/report_B_draft.md`.

**A cần biết:** (a) `requirements-b.txt` đã xóa, chỉ dùng `requirements.txt`; (b) tham số MP cần A tinh chỉnh (mục 2.3), không dựa trên nhãn; (c) PR #1 chứa toàn bộ thay đổi; (d) mục 3.8 liệt kê các lỗi môi trường có thể gặp lại.

