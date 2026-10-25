# Bàn giao của Mẫn (Spark + dữ liệu), ngày 25/10/2026

Người viết: B (Mẫn). Người nhận: A (Mai).
Repo: https://github.com/thanhmai2403/p3-streaming — nhánh `b/data`, PR **#1** (`b/data` → `main`).

**Tóm tắt:**
- Đã xong **100% Ưu tiên 0, Ưu tiên 1** (handover_A mục 4) và thêm SOTA **Half-Space Trees**. Đã merge `origin/main` của A vào `b/data` và mở PR #1.
- **Cổng tương đương đã qua**: Spark `applyInPandasWithState` khớp **759/759** alert với bản tham chiếu, `max_diff = 0.00e+00` (`score_mismatch = 0`). So theo **tập (`seq`, `detector`) của riêng bản ghi `alert`**, đúng README mục 9 — không so số lượng.
- **Đã chấm xong** `results/summary/rq2_offline_quality.csv` cho cả 4 phương pháp: `hst` F1 **0,4476** > `zscore` 0,3642 > `ewma` 0,3244 > `mp` 0,0635. MP thấp do **tham số** của A (mục 2.3), không do bộ chấm.
- **Chưa chạy được Docker trên máy B** trong phiên này (không có Docker CLI trong PATH) → các lệnh submit Flink/Spark trong `run_experiment.sh` mới chỉ được kiểm tra cú pháp (`bash -n`), **chưa có số liệu RQ1/RQ3 của Spark**. Đây là việc đầu tiên cần làm khi Docker chạy lại (mục 6).

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
| `spark_job/job.py` (chế độ test) | `--test-parquet` (chạy đúng hàm xử lý, không cần Spark), `--test-parquet-spark` (**applyInPandasWithState THẬT** trên file source, `trigger(once=True)`, sink JSON) — công cụ của cổng tương đương |
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
| Smoke streaming local (2 pha, checkpoint recovery) | **PASS** | pha 1: 244 bản ghi (162 alert + 82 probe); pha 2 restart từ checkpoint: +129 bản ghi |

### 2.2 Bảng chấm SOTA / baseline — `results/summary/rq2_offline_quality.csv`

Cùng một bộ chấm (`measure/measure_quality.py`, cửa sổ `[start, end + 1 h]` trên `labels.csv` 90 nhãn):

| engine | detector | alert | alert trong cửa sổ | phát hiện | Precision | Recall | F1 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `offline` | `hst` (SOTA của B) | 116 | 85 | 29/90 | **0,7328** | 0,3222 | **0,4476** |
| `offline` | `zscore` (bản tham chiếu) | 348 | 101 | 44/90 | 0,2902 | 0,4889 | 0,3642 |
| `offline` | `ewma` (bản tham chiếu) | 411 | 92 | 53/90 | 0,2238 | **0,5889** | 0,3244 |
| `offline` | `mp` (SOTA của A) | 162 | 8 | 8/90 | 0,0494 | 0,0889 | 0,0635 |

Đọc bảng:
- **HST** là phương pháp có Precision cao nhất (0,7328): 85/116 alert rơi đúng cửa sổ, nhưng chỉ bắt được 29/90 bất thường. Đây là đánh đổi hợp lý của mô hình bán giám sát dựa trên ngưỡng phân vị.
- **EWMA** có Recall cao nhất (0,5889) nhưng Precision thấp nhất trong nhóm streaming vì EWMA phản ứng với cả `level_shift` dài (phát alert lặp trên toàn đoạn).
- **HST ra đúng số của lần chấm trước** (0,7328 / 0,3222 / 0,4476) sau khi merge ⇒ merge **không** làm lệch cách chấm. Đây là kiểm tra hồi quy cho `measure_quality.py`.
- Cả 4 dòng đều `has_recv_ts_ms = False` nên cột `detection_delay_*` để trống: SOTA chạy offline, không có giờ máy lúc nhận. Với dòng của streaming, cột này sẽ có số.

### 2.3 Vì sao Matrix Profile của A ra F1 = 0,0635 (cần A quyết)

Đây **không phải** lỗi bộ chấm. Bằng chứng:

1. **Số alert không ít** (162) nhưng chỉ **8** rơi vào cửa sổ nhãn, tức alert nằm ở chỗ khác chứ không phải bị mất.
2. **Lệch thời điểm**: trung vị khoảng cách từ alert tới `start_ms` của nhãn là **123 giờ trước nhãn**; chỉ **19/90** nhãn có alert trong 24 giờ trước `start_ms`. MP đang phát alert ở các *discord tự nhiên* của PM2.5 chứ không ở chỗ B chèn bất thường.
3. **Ngưỡng quá thấp so với phân bố thật**: `sota.calib_fraction = 0,2` + `sota.threshold_quantile = 0,99` ⇒ ngưỡng lấy trên 20 % dữ liệu **đầu năm** (mùa khô, PM2.5 nền cao): `hn_01` 3,87 / `hn_02` 2,96 / `hn_03` 3,42. Sang mùa mưa (nền thấp) và các đoạn `level_shift` dài, khoảng cách matrix profile vượt ngưỡng liên tục ⇒ alert dồn về mùa khô + các đoạn nhiễu.
4. **`refractory = 24`** chỉ bỏ 24 mẫu sau mỗi alert, trong khi `level_shift` dài tới 24 mẫu và `drift` tới 48 mẫu ⇒ một đoạn bất thường dài vẫn sinh nhiều alert liên tiếp.

Gợi ý tinh chỉnh (A quyết, B không đổi tham số của A):
- Hạ `threshold_quantile` xuống 0,95–0,97, hoặc calibrate trên **50 % dữ liệu đầu** để ngưỡng bám được cả mùa mưa.
- Chạy thêm `--causal` (`stumpi`) rồi so — bản offline dùng tương lai nên `level_shift` dài bị "coi là bình thường" ở phần đầu đoạn.
- Chạy ablation `--no-normalize` để xem z-normalize có làm mất biên độ bất thường không.
- Nếu vẫn thấp: dùng MP như **feature** (khoảng cách) thay vì ngưỡng cứng, hoặc gộp alert theo cụm thời gian trước khi chấm.

---

## 3. Phát hiện kỹ thuật quan trọng phía B

### 3.1 Cặp `pandas==2.2.3` + `pyarrow==19.0.0` cho PySpark 4.1.3

`pandas 3.1.0` + `pyarrow 26` (bản pip cài mới nhất lúc đó) **làm chết pandas UDF** của Spark 4.1.3:

```
TypeError: 'float' object cannot be interpreted as an integer
  at pyarrow.lib.RecordBatch.slice (worker pandas UDF)
```

Hạ về **pandas 2.2.3 + pyarrow 19.0.0** (đúng cặp trong `docker/Dockerfile.spark`) thì chạy ổn định. Đã ghim trong `requirements.txt` kèm ghi chú; **đừng nâng cấp** hai gói này khi chưa thử lại UDF.

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

### 3.5 Thông số máy B (để A chốt máy đo) và **cảnh báo RAM**

| | Máy B |
| --- | --- |
| CPU | AMD Ryzen 5 5600U — **6 core vật lý / 12 luồng** |
| RAM | **13,84 GB** |
| OS | Windows 11 Home + WSL2 Ubuntu |
| Trong WSL2 | `nproc = 8`, `MemTotal = 9 154 240 kB ≈ 8,73 GiB` (chưa có `.wslconfig` riêng) |
| Docker | Docker Desktop (đang Stop trong phiên này) |

**Cảnh báo:** compose hiện tại cần Kafka `2 CPU / 3 GB` + engine `4 CPU / (1,2 + 2×3,5 GB)` ≈ **11,2 GB**, **vượt** RAM WSL (8,73 GB) ⇒ container sẽ bị OOM-kill giữa run. Trước khi chạy RQ1/RQ3 phải làm **một trong hai**:
- tăng `memory=` trong `%UserProfile%\.wslconfig` lên `10GB`–`11GB` (máy có 13,84 GB) rồi `wsl --shutdown`; hoặc
- hạ Kafka xuống `1,5–2 GB` (`mem_limit`, `KAFKA_HEAP_OPTS`) — cách này **thay đổi môi trường đo**, phải ghi vào báo cáo.

### 3.6 Ba bẫy bash khi viết `run_experiment.sh`

1. **`pipefail` + SIGPIPE**: `flink list -r | grep -q ...` làm `grep` thoát sớm, `flink` nhận SIGPIPE (141), `pipefail` khiến cả pipeline trả 141 ⇒ hàm "job đang chạy" trả **sai**. Nay dùng `out=$(... || true)` rồi `[[ $out =~ [0-9a-f]{32} ]]`.
2. **CRLF**: repo đặt `core.autocrlf=true` nên file `.sh` trong working tree là CRLF; blob trong git là LF. Trong WSL phải chạy bản từ `git show` hoặc bật `git config core.autocrlf input` cho repo. Kiểm tra: `git show HEAD:scripts/run_experiment.sh | file -`.
3. **`docker compose stop`** engine kia phải chạy **trước** `up -d` engine mình, nếu không 2 engine cùng chiếm 4 core ⇒ mọi so sánh vô nghĩa (script đã làm đúng thứ tự này).

### 3.7 `measure_quality.py`: detection delay không cần `sent.csv`

`ingest_ts_ms` đã được engine **chép nguyên** vào bản ghi alert (README 5.2) nên `recv_ts_ms − ingest_ts_ms` tính được ngay từ `alerts.jsonl` + `recv_ts_ms` do `measure_perf.py` gắn. `sent.csv` của producer là thống kê **theo giây** (không phải từng sự kiện) nên **không dùng được** cho mục đích này — `--sent` chỉ còn là nguồn dự phòng cho dữ liệu cũ.

---

## 4. Trả lời 5 câu hỏi mở ở Mục 7 của `handover_A.md`

### 4.1 Máy nào là máy đo?

**Đề xuất: máy B** — nhưng cần A xác nhận, vì B không biết cấu hình máy A.

| Lý do chọn máy B | Số liệu |
| --- | --- |
| CPU đủ cho 4 core engine + Kafka + producer/measure | Ryzen 5 5600U, 6 core / 12 luồng, WSL `nproc = 8` |
| RAM đủ **sau khi** tăng WSL | 13,84 GB → đặt WSL 10–11 GB (compose cần ~11,2 GB) |
| Đã có sẵn code + dữ liệu + nhãn của B | không phải copy parquet 2 chiều |

**Quy tắc bắt buộc** (đề nghị ghi vào báo cáo): cả Flink và Spark chạy **cùng một máy, cùng một phiên Docker**, cùng giới hạn CPU/RAM. Nếu A chọn máy A thì B **không** tự chạy RQ1/RQ3, chỉ chạy `rq0` (kiểm tra tương đương) và `rq2` (chất lượng) trên máy B, vì `rq2` offline không phụ thuộc máy đo.

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

### 4.5 Nếu máy B là máy đo: B có chạy RQ1/RQ3 phía Flink bằng script của A không? B có viết nháp Thảo luận không?

**Có, cả hai.** `scripts/run_experiment.sh` đã hỗ trợ `flink` và `spark` với cùng 7 bước (B viết lại theo handover mục 4), nên B chạy được Flink mà không phải sửa gì thêm — chỉ cần A xác nhận **mức tải** (4.2) và **thứ tự chạy** (đề nghị: `rq0` → `rq2` → `rq1` → `rq3`).

Nháp phần Thảo luận/Giới hạn của B đã có trong **`docs/report_B_draft.md`** (mục 8 và 9), trong đó ghi sẵn luận điểm quan trọng: kết quả "Flink có thông lượng cao nhất" của ShuffleBench là với **code Java**; với Python, PyFlink trả giá **cầu nối Java–Python ở từng bản ghi**, còn PySpark `applyInPandasWithState` xử lý theo **lô pandas**.

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

# 4) hình cho báo cáo
python scripts/plot_results.py                # -> results/plots/*.png, *.pdf
python scripts/plot_results.py --check        # kiểm tra code vẽ, không ghi đè

# 5) xem kết quả
python -c "import pandas as pd;print(pd.read_csv('results/summary/rq1.csv').to_string())"
docker compose exec spark-master ls /checkpoints/spark/<run_id>   # checkpoint RocksDB
```

---

## 6. Việc B làm tiếp (theo thứ tự)

1. **Tăng RAM WSL** lên 10–11 GB (`.wslconfig`) rồi `wsl --shutdown`; build `p3-spark:4.1.3` và `docker compose up` **lần đầu** (chưa từng chạy trên máy này).
2. Chạy `rq0 spark equiv 1` → phải ra `TƯƠNG ĐƯƠNG 759/759` **qua Kafka thật** (hiện mới kiểm tra bằng file source).
3. Chạy `rq2 spark replay 1` → điền dòng `spark` vào `results/summary/rq2.csv` và bảng chất lượng streaming.
4. Chạy `rq1` / `rq3` cho Spark theo mức tải A chốt, 3 lần lặp, **chỉ giữ run `clock_ok = true`**.
5. Chạy `scripts/plot_results.py` sau mỗi đợt để hình luôn khớp số mới.
6. Viết tiếp `docs/report_B_draft.md` khi có số RQ1/RQ3 thật.

**A cần biết:** (a) `requirements-b.txt` đã **xóa**, chỉ dùng `requirements.txt`; (b) MP cần A tinh chỉnh tham số (mục 2.3); (c) PR #1 chứa toàn bộ thay đổi trong commit này.



