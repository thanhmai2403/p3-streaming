# Bàn giao của A (Flink + hạ tầng), ngày 25/10/2026

Người viết: A (Mai). Người nhận: B.
Repo: https://github.com/thanhmai2403/p3-streaming

**Tóm tắt:**
- Hạ tầng (Kafka + Flink) chạy được. Job Flink, producer, script đo và Matrix Profile đã viết xong, có test.
- **Chưa qua cổng tương đương**, vì đang chờ B push bản tham chiếu và dữ liệu.
- **Chưa có số liệu hiệu năng dùng được**, vì phát hiện đồng hồ WSL2 bị nhảy giữa lúc đo (mục 3).

---

## 1. A đã hoàn thành

### 1.1 Hạ tầng (đã chạy trên máy A)

| File | Nội dung |
| --- | --- |
| `docker-compose.yml` | Kafka 4.2.2 KRaft, 1 broker (các topic nội bộ RF = 1); Flink 2.2.1: 1 JobManager + 2 TaskManager × 2 slot; **thread mode cho Python** |
| `docker/Dockerfile.flink` | `flink:2.2.1` + Python 3.11 (build với `--enable-shared`) + `apache-flink==2.2.1` + connector `flink-sql-connector-kafka-5.0.0-2.2.jar` |
| `scripts/create_topics.sh` | Xóa rồi tạo lại `sensor` và `alerts`, 6 partition |
| `scripts/check_clock.sh` | **Mới.** So đồng hồ WSL, container và Windows; lệch quá 50 ms thì báo lỗi |
| `config.yaml`, `common/config.py` | Nguồn tham số duy nhất và hàm đọc |

Tài nguyên đã đặt (**phía Spark phải đặt y hệt**):

| Container | CPU | RAM |
| --- | --- | --- |
| Kafka | 2 | 3 GB |
| JobManager (điều phối) | 0,5 | 1,2 GB |
| TaskManager × 2 | 2 mỗi cái → **tổng 4 core** | 3,5 GB mỗi cái |

### 1.2 Code (đã test, xem mục 1.3)

| File | Nội dung |
| --- | --- |
| `common/schema.py` | Parse và kiểm tra sự kiện; tạo `alert`/`probe` đúng README 5.2; `make_seq` / `split_seq` |
| `flink_job/detector.py` | z-score, EWMA, quy tắc dữ liệu trễ (README 6.1–6.3), viết bằng Python thuần |
| `flink_job/job.py` | Kafka → parse → key_by(station_id, param) → detect → Kafka. RocksDB, checkpoint 10 s vào `/checkpoints/flink/<run_id>`. Có cờ `--delivery EXACTLY_ONCE` cho RQ3. Bản ghi hỏng bị bỏ qua và đếm, không làm chết job |
| `producer/producer.py` | `--mode rate`: 1000 trạm random walk. `--mode replay`: phát lại parquet, có `--late-fraction` và `--late-shift`. Hỗ trợ `--producer-id` / `--n-producers`; ghi `sent.csv`; có `--dry-run` |
| `measure/measure_perf.py` | `collect`: đọc `alerts`, **gắn `recv_ts_ms`**, ghi `latency.csv`, `alerts.jsonl`, `docker_stats.csv`, `meta.json`. `summarize`: p50/p95/p99, thông lượng, bền vững, trùng/mất, phục hồi RQ3, **kiểm tra đồng hồ**; cập nhật `results/summary/<rq>.csv` |
| `sota/matrix_profile.py` | STUMPY: bản offline (mặc định, z-normalized), `--no-normalize`, `--causal` (stumpi) |
| `requirements.txt` | `pip freeze` của venv host |

### 1.3 Kiểm thử (`tests/`, chạy được không cần Docker, trừ test_flink_local)

| Test | Kết quả |
| --- | --- |
| `test_detector` | `detector.py` khớp với bản tham chiếu viết độc lập bằng pandas, trên 1200 sự kiện có dữ liệu trễ; score lệch 3e-12 |
| `test_flink_local` | Job Flink thật trên MiniCluster, parallelism 4, RocksDB: khớp 100%; ở cả thread mode và process mode |
| `test_producer` | Tốc độ, `seq`, chia trạm cho nhiều producer, độ lùi khi replay |
| `test_measure` | Trùng/mất, thời gian phục hồi, phát hiện lỗi đồng hồ |
| `test_mp` | Ba biến thể Matrix Profile chạy được trên chuỗi giả |

Lệnh chạy: `PYTHONPATH=. python -m tests.test_detector` (thay tên test tương ứng).

---

## 2. Thay đổi quy ước mà B CẦN BIẾT

1. **README 5.2 và 10.4:** `alerts.jsonl` có thêm `recv_ts_ms`. Trường này do `measure_perf.py` gắn lúc nhận, engine **không** phát ra. B dùng nó để tính detection delay.
2. **README 6.3 (Flink):** Flink **không gán timestamp/watermark** (`flink.assign_timestamps: false`) và chạy Python ở **thread mode**.
   - Lý do: job không dùng timer event-time.
   - Đo thử thấy nhanh hơn khoảng 2,5 lần mà output không đổi.
   - Không ảnh hưởng đến Spark.
3. **README 10.3 (SOTA):**
   - Output của MP có `seq`, `ingest_ts_ms`, `emit_ts_ms` bằng `null`, và không có `recv_ts_ms` → **`measure_quality.py` phải xử lý được các giá trị null này**.
   - MP bỏ qua 24 mẫu sau mỗi alert (`sota.mp.refractory`).
4. **`config.yaml` có thêm các mục:**
   - `producer` (seed, dữ liệu tổng hợp)
   - `measure` (ngưỡng bền vững, tham số phục hồi)
   - `sota` (calib 20%, phân vị 99%, tham số MP)
   - `flink.python_execution_mode`, `flink.assign_timestamps`
5. **Chế độ `replay` của producer do A viết tạm** để chạy cổng tương đương.
   - Cột cần có: `station_id, param, value, event_time_ms`.
   - Sắp theo `event_time_ms`. Sự kiện bị giữ lại được gửi sau k sự kiện kế tiếp **cùng trạm**.
   - **B review, hoặc thay bằng bản của B.**
6. **Tên run kiểm tra tương đương:** `rq0_<engine>_equiv_r<n>`, vì `measure_perf` yêu cầu tiền tố `rq<số>_`.

---

## 3. Kết quả chạy thử và phát hiện quan trọng

Đã chạy 3 lần thử: Flink, 5000 sự kiện/giây trong 60 giây.

| | Kết quả |
| --- | --- |
| Tính đúng | 300 000 sự kiện → 3000 probe, **không trùng, không mất**; 16 610 alert |
| Process mode → thread mode | CPU TaskManager tăng từ khoảng 65% lên **150–187% / 200%** khi dồn việc; ước tính xử lý từ khoảng 1 900 lên **khoảng 3 300–3 500 sự kiện/giây** trên 4 core |
| Kafka → consumer đo | p50 khoảng **0,3 s**, ổn định. Kafka không phải chỗ nghẽn |
| **Đồng hồ** | **Đồng hồ WSL nhảy khoảng 66 s** giữa lúc chạy, và container Docker lệch với WSL. **Số liệu độ trễ của r1–r3 không dùng được** |

Hệ quả cho cả hai người:

- **Trước mỗi lần đo phải chạy `bash scripts/check_clock.sh`** và phải ra OK. Nếu lệch: chạy `wsl --shutdown` trong PowerShell, rồi mở lại Ubuntu và Docker Desktop. **Không để máy ngủ** khi đang đo.
- Mọi run có `clock_ok = false` trong `summary.json` thì **bỏ đi và chạy lại** với số `r` mới.
- **Các mức tải RQ1 trong kế hoạch (5k/20k/50k/100k) quá cao với PyFlink.** Dự kiến mức hợp lý là khoảng **1 000 / 2 000 / 3 000 / 4 000** sự kiện/giây. A sẽ chốt sau một run có đồng hồ sạch, rồi báo B.
- Ghi vào phần **Giới hạn**: chạy trên WSL2 + Docker Desktop nên đồng hồ có thể trôi; nhóm đã kiểm tra đồng hồ trước và sau mỗi run.
- Ghi vào phần **Thảo luận**: kết quả "Flink có thông lượng cao nhất" của ShuffleBench là với code Java. Với Python, Flink phải đi qua cầu nối Java–Python ở từng bản ghi, còn Spark `applyInPandasWithState` xử lý theo lô pandas.

---

## 4. Việc B cần làm tiếp (theo thứ tự ưu tiên)

### Ưu tiên 0: chặn cổng ngày 2, làm ngay

- [ ] **Push lên nhánh `b/...` và mở PR:**
  - `common/detectors_ref.py`
  - `measure/compare_outputs.py`
  - `data/processed/equiv_10k.parquet` (dùng `git add -f` vì `.gitignore` chặn `*.parquet`)
  - Code Spark: `spark_job/job.py`, `docker/Dockerfile.spark`
- [ ] **Xác nhận cách so khớp 759/759** đúng README mục 9: so **tập (`seq`, `detector`)** và **`score` lệch < 1e-6**, không chỉ so số lượng.
- [ ] **Thêm Spark vào `docker-compose.yml`, tài nguyên y hệt Flink:**
  - Master: 0,5 CPU / 1,2 GB
  - 2 worker, mỗi worker 2 CPU / 3,5 GB
  - `spark.sql.shuffle.partitions = 4`
  - Checkpoint `/checkpoints/spark/<run_id>`
  - Không gọi `withWatermark`; `sort_values("seq")` trong mỗi nhóm
- [ ] **Chốt máy đo** cho RQ1/RQ3 của cả hai engine. Gửi A kết quả `nproc` và RAM trong WSL của máy B. Nếu máy B mạnh hơn, Flink cũng chạy trên máy B.

### Ưu tiên 1: cần cho thí nghiệm

- [ ] **`measure/measure_quality.py`:**
  - Đọc `alerts.jsonl` (có `recv_ts_ms`) và `labels.csv`.
  - Cửa sổ đúng là `[start_ms, end_ms + 1 giờ]`.
  - Tính theo từng (engine, detector).
  - **Chấp nhận `seq` / `ingest_ts_ms` = null** của SOTA. Với SOTA chỉ tính P/R/F1, không tính detection delay.
- [ ] **`scripts/run_experiment.sh <rq> <engine> <cấu hình> <lần>`**, dùng các lệnh của A:
  1. `bash scripts/check_clock.sh`. Lệch thì dừng.
  2. Dừng engine kia, chạy `bash scripts/create_topics.sh`, xóa checkpoint.
  3. Submit job. Flink: `docker compose exec flink-jobmanager flink run -d -py /opt/p3/flink_job/job.py --run-id <run_id> [--delivery EXACTLY_ONCE]`.
  4. `python measure/measure_perf.py collect --run-id <run_id> --docker-stats &`
  5. `python producer/producer.py --mode rate --rate <R> --duration 210 --out-dir results/raw/<run_id>` (các tham số chia producer `--producer-id` / `--n-producers` thêm khi cần).
  6. **RQ3:** ở giây 60, chạy `docker kill flink-tm1` (hoặc container worker Spark) rồi `docker start` lại. **Ghi `kill_ts_ms` (epoch ms) vào `results/raw/<run_id>/meta.json`.** `summarize` sẽ tự tính thời gian phục hồi.
  7. Hủy job, rồi chạy lại `check_clock.sh` ở cuối.
- [ ] **Đưa A** `data/processed/openaq_hn_injected.parquet` và `labels.csv` để chạy Matrix Profile trên dữ liệu thật.
- [ ] **README 7.1:** cho A biết **tần suất gốc** của dữ liệu OpenAQ và cách đã gộp về giờ (trung bình? mốc đầu hay cuối giờ? tối thiểu bao nhiêu mẫu mỗi giờ?) để A ghi vào README. Hoặc B tự sửa trong một commit riêng.
- [ ] **Review** chế độ `replay` của producer (mục 2.5).

### Ưu tiên 2

- [ ] Half-Space Trees (`sota/half_space_trees.py`), output cùng schema, `engine="offline"`, `detector="hst"`.
- [ ] RQ2 cho cả hai engine (replay OpenAQ, `--late-fraction 0.05 --late-shift 1 4`).
- [ ] RQ1 và RQ3 cho Spark, theo các mức tải A chốt.
- [ ] Review PR của A.

---

## 5. A làm tiếp

1. Chạy r4 với đồng hồ sạch, chốt 4 mức tải RQ1, báo B.
2. Khi B push xong: chạy Flink trên `equiv_10k` (`--mode replay --speed 2000`, run `rq0_flink_equiv_r1`) và `compare_outputs.py` → **phải ra 759/759**.
3. RQ1 Flink (4 mức × 3 lần), RQ3 Flink (kill TaskManager × 3, thêm một lần EXACTLY_ONCE).
4. Matrix Profile trên dữ liệu thật, chấm bằng `measure_quality.py`.
5. Báo cáo: Mở đầu, Nghiên cứu liên quan, Kiến trúc, Kết quả Flink, Thảo luận, Phụ lục; slide 1–7; video demo Flink.
6. `docs/ai-log.md`: bổ sung các mục cho hạ tầng, job, đo đạc. Ghi cả các lỗi đã gặp: `USER root` trong Dockerfile, thứ tự sự kiện bị xáo khi đổi parallelism, `write_meta` chưa tạo thư mục, process mode chậm, đồng hồ WSL.

---

## 6. Lệnh nhanh

```bash
docker compose build && docker compose up -d && bash scripts/create_topics.sh
bash scripts/check_clock.sh
docker compose exec flink-jobmanager flink run -d -py /opt/p3/flink_job/job.py --run-id <run_id>
docker compose exec flink-jobmanager flink list            # / flink cancel <JobID>
python measure/measure_perf.py collect --run-id <run_id> --docker-stats &
python producer/producer.py --mode rate --rate 2000 --duration 210 --out-dir results/raw/<run_id>
python producer/producer.py --mode replay --file data/processed/equiv_10k.parquet --speed 2000
python measure/measure_perf.py summarize --run-id <run_id> [--kill-ts-ms <ms>]
python sota/matrix_profile.py --file data/processed/openaq_hn_injected.parquet
```

Giao diện Flink: http://localhost:8081

## 7. Câu hỏi đang mở, cần trả lời ở buổi sync tới

1. Máy nào là máy đo?
2. Chốt các mức tải RQ1 (đề xuất khoảng 1k / 2k / 3k / 4k sự kiện/giây).
3. Quy tắc gộp dữ liệu về giờ (README 7.1).
4. B có đồng ý các thay đổi ở mục 2 (đặc biệt 2.2 và 2.3) không?
5. Nếu máy B là máy đo: B chạy luôn RQ1/RQ3 phía Flink bằng script của A? B viết nháp mục Thảo luận?
