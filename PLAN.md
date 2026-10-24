# KẾ HOẠCH HÀNH ĐỘNG CHI TIẾT – THÀNH VIÊN B (PySpark & Data)
> Căn cứ theo bản quy ước dùng chung `README.md` (chốt ngày 23/10/2026).

---

## 1. Trách nhiệm chính của B
- **Dữ liệu & Nhãn:** Tải/làm sạch dữ liệu OpenAQ Hà Nội, chèn bất thường và xuất `labels.csv` + `openaq_hn_injected.parquet`.
- **Producer:** Viết chế độ `--mode replay` (có mô phỏng gửi trễ ngẫu nhiên 5%).
- **Bản cài đặt tham chiếu:** Viết `common/detectors_ref.py` (chuẩn Python thuần để so khớp kết quả).
- **Hạ tầng Docker:** Viết `docker/Dockerfile.spark` và thêm `spark-master`, `spark-worker x2` vào `docker-compose.yml`.
- **Engine Streaming:** Cài đặt job PySpark Structured Streaming với `applyInPandasWithState`.
- **SOTA Online:** Triển khai Half-Space Trees (`river`).
- **Đánh giá chất lượng:** Viết `measure/measure_quality.py` và `measure/compare_outputs.py`.
- **Kịch bản chạy:** Viết `scripts/run_experiment.sh` điều phối các đợt chạy RQ1, RQ2, RQ3.
- **Báo cáo & Slide:** Viết phần Phương pháp phát hiện, SOTA, Thiết kế thí nghiệm, Kết quả Spark, Giới hạn.

---

## 2. Lộ trình thực hiện chi tiết trong 4 ngày

### NGÀY 1 – Môi trường, Dữ liệu và Khung Job
- [ ] **Sáng:** 
  - Đọc và thống nhất các trường trong `config.yaml` và schema chung.
  - Viết `common/detectors_ref.py`: Cài đặt chuẩn cho z-score và EWMA kèm logic lọc trễ theo đúng mục 6 trong `README.md`.
- [ ] **Chiều:**
  - Viết `data/inject_anomalies.py`: Làm sạch dữ liệu trong `data/raw/`, chèn spike, level shift, drift với seed 42 -> xuất `data/processed/openaq_hn_injected.parquet` và `labels.csv`.
  - Cấu hình Spark vào `docker-compose.yml` (Spark 4.1.3, 2 worker x 2 core, giới hạn tổng 4 core + 8 GB RAM).
- [ ] **Tối:**
  - Viết `producer/producer.py` chế độ `--mode replay` (đọc parquet, phát lại theo `--speed`, hỗ trợ `--late-fraction 0.05`).
  - Viết khung job Spark `spark_job/job.py`: Đọc Kafka topic `sensor` -> parse JSON -> xuất ra topic `alerts`.
- **Cổng Ngày 1:** `docker compose up` chạy thông suốt Spark + Kafka; job Spark đọc được Kafka và ghi ra `alerts`.

### NGÀY 2 – Hoàn thiện Bộ phát hiện & Công cụ Đo
- [ ] **Sáng:**
  - Cài đặt z-score trượt (cửa sổ 24, |z| > 3.0) và EWMA (λ = 0.3, L = 3.0) trong `spark_job/job.py` bằng `applyInPandasWithState`.
  - Đảm bảo trong mỗi micro-batch phải `sort_values("seq")` trước khi chạy state.
  - Gắn probe cho mỗi sự kiện có `seq % 100 == 0`.
- [ ] **Chiều:**
  - Viết `measure/compare_outputs.py`.
  - Cùng Thành viên A chạy file test `data/processed/equiv_10k.parquet` qua bản tham chiếu, Flink và Spark -> So khớp để danh sách alert lệch nhau < 1e-6.
  - Viết `measure/measure_quality.py` (tính Precision, Recall, F1 theo sự kiện, Detection Delay) và `scripts/run_experiment.sh`.
- [ ] **Tối:**
  - Viết `sota/half_space_trees.py` bằng thư viện `river` (đánh giá trên cùng tập dữ liệu và chấm bằng `measure_quality.py`).
  - Chạy thử hoàn chỉnh 1 cấu hình Spark từ đầu đến cuối.
- **Cổng Ngày 2:** Spark cho kết quả khớp hoàn toàn với bản tham chiếu trên file kiểm tra; script đo chạy ra số liệu chuẩn.

### NGÀY 3 – Chạy Thí nghiệm & Thu thập Số liệu
- [ ] **Sáng:** Chạy thí nghiệm RQ2 (chất lượng phát hiện trên replay OpenAQ có dữ liệu trễ) 3 lần, thu thập số liệu.
- [ ] **Chiều:** Trên máy đo hiệu năng chung, chạy RQ1 cho Spark (4 mức tải x 3 lần) và RQ3 (kill worker ở giây thứ 60 x 3 lần).
- [ ] **Tối:**
  - Vẽ biểu đồ RQ2, tổng hợp bảng so sánh F1 và Detection Delay (Spark vs Baseline vs SOTA).
  - Viết nháp phần Thiết kế thí nghiệm và Kết quả Spark trong báo cáo.
  - Sync 10 phút: Giải thích kiến trúc và luồng xử lý của Spark cho A nghe.
- **Cổng Ngày 3:** Đầy đủ dữ liệu thô trong `results/raw/`, file tổng hợp `results/summary/` và biểu đồ trong `figures/`.

### NGÀY 4 – Báo cáo, Slide và Demo
- [ ] **Sáng:** Viết các phần được phân công: Tóm tắt, Giới hạn, Kết luận, gom và kiểm tra bài gốc các tài liệu tham khảo. Đọc chéo và sửa bài của A.
- [ ] **Chiều:** Làm slide từ trang 8 đến 14; quay video demo Spark 2–3 phút; ghép slide hoàn chỉnh.
- [ ] **Tối:** Tập thuyết trình thử cùng A (≤ 15 phút), kiểm tra checklist nộp bài cuối cùng.
- **Cổng Ngày 4:** Đầy đủ repo sạch, PDF báo cáo IEEE, PPT slide và video demo.

---

## 3. Các quy tắc kỹ thuật sống còn của B
1. **Shuffle Partitions:** Bắt buộc đặt `spark.sql.shuffle.partitions = 4`[cite: 3, 5].
2. **Không dùng watermark Spark:** Tự cài kiểm tra trễ trong state: `if event_time_ms < max_et - 7200000: drop`[cite: 5].
3. **Thứ tự xử lý:** Bắt buộc sắp xếp DataFrame theo `seq` tăng dần trước khi lặp cập nhật state[cite: 5].
4. **Ghi chép AI Log:** Mọi prompt lớn và đoạn code Cline sinh ra đều phải lưu vào `docs/ai-log.md`[cite: 3, 5].