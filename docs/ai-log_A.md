# Nhật ký dùng AI có kiểm soát – phần ROLE A (Flink + hạ tầng)

Yêu cầu của môn: dùng các nền tảng chat AI để research và viết code, nhưng **người làm phải định hướng,
kiểm soát và hiểu kiến trúc, quy trình**. File này ghi lại cách A dùng AI (Claude) và các lần AI sai /
phải sửa, để người đọc thấy kết quả không phải "AI viết sao để vậy".

## 1. Cách làm việc

| Bước | Ai làm | Ghi chú |
|---|---|---|
| Chọn đề tài, câu hỏi nghiên cứu, SOTA (Matrix Profile, Half-Space Trees) | Nhóm | AI chỉ liệt kê lựa chọn |
| Thiết kế kiến trúc (Kafka → engine → Kafka → measure), schema bản ghi, quy ước run_id | Nhóm + AI | Chốt trong README, B duyệt |
| Viết code (job Flink, producer, measure, script chạy) | AI viết nháp | A đọc, chạy test, chạy thật trên máy mình |
| Chạy thí nghiệm | A | AI không truy cập được cluster; A dán output về để phân tích |
| Kết luận từ số liệu | A | Mọi nhận định phải chỉ ra được file/cột số liệu chứng minh |

Nguyên tắc kiểm soát đã áp dụng:

1. **Có cổng trước khi đo**: test đơn vị (`pytest tests/`) và cổng tương đương RQ0 (Flink = Spark = bản
   tham chiếu độc lập, 759/759 cảnh báo, sai số tối đa 3,55e-15) phải qua thì mới đo hiệu năng.
2. **Kiểm file khi chuyển**: mọi file AI gửi đều kèm md5, A so md5 sau khi copy vào repo.
3. **Không ghi đè kết quả**: run lỗi/không hợp lệ chuyển sang `results/invalid/` kèm README lý do, chạy
   lại với run_id mới.
4. **Kiểm chứng bằng nguồn độc lập** trước khi sửa cấu hình (bài học ở mục 2.11).

## 2. Các sự cố và cách xử lý

| # | Hiện tượng | AI đề xuất ban đầu | Kiểm chứng / kết quả | Bài học |
|---|---|---|---|---|
| 2.1 | Build image Flink lỗi `apt Permission denied` | Thêm `USER root` khi cài, trả về `USER flink` | Build thành công | Image chính thức chạy bằng user thường |
| 2.2 | File AI ghi sang máy không đổi nội dung | — | Phát hiện nhờ so md5 | Từ đó luôn so md5 |
| 2.3 | Test Flink cục bộ ra sai thứ tự sự kiện | Source song song 1 ≠ job 4 làm `key_by` chèn bước chia round-robin | Test dùng FileSource 4 file, job không đặt parallelism riêng | Hiểu cách PyFlink chèn operator khi `key_by` |
| 2.4 | Độ trễ Flink cao bất thường (process mode) | Chuyển `python.execution-mode: thread`, bỏ gán timestamp | Cấu hình ở mức job **không có tác dụng**, phải đặt ở cluster (compose); sau đó p50 ≈ 0,4 s | Kiểm tra cấu hình thực sự có hiệu lực qua REST `/jobmanager/config` |
| 2.5 | Độ trễ âm / nhảy hàng chục giây | Ban đầu nghi lệch giờ container | Đo được giờ thật WSL2 nhảy +66 s, +31 s, −34 s giữa lúc chạy. Chuyển mọi mốc đo sang `CLOCK_MONOTONIC` (cùng kernel, kiểm bằng boot_id) | Script kiểm đồng hồ đầu tiên báo động giả vì sai số đo > ngưỡng → viết lại |
| 2.6 | Job Flink thứ 2 / khi restart lỗi `NoSuchFieldException loadedLibraryNames` | pemja dùng reflection, xung đột classloader trên Java 17 | Đưa `flink-python` vào `lib/` + `classloader.parent-first-patterns.additional: pemja.`; kiểm bằng 3 job liên tiếp + failover | Lỗi chỉ lộ ra khi chạy nhiều job, test 1 job không thấy |
| 2.7 | 3/24 run RQ1 hỏng vì Kafka admin timeout | Thêm chờ Kafka sẵn sàng + thử lại khi tạo topic | Hết lỗi timeout; vẫn còn 1 lần xoá topic > 60 s (RQ3 EO r2) → chạy lại | Hạ tầng 1 broker trên laptop không ổn định tuyệt đối |
| 2.8 | Script tổng hợp dò tải crash khi run không có probe | Xử lý run "HỎNG" riêng | — | |
| 2.9 | `missing_seq` lớn ở Flink 8000 ev/s | Nghi mất dữ liệu | Thực ra measure dừng trước khi Flink xử lý hết backlog; không phải mất | Phân biệt "chưa nhận" và "mất" |
| 2.10 | RQ3 báo "phục hồi 0,0 s" khi job chết hẳn | Công thức chỉ xét tới probe cuối cùng | Sửa: xét tới lúc producer gửi xong; thêm cờ `recovered` | Chỉ số phải xử lý được trường hợp xấu nhất |
| 2.11 | **RQ3 Flink "không phục hồi"** ở 6/11 run | AI chẩn đoán **sai** 2 lần: (a) GIL làm kẹt; (b) dựa vào chỉ số REST `read/write-records = 0` kết luận source rỗi → đổi cấu hình restart (chờ 20 s, heartbeat 15 s) | So **tổng offset topic alerts** (Kafka) với số bản ghi measure đọc được: r7 Kafka có 28 811, measure chỉ đọc 6 989. Nguyên nhân thật: `measure_perf --idle-timeout` mặc định 20 s, measure tự thoát trong lúc Flink restart. Chỉ số REST = 0 ngay cả ở run phục hồi tốt → chỉ số không dùng được với job PyFlink | **Kiểm chứng bằng nguồn độc lập trước khi sửa.** 11 run chuyển sang `results/invalid/`, tắt idle-timeout, chạy lại; delay restart đưa về 5 s |
| 2.12 | RQ3 EXACTLY_ONCE báo phục hồi 150–155 s dù p99 đã về mức nền | Cửa sổ 5 s xen kẽ cửa sổ rỗng vì sink EO chỉ commit 10 s/lần | Dùng cửa sổ 15 s cho EO (`scripts/rq3_report.py`) | Định nghĩa chỉ số phải khớp cơ chế của hệ thống được đo |
| 2.13 | Ngưỡng phục hồi tương đối (2 × p99 nền) có lợi cho Spark (nền 6–9 s) | — | Báo cáo dùng **thời gian mất điện** (tuyệt đối) làm chỉ số chính, tách riêng thời gian xử lý bù | |

## 3. Những gì AI không quyết định

- Cấu hình tài nguyên 2 TaskManager × 2 slot giống 2 worker × 2 core của Spark, và quyết định **giữ**
  cấu hình này dù GIL làm Flink thread mode chỉ dùng ~265 %/400 % CPU (ghi vào phần Giới hạn thay vì
  đổi sang 4 TM × 1 slot).
- Việc chấp nhận/loại run: A quyết định dựa trên log và số liệu.
- Cách diễn giải kết quả trong báo cáo.

## 4. Lệnh tái lập

```bash
pytest tests/                                        # cổng 1
bash scripts/run_experiment.sh rq0 flink equiv 1     # cổng 2 (so với spark + tham chiếu)
bash scripts/run_experiment.sh rq1 flink rate2000 1  # RQ1
bash scripts/run_experiment.sh rq3 flink rate2000 1  # RQ3 (DELIVERY=EXACTLY_ONCE cho EO)
python scripts/rq3_report.py && python scripts/plot_a.py
```
