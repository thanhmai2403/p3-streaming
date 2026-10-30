# Bàn giao B → A (đợt 2) — ghép báo cáo, ghép slide, nộp bài

> Viết ngày **30/10/2026**. Mục đích: A có đủ thông tin để **ghép bài và nộp** mà không phải hỏi lại B.
> Tài liệu chi tiết của B vẫn là `docs/handover_B.md` (mục 1–7); tài liệu này chỉ chứa phần **A cần**.

## 1. B đã giao những gì

| Hạng mục | File / vị trí | Trạng thái |
| --- | --- | --- |
| Báo cáo phần B | `docs/report_B_draft.md` — §0 Tóm tắt, §4.1–4.7, §5.1–5.7, §8.1–8.11, §9 | **xong** |
| Slide 8–14 (chữ để dán vào deck) | `docs/slides_B_8_to_14.md` — kèm phân công deck, quy ước trình bày, ghi chú thuyết trình, nguồn số | **xong** |
| Kịch bản video demo Spark 2'30" | `docs/slides_B_8_to_14.md` mục “Video demo Spark” (bảng theo giây + lệnh chính xác + 3 lỗi hay gặp) | **xong kịch bản**, còn **quay** |
| Số liệu | `results/summary/*.csv` + `results/raw/*/` + `results/plots/` | **xong** |
| Checklist nộp bài | `docs/slides_B_8_to_14.md` mục “Checklist trước khi nộp” | **xong** |

## 2. Bốn câu trả lời RQ (dùng khi ghép phần Kết luận)

| RQ | Trả lời một dòng | File số |
| --- | --- | --- |
| RQ0 | Đổi engine **không** đổi kết quả: 759/759 alert, `only = 0/0`, lệch điểm số lớn nhất 3,55e-15 | `results/summary/rq0.csv` |
| RQ1 | Không engine nào nhanh hơn toàn diện: Flink p99 < 2 s tới 2 000 sự kiện/giây nhưng bão hoà 3 465–3 884; Spark giữ 8 377–9 096 sự kiện/giây với p50 7–9 s | `results/summary/rq1.csv` |
| RQ2 | Chất lượng **bất biến** theo engine: z-score F1 0,4236 / EWMA 0,3377 ở **cả hai**; độ trễ cảnh báo khác nhau (1,03 s vs 28,65 s — chỉ là định hướng, xem §5 dưới) | `rq2_*_quality.csv` |
| RQ3 | 9/9 run **không mất probe**; `AT_LEAST_ONCE` trùng 40–1 786, `EXACTLY_ONCE` 0 trùng; `outage_s` 13,0 (Spark) / 24,8 (ALO) / 38,9 (EO) | `results/summary/rq3_detail.csv` |

## 3. Việc A cần làm (theo thứ tự)

1. **Đánh số mục.** Bản nháp của B nhảy §0 → §4 → §5 → §8 → §9 vì §1–3 và §6–7 là phần của A. Chốt số mục
   rồi cập nhật cột “Mục báo cáo” trong bảng tra nhanh cuối `docs/slides_B_8_to_14.md` nếu số thay đổi.
2. **Tài liệu tham khảo.** Mục `[R9]` (ShuffleBench) trong `report_B_draft.md` **chưa mở được bài gốc** để đối
   chiếu. A kiểm tra lại tên/venue/năm: nếu không xác minh được thì **xoá `[R9]`** và xoá câu trích tương ứng
   trong §8/§9 (đừng để trích dẫn chưa kiểm trong bài nộp).
3. **Ghép slide.** Deck 14 slide / 15 phút: A slide 1–7 (3'00 + 2'30 + 2'00), B slide 8–14 (4'00 + 3'00).
   Nếu A đã đưa RQ0 vào slide 6–7 thì **bỏ slide 8** và dồn số vào slide 9 (một dòng “cổng tương đương 759/759”).
4. **Chạy lại kiểm tra (1 lần, trước khi nộp):**
   ```bash
   python scripts/plot_results.py --check     # -> CHECK OK, không ghi đè hình
   git status --short                         # phải sạch (không còn _tmp_*, không còn results/raw/rq0_spark_demo_*)
   ```
5. **Video:** B quay; A chèn vào slide 12 (sau RQ3) hoặc cuối deck. Không cắt ghép — quay 1 lần liền mạch.

## 4. Ba điều **không** được phát biểu khi trình bày

| Đừng nói | Vì |
| --- | --- |
| “Spark nhanh hơn / chậm hơn Flink” (không kèm điều kiện) | RQ1 là **đánh đổi**: Flink giữ đuôi độ trễ tới 2 000 sự kiện/giây, Spark giữ nhịp 8 000 nhưng p50 7–9 s |
| “Flink phục hồi nhanh hơn Spark” | `recovery_s` **không so trực tiếp được**: ngưỡng là 2 × p99 nền, mà nền Spark 6,36–9,31 s còn Flink ALO 1,54–3,44 s. Chỉ so `outage_s` |
| “Engine này chính xác hơn engine kia” | RQ2 cho **kết quả y hệt** (F1 trùng khít, `f1_std = 0`); khác biệt duy nhất là **độ trễ cảnh báo** |

## 5. Ba cảnh báo về phạm vi số liệu (đã ghi trong §8, nhắc lại để A không nói quá)

- **Hai máy khác nhau:** RQ1 và RQ3 chạy trên máy đo (4 core), RQ0 và RQ2 chạy trên máy B (WSL2 8,73 GiB,
  `docker-compose.lowmem.yml`). Chỉ so sánh **trong cùng một bảng**, không trộn hai máy vào một khẳng định.
- **RQ2 delay 1,03 s vs 28,65 s chỉ là định hướng:** RQ2 replay là **burst** (19 568 sự kiện trong ~11 s), nên
  số của Spark phản ánh **thời gian xả hàng đợi**, không phải năng lực xử lý. Kết luận hiệu năng phải lấy RQ1.
- **Máy cá nhân:** mọi số tuyệt đối **không** suy ra cho cluster; nói rõ “trên máy cá nhân, 4 core, một engine
  mỗi lần”.
