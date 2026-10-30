# Slide 8–14 (phần của B) — P3 Streaming: Flink vs Spark cho phát hiện bất thường

Tài liệu này là **nội dung chữ** của 7 slide cuối (8 → 14) + kịch bản **video demo Spark 2–3 phút**,
để dán vào file trình chiếu (PowerPoint/Google Slides) và để người trình bày đọc trước. Mọi con số ở đây
**lấy từ báo cáo** (`docs/report_B_draft.md`) và từ các file trong `results/summary/` — cột “Nguồn số” ở
cuối mỗi slide ghi rõ file + mục báo cáo để đối chiếu khi bị hỏi.

**Phân công deck (14 slide, 15 phút):**

| Slide | Nội dung | Người | Thời lượng |
| --- | --- | --- | --- |
| 1–3 | Mở đầu, câu hỏi nghiên cứu, nghiên cứu liên quan | A | 3'00" |
| 4–5 | Kiến trúc hệ thống + dữ liệu/bộ phát hiện | A | 2'30" |
| 6–7 | Phương pháp đo + kết quả RQ1 của Flink | A | 2'00" |
| **8–11** | **RQ0 (tương đương), RQ1 (so sánh), RQ2 (chất lượng + độ trễ), SOTA** | **B** | **4'00"** |
| **12–14** | **RQ3 (chịu lỗi), giới hạn, kết luận + Q&A** | **B** | **3'00"** |
| — | Video demo Spark (phát ở slide 12 hoặc cuối) | B | 2'30" |

Nếu A đã đưa RQ0 vào slide 6–7 thì **bỏ slide 8** và dồn số của nó vào slide 9 (một dòng “cổng tương đương
759/759”); khung 9–14 giữ nguyên thứ tự: **kết quả → so sánh → giới hạn → kết luận**. Đừng đổi thứ tự
RQ1 trước RQ2: RQ1 là chỗ duy nhất được phép nói về hiệu năng, RQ2 nói về chất lượng.

**Quy ước trình bày (dùng chung với A):**

- Khổ 16:9, font ≥ 24 pt cho bullet, ≥ 40 pt cho “số liệu nhấn”; mỗi slide **≤ 6 bullet, ≤ 12 từ/bullet**.
- Số thập phân dùng **dấu phẩy** (0,4236) như báo cáo; hàng nghìn dùng **dấu cách** (1 030, 28 652).
- Đơn vị luôn ghi kèm: `sự kiện/giây`, `ms`, `s`. Không viết “nhanh hơn 28 lần” mà không nói **cái gì** nhanh hơn.
- Mỗi slide kết quả có **một câu chốt** ở góc dưới (in đậm) — đó là câu người nghe phải nhớ.
- Hình: dùng bản `.png` trong `results/plots/` (đã xuất kèm `.pdf` nếu cần in).

---

## Slide 8 — RQ0: hai engine, **một** ngữ nghĩa (cổng tương đương)

**Tiêu đề trên slide:** RQ0 — Đổi engine có đổi kết quả phát hiện không? (Không: 759/759)

**Số liệu nhấn (ô lớn, giữa slide):**

```
759 / 759 alert     only = 0 / 0     score_mismatch = 0
max_diff = 3,55 × 10⁻¹⁵ (Flink)  |  0,00e+00 (Spark)
```

**Nội dung hiển thị (5 bullet):**

- Đặc tả thuật toán **nằm ngoài engine**: `common/detectors_ref.py` là bản tham chiếu độc lập.
- Quy tắc dữ liệu trễ **2 giờ nằm trong state**, không dùng watermark → hai engine bỏ **cùng** sự kiện.
- Kết quả qua **Kafka thật** (không phải đọc file): Flink 2 run, Spark 1 run — cả 3: `missing_seq = 0`,
  `dup_seq = 0`, `clock_ok = true`.
- So **theo tập** `(seq, detector)`, không so theo thứ tự: `only_flink = 0`, `only_spark = 0`.
- Bền cả khi xấu nhất: Spark đọc **20 micro-batch** trên **RocksDB** + 5 % sự kiện đến trễ →
  `late_dropped = 544` **bằng đúng** bản tham chiếu, 1 325 alert + 188 probe khớp tuyệt đối.

**Câu chốt:** *Khác biệt ở các slide sau là khác biệt của **engine**, không phải của thuật toán.*

**Ghi chú thuyết trình (≈ 60 s):**
“Câu hỏi đầu tiên không phải engine nào nhanh hơn, mà là **đổi engine có đổi kết quả không**. Nếu đổi, mọi
so sánh sau đó vô nghĩa. Nhóm tách đặc tả ra khỏi engine: quy tắc bỏ dữ liệu trễ 2 giờ nằm trong state chứ
không dùng watermark, nên hai engine bỏ đúng cùng sự kiện. Chạy thật qua Kafka: 759 alert ở cả hai, không
alert nào chỉ có ở một bên, sai khác điểm số lớn nhất 3,55 × 10⁻¹⁵ — cách ngưỡng chấp nhận 10⁻⁶ mười bậc.
Điều đáng nói hơn: khi ép Spark đọc thành 20 micro-batch nhỏ trên RocksDB, state phải xuống đĩa rồi đọc lại
giữa các batch, và 5 % sự kiện bị đảo vị trí — kết quả vẫn khớp, kể cả 544 sự kiện bị bỏ vì trễ.”

**Nguồn số:** `results/summary/rq0.csv` (3 dòng `rq0_*`), `results/raw/rq0_flink_equiv_r{1,2}/`,
`results/raw/rq0_spark_equiv_r1/`; multibatch: `results/raw/rq0_spark_multibatch_late/check.json`
(`state_store = rocksdb`). Báo cáo: §5.4 (bảng cổng tương đương), §8.4, §9 đoạn 1.

---

## Slide 9 — RQ1: không engine nào nhanh hơn toàn diện, chỉ là **đánh đổi**

**Tiêu đề trên slide:** RQ1 — Flink giữ đuôi độ trễ, Spark giữ nhịp gửi

**Bảng trên slide (min–max của 3 lần lặp mỗi mức, `ms`):**

| Tải (sự kiện/giây) | Flink p99 | Flink thực (sự kiện/giây) | Spark p50 | Spark p99 | Spark thực (sự kiện/giây) |
| --- | --- | --- | --- | --- | --- |
| 1 000 | **705 – 1 236** | 998 – 1 002 | 5 713 – 7 507 | 16 048 – 32 696 | 1 073 – 1 135 |
| 2 000 | **1 049 – 1 975** | 1 997 – 2 004 | 3 970 – 7 491 | 7 489 – 35 424 | 2 059 – 2 236 |
| 4 000 | 27 402 – 66 386 | **3 465 – 3 884** | 3 175 – 7 654 | 8 098 – 31 167 | 4 150 – 4 503 |
| 8 000 | 57 949 – 136 142 | 4 584 – 7 413 | 7 263 – 8 659 | 24 246 – 34 852 | **8 377 – 9 096** |

**Hình:** `results/plots/rq1_throughput_latency.png` (điểm thông lượng–p99, có nhãn cấu hình).
Bản thay thế khi cần nhìn theo mức tải: `results/plots/rq1_rates.png`.

**Nội dung hiển thị (3 bullet + 1 cảnh báo):**

- **Flink: p99 < 2 s tới 2 000 sự kiện/giây**, nhưng bão hoà ở **3,5 – 3,9 nghìn sự kiện/giây** (4 000) và
  **mất probe** ở 8 000 (`missing_seq` 0 – 2 952).
- **Spark: giữ được 8 000 sự kiện/giây** (8 377 – 9 096), đổi lại p50 **7 – 9 s**, p99 **24 – 35 s** —
  micro-batch: mỗi bản ghi phải chờ trigger.
- Cùng điều kiện: **4 core** (2 worker × 2 core), **parallelism 4**, checkpoint 10 s, RocksDB, **mỗi lần chỉ
  bật một engine**, `check_clock.sh` trước **và** sau mỗi run.
- ⚠️ Run đồng hồ lệch (`rq1_flink_try5000_r4`: 19 probe `emit < ingest`) **bị loại**, không báo cáo; bảng
  chỉ còn 4 mức × 3 lần = **24 run hợp lệ**.

**Câu chốt:** *Cần cảnh báo dưới 2 giây → Flink; cần không bỏ sự kiện ở 8 000/giây → Spark.*

**Ghi chú thuyết trình (≈ 75 s):**
“Đây là slide duy nhất được phép nói về hiệu năng. Cách đọc: đừng tìm engine thắng, hãy tìm **điểm gãy**.
Flink gãy ở đâu? Ở 1 000 và 2 000 sự kiện/giây p99 còn 0,7–2,0 giây, nhưng lên 4 000 thì p99 vọt lên
27–66 giây và thông lượng thực tụt còn 3 500–3 900 — tức Flink xử lý **chậm hơn nhịp gửi** nên hàng đợi
dài ra. Ở 8 000 còn mất probe. Spark thì ngược lại: nhận đủ 8 000/giây nhưng mỗi bản ghi phải chờ
micro-batch nên p50 đã là 7–9 giây. p50 của Spark **không phải** lỗi cấu hình, đó là bản chất trigger.
Hai cảnh báo khi trích số: thứ nhất, đây là máy cá nhân trong WSL2 nên **không** suy ra cho cluster; thứ
hai, run `try5000` của Flink có đồng hồ nhảy 31 giây nên bị loại — nhóm không báo cáo số không kiểm chứng
được.”

**Nguồn số:** `results/summary/rq1.csv` (24 dòng `rate*`, loại 4 dòng `try5000`). Báo cáo: §5.6, §5.1
(môi trường), §8.2 (đồng hồ), §9 (RQ1).

---

## Slide 10 — RQ2: chất lượng phát hiện **bất biến** theo engine

**Tiêu đề trên slide:** RQ2 — Đổi engine không đổi Precision/Recall/F1, chỉ đổi **độ trễ**

**Bảng trên slide (trung bình 3 lần lặp mỗi engine, `n_reps = 3`):**

| Engine | Detector | Precision | Recall | **F1** | delay trung vị | delay p95 |
| --- | --- | --- | --- | --- | --- | --- |
| Flink | `zscore` | 0,2815 | 0,8556 | **0,4236** | **1 030 ms** | 2 033 ms |
| Flink | `ewma` | 0,2032 | **1,0000** | **0,3377** | **1 009 ms** | 1 995 ms |
| Spark | `zscore` | 0,2815 | 0,8556 | **0,4236** | 28 652 ms | 32 597 ms |
| Spark | `ewma` | 0,2032 | **1,0000** | **0,3377** | 28 940 ms | 32 595 ms |

**Số liệu nhấn (ô lớn):**

```
f1_std = 0 ở CẢ HAI engine      n_alert = 572 (z-score) / 753 (EWMA) — giống tuyệt đối
```

**Hình:** `results/plots/rq2_quality_comparison.png` (8 nhóm cột: 4 offline + `flink/zscore`,
`flink/ewma`, `spark/zscore`, `spark/ewma`).

**Nội dung hiển thị (4 bullet):**

- Bốn dòng P/R/F1 chỉ có **hai** bộ giá trị: mỗi detector cho **đúng cùng** kết quả ở hai engine.
- Vì hai engine dùng **cùng đặc tả** (§4.2–4.4) và **cùng bộ chấm** (§4.7) — chất lượng là **đại lượng xác định**.
- Streaming **bám sát offline**: z-score F1 0,4306 → **0,4236** (phát hiện 77/90 cửa sổ nhãn),
  EWMA 0,3398 → **0,3377** (vẫn 90/90); chênh lệch do **544 sự kiện bị bỏ vì trễ**.
- Lặp lại chỉ làm rõ **phân bố độ trễ**, không làm đổi P/R/F1 (khác RQ1/RQ3, nơi cả hai đều biến thiên).

**Câu chốt:** *Engine không quyết định phát hiện đúng hay sai — chỉ quyết định **bao lâu** thì biết.*

**Ghi chú thuyết trình (≈ 60 s):**
“Kết quả quan trọng nhất của RQ2 là một **bất biến**, không phải một chênh lệch: bốn dòng trong bảng chỉ có
hai bộ số. Cùng đặc tả, cùng bộ chấm nên z-score ra F1 0,4236 ở cả Flink và Spark; EWMA ra 0,3377 ở cả
hai; `f1_std` bằng 0 trong cả hai file gộp, và số alert cũng trùng khít 572/753. Nếu bị hỏi ‘thế chất lượng
streaming có kém offline không’, câu trả lời là **kém rất nhẹ và có nguyên nhân**: 0,4306 xuống 0,4236 vì
quy tắc không chờ bỏ mất 544 sự kiện, làm chuỗi trạng thái lệch ở đúng các vị trí đó. Đây là giá của quy
tắc, không phải sai số đo — và nó **giống hệt nhau** ở hai engine, thêm một bằng chứng cho tính xác định.”

**Nguồn số:** `results/summary/rq2_offline_quality.csv`, `rq2_flink_replay_quality.csv`,
`rq2_spark_replay_quality.csv` (cột `*_std`, `n_reps = 3`); từng lần ở `results/summary/
rq2_<engine>_replay_r<n>_quality.csv`. Báo cáo: §5.5, §4.7 (cách chấm), §9 (RQ2).

---

## Slide 11 — RQ2 (tiếp): cái giá của độ trễ + so với SOTA ngoài luồng

**Tiêu đề trên slide:** Cùng chất lượng, khác **độ trễ**: 1,03 s so với 28,65 s

**Số liệu nhấn (hai ô lớn cạnh nhau):**

```
Flink  z-score delay trung vị 1,03 s  (p95 2,03 s)
Spark  z-score delay trung vị 28,65 s (p95 32,60 s)   → ≈ 28×
```

**Nội dung hiển thị (3 bullet + 1 cảnh báo):**

- Chênh lệch **giải thích được bằng mô hình thực thi**: Flink xử lý **từng bản ghi**, Spark xử lý theo
  **micro-batch** (bản ghi phải chờ trigger và xếp hàng trong batch).
- Bảng SOTA cùng bộ chấm (`90` nhãn, 19 568 mẫu, ngưỡng phân vị 99 % trên 20 % dữ liệu đầu):

| Phương pháp | Precision | Recall | F1 | delay theo giờ sự kiện (trung vị) |
| --- | --- | --- | --- | --- |
| **Half-Space Trees (SOTA)** | **0,7328** | 0,3222 | **0,4476** | 0,0 h |
| Rolling z-score (baseline) | 0,2852 | **0,8778** | 0,4306 | 0,0 h |
| EWMA (baseline) | 0,2047 | **1,0000** | 0,3398 | 0,0 h |
| Matrix Profile (SOTA) | 0,0494 | 0,0889 | 0,0635 | 1,0 h |

- HST **precision cao nhất** nhưng recall thấp (bắt 29/90) ⇒ chênh HST – z-score chỉ **0,017 F1** trên
  **90 nhãn, 1 lần chạy** ⇒ bài **không** tuyên bố phương pháp nào “tốt hơn”.
- ⚠️ Con số 28× **chỉ là định hướng**: RQ2 chạy trên **máy B** với `docker-compose.lowmem.yml`, replay
  dồn 19 568 sự kiện vào ~11–13 s nên đây là **thời gian xả hàng đợi**, không phải độ trễ ổn định (§8.9, §8.10).

**Câu chốt:** *Với người dùng cuối, cảnh báo sớm hơn 27 giây có thể quan trọng hơn vài phần trăm F1.*

**Ghi chú thuyết trình (≈ 65 s):**
“Cùng F1 nhưng độ trễ khác gần 28 lần — vì sao? Flink xử lý từng bản ghi, Spark theo micro-batch: bản ghi
vào batch nào thì phải chờ batch đó chạy xong. Nhưng phải nói ngay **giới hạn**: RQ2 chạy trên máy B với
cấu hình tiết kiệm RAM, và producer phát lại 19 568 sự kiện trong khoảng 11–13 giây, nhanh hơn nhịp engine
xử lý, nên con số này đo **thời gian xả hàng đợi**, không phải độ trễ trạng thái ổn định. Vì vậy nhóm chỉ
đọc nó là **định hướng**, còn kết luận hiệu năng dựa trên RQ1 — nơi hai engine chạy cùng máy, cùng 4 core.
Phần SOTA: Half-Space Trees có precision cao nhất 0,7328 nhưng chỉ bắt 29/90 cửa sổ, nên F1 0,4476 chỉ hơn
z-score 0,017 — với 90 nhãn và một lần chạy, đó là **chưa phân biệt được**. Matrix Profile chỉ 0,0635, và
nguyên nhân là ngưỡng hiệu chỉnh trên 20 % dữ liệu đầu cộng refractory ngắn — bài học về hiệu chỉnh ngưỡng.”

**Nguồn số:** `results/summary/rq2_<engine>_replay_quality.csv` (cột `detection_delay_ms_*`),
`results/summary/rq2_offline_quality.csv` (4 phương pháp, `detection_delay_hours_event_median`).
Báo cáo: §5.5, §4.5–4.6 (HST/MP), §8.7 (hiệu chỉnh ngưỡng), §8.9–8.10 (hai máy + burst), §9 (RQ2, SOTA).

---

## Slide 12 — RQ3: mất một worker thì sao? (9/9 run **không mất dữ liệu**)

**Tiêu đề trên slide:** RQ3 — Kill worker ở giây 60: không mất gì, nhưng trả giá bằng **thời gian** hoặc **trùng**

**Bảng trên slide (trung vị của 3 lần lặp; ngoặc = khoảng quan sát):**

| Chế độ | `outage_s` (mất kết nối) | Thời gian phục hồi | `catchup_s` (xử lý bù) | Trùng (`dup_seq`) | Mất (`missing_seq`) |
| --- | --- | --- | --- | --- | --- |
| Spark (`spark-ckpt`, cửa sổ 5 s) | **13,0** (8,3 – 15,0) | **25 s** (20 – 25) | 9,7 (6,0 – 16,7) | **0** | **0** |
| Flink `AT_LEAST_ONCE` | 24,8 (23,0 – 29,0) | 145 s (130 – *> 156*) | 110,9 (106,3 – 115,4) | **40 – 1 786** | **0** |
| Flink `EXACTLY_ONCE` (cửa sổ 15 s) | 38,9 (26,5 – 48,6) | 120 s (60 – 135) | 86,4 (21,1 – 93,5) | **0** | **0** |

**Hình:** `results/plots/rq3_timeline.png` (p99 theo cửa sổ 10 s quanh lúc kill, t = 0) và
`results/plots/rq3_breakdown.png` (outage + xử lý bù = thời gian phục hồi, từng run một chấm).

**Nội dung hiển thị (3 bullet + 1 cảnh báo):**

- **9/9 run không mất probe nào** → được phép so thời gian phục hồi mà không kèm điều kiện “nhưng đã mất dữ liệu”.
- **Trùng là giá của `AT_LEAST_ONCE`, thời gian là giá của `EXACTLY_ONCE`**: ALO phát lại từ checkpoint gần
  nhất ⇒ 40 / 178 / 1 786 probe trùng; EO **0 trùng** nhưng `catchup_s` biến thiên 21 – 94 s.
- **`outage_s` mới là đại lượng so sánh được giữa hai engine**: Spark 13 s (chỉ restart worker, driver sống),
  Flink 24,8 s (ALO) / 38,9 s (EO) (restart TaskManager + nạp lại RocksDB + replay từ checkpoint).
- ⚠️ `recovery_s` **không** so trực tiếp được: ngưỡng là `p99 ≥ 2 × p99 nền` mà **nền khác nhau**
  (Spark 6,36 – 9,31 s; Flink ALO 1,54 – 3,44 s) ⇒ nền cao “hồi phục” sớm hơn **theo đúng định nghĩa**.

**Câu chốt:** *Không mất dữ liệu ở cả ba chế độ; khác biệt nằm ở **đánh đổi trùng ↔ thời gian**, đo bằng
`outage_s` + `catchup_s`, không bằng một con số duy nhất.*

**Ghi chú thuyết trình (≈ 70 s):**
“Kịch bản: 2 000 sự kiện/giây, giết một worker ở giây 60, start lại sau 10 giây. Kết quả đầu tiên và quan
trọng nhất: **không mất probe nào ở cả 9 run**. Vậy khác nhau ở đâu? Flink `AT_LEAST_ONCE` phát lại từ
checkpoint gần nhất trong khi sink đã ghi một phần, nên sinh 40 đến 1 786 bản ghi trùng; `EXACTLY_ONCE`
không trùng bản ghi nào nhưng phải trả bằng thời gian xử lý bù. Và đây là chỗ dễ nói sai nhất: **thời gian
phục hồi không so sánh trực tiếp được giữa hai engine**, vì ngưỡng của nó là gấp đôi p99 nền, mà nền của
Spark 6–9 giây còn Flink chỉ 1,5–3,4 giây — hệ thống có nền cao hơn sẽ ‘hồi phục’ sớm hơn theo đúng định
nghĩa. Đại lượng so sánh được là **thời gian mất kết nối**: Spark 13 giây, Flink 25–39 giây. Thêm một lưu ý
trung thực: một run ALO chưa hồi phục trong 156 giây quan sát, nên con số 145 giây là **trung vị có cận dưới**.”

**Nguồn số:** `results/summary/rq3_detail.csv` (dựng bởi `scripts/rq3_report.py`; `rec_w5_s` / `rec_w15_s`,
`censored`). Báo cáo: §5.3 (định nghĩa độ đo), §5.7, §8.8 (một điểm lỗi, một broker), §9 (RQ3).
Video demo Spark: phát **sau** slide này (xem mục “Video demo Spark” cuối tài liệu).

---

## Slide 13 — Đe dọa hợp lệ: nhóm đã xử lý gì, và **không** được kết luận gì

**Tiêu đề trên slide:** Đọc kết quả đúng hay sai, phụ thuộc 8 điều kiện này

**Bảng trên slide (đe dọa → cách xử lý / phát biểu bị giới hạn):**

| # | Đe dọa | Đã làm gì / phát biểu thế nào |
| --- | --- | --- |
| 1 | Máy cá nhân, Docker Desktop + WSL2, **hai máy** | Chỉ so sánh **cùng máy, cùng phiên, một engine/lần**; số tuyệt đối không suy ra cho cluster (§8.1, §8.9) |
| 2 | Đồng hồ WSL trôi (từng nhảy ≈ 66 s) | `check_clock.sh` **trước và sau** mỗi run; run `clock_ok = false` bị **loại** khỏi bảng (§8.2) |
| 3 | Mỗi cấu hình chỉ **3 lần lặp** | Báo cáo trung bình + **min–max**, không kiểm định; chênh < 10–15 % coi là **chưa phân biệt được** (§8.3) |
| 4 | **PyFlink thread mode** ≠ Flink JVM | Chỉ kết luận trong phạm vi **Python API**; chênh lệch là chi phí **cầu nối ngôn ngữ** (§8.4) |
| 5 | Nhãn bất thường do nhóm **tự chèn** (90 nhãn) | Nêu rõ Precision có thể **bị đánh giá thấp**; không kết luận theo mùa (§8.5, §8.6) |
| 6 | Ngưỡng SOTA cố định (phân vị 99 % trên 20 % đầu) | So sánh SOTA là **cấu hình cụ thể**, không phải giới hạn trên của thuật toán (§8.7) |
| 7 | Chỉ kill **1 worker**, Kafka **1 broker / RF = 1** | Chỉ trả lời “mất một phần năng lực xử lý”, **không** kết luận về tính sẵn sàng toàn hệ thống (§8.8) |
| 8 | Replay RQ2 dồn thành **burst** (~11–13 s cho 19 568 sự kiện) | RQ2 delay = **thời gian xả hàng đợi**; hiệu năng chỉ lấy từ **RQ1** (§8.10) |

**Câu chốt:** *Mọi kết luận trong bài là **so sánh tương đối trong cùng điều kiện**, không phải benchmark tuyệt đối.*

**Ghi chú thuyết trình (≈ 55 s):**
“Slide này để trả lời trước câu hỏi khó: ‘số của bạn có đáng tin không?’. Nhóm không che giới hạn mà đặt
chúng cạnh kết quả. Ba điều quan trọng nhất: một, thí nghiệm chạy trên **hai máy** — RQ0/RQ2 trên máy B cấu
hình tiết kiệm RAM, RQ1/RQ3 trên máy đo — nên chỉ số tuyệt đối không được trộn; mỗi so sánh giữa hai engine
luôn nằm trong **một máy**. Hai, đồng hồ từng nhảy 66 giây trên WSL, nên có cổng kiểm tra trước và sau mỗi
run, run nào lệch thì **loại** chứ không báo cáo. Ba, chỉ 3 lần lặp nên nhóm báo min–max và **không** dùng
kiểm định; chênh lệch nhỏ hơn 10–15 % thì nói là chưa phân biệt được. Ngoài ra job Flink ở đây là PyFlink
thread mode, không phải Flink JVM — nên kết quả ShuffleBench ‘Flink nhanh nhất’ không áp dụng cho bài này.”

**Nguồn số/ý:** báo cáo §8.1 – §8.11 (đầy đủ 11 mục); §5.1 (phiên bản, môi trường). Không có số mới trên
slide này — nếu bị hỏi chi tiết, mở §8 tương ứng.

---

## Slide 14 — Kết luận: chọn engine theo **ràng buộc**, không theo “ai hơn ai”

**Tiêu đề trên slide:** Kết luận — bốn câu trả lời, một khung so sánh tái lập được

**Bốn câu trả lời (4 ô, mỗi ô 1 dòng):**

| | Câu hỏi | Trả lời một dòng |
| --- | --- | --- |
| **RQ0** | Đổi engine có đổi kết quả? | **Không** — 759/759 alert, lệch điểm số 3,55 × 10⁻¹⁵, bền qua 20 micro-batch + 5 % trễ |
| **RQ1** | Engine nào nhanh hơn? | **Không có engine thắng tuyệt đối** — Flink p99 < 2 s tới 2 000/giây; Spark giữ 8 000/giây với p50 7–9 s |
| **RQ2** | Chất lượng và độ trễ? | P/R/F1 **bất biến** theo engine (0,4236 / 0,3377); chỉ delay khác **1,03 s** so với **28,65 s** |
| **RQ3** | Mất một worker thì sao? | **9/9 không mất dữ liệu**; ALO trùng 40–1 786, EO 0 trùng; `outage_s` 13 s (Spark) / 24,8–38,9 s (Flink) |

**Nội dung hiển thị (4 bullet):**

- **Ngữ nghĩa tách khỏi engine** nhờ quy tắc trễ nằm trong state (không watermark) + bộ chấm xác định.
- **So sánh được kiểm soát**: cùng 4 core, cùng checkpoint 10 s, RocksDB, một engine mỗi lần, một máy mỗi so sánh.
- **Chất lượng là đại lượng xác định**, hiệu năng và thời gian phục hồi thì không → phải báo min–max và nêu định nghĩa.
- **Hướng tiếp theo**: hiệu chỉnh ngưỡng thích nghi cho MP/HST; cài đặt **Java** để tách chi phí cầu nối Python;
  cluster nhiều máy + Kafka nhiều broker; `REPLAY_SPEED` trải hết cửa sổ để đo delay **ổn định**.

**Câu chốt (dòng cuối slide, in đậm):**
*Cần cảnh báo nhanh → Flink; cần giữ nhịp lớn → Spark; cần hai engine cho **cùng** kết quả → cả hai, miễn là
đặc tả nằm trong state chứ không trong engine.*

**Ghi chú thuyết trình (≈ 55 s):**
“Tóm lại bốn câu. RQ0: đổi engine **không** đổi kết quả — đó là điều kiện để ba câu còn lại có nghĩa. RQ1:
không có engine thắng toàn diện, chỉ có điểm gãy khác nhau — Flink gãy sau 2 000 sự kiện/giây, Spark gãy
ngay ở độ trễ. RQ2: chất lượng giống hệt nhau, chỉ độ trễ khác. RQ3: cả ba chế độ không mất dữ liệu, khác
nhau ở chỗ trả giá bằng **bản ghi trùng** hay bằng **thời gian**. Hướng tiếp theo rõ nhất là cài đặt Java cho
cả hai engine — vì p99 của Flink chỉ 0,7–1,2 giây ở 1 000 sự kiện/giây, chi phí cầu nối Python là ứng viên
hàng đầu cho phần bị mất khi tăng tải. Cảm ơn, nhóm sẵn sàng nhận câu hỏi.”

**Nguồn số:** tổng hợp từ slide 8–12; báo cáo §9 (Kết luận và hướng phát triển).

---

## Video demo Spark — 2 phút 30 giây

Mục tiêu của video **không** phải chứng minh “Spark nhanh/chậm”, mà chứng minh **quy trình chạy được thật**:
Spark đọc Kafka → giữ state trên RocksDB → ghi alert → cổng tương đương **PASS 759/759**. Vì vậy chọn
`rq0` (ngắn nhất, ~90 s) chứ **không** quay `rq1` (210 s/run) hay `rq3` (phải chờ kill + phục hồi).

### Chuẩn bị (làm trước khi bấm quay, video không tính phần này)

Máy B (WSL2 8,73 GiB) phải dùng **lowmem override**; nếu quay trên máy đo thì bỏ các biến `COMPOSE_FILE`,
`SPARK_*_MEMORY` và không cần `docker-compose.lowmem.yml`.

```bash
# Git Bash trên Windows, tại thư mục repo
cd /c/p3-streaming
export MSYS_NO_PATHCONV=1
export COMPOSE_FILE="docker-compose.yml:docker-compose.lowmem.yml"
export SPARK_EXECUTOR_MEMORY=1024m SPARK_DRIVER_MEMORY=512m

# 1) image đã build sẵn (chạy trước, có thể mất 5–10 phút nếu chưa có)
docker compose build

# 2) bật Kafka + Spark (KHÔNG bật Flink: quy ước mỗi lần chỉ một engine)
docker compose up -d kafka spark-master spark-worker-1 spark-worker-2
bash scripts/create_topics.sh

# 3) đóng băng cửa sổ terminal: font >= 16, tắt thông báo Windows
bash scripts/check_clock.sh          # phải PASS ngay trước khi quay
```

Nếu `check_clock.sh` FAIL: đóng Docker Desktop, trong PowerShell chạy `wsl --shutdown`, mở lại rồi kiểm tra lại
(`README` mục 10.2 / `docs/handover_B.md` mục 3.8) — **không quay** khi cổng đồng hồ chưa qua.

### Kịch bản quay (tổng 2'30", quay 1 lần, không cắt)

| Thời điểm | Thao tác trên màn hình | Lời thoại (tóm tắt) |
| --- | --- | --- |
| 0:00 – 0:15 | Slide tiêu đề demo + `docker compose ps` | “Demo chạy Spark trên Kafka thật, cùng bộ dữ liệu 10 000 sự kiện đã dùng cho cổng tương đương.” |
| 0:15 – 0:30 | `bash scripts/check_clock.sh` → PASS | “Trước khi đo, cổng đồng hồ: lệch quá 50 ms là dừng, vì mọi độ trễ đều là hiệu hai mốc giờ.” |
| 0:30 – 1:35 | `bash scripts/run_experiment.sh rq0 spark demo 1` (~60–90 s) | “Script làm đủ 7 bước: kiểm đồng hồ, reset topic + checkpoint, submit job, chạy đo, phát lại 10 000 sự kiện qua Kafka, drain, huỷ job và tổng hợp.” |
| 1:35 – 1:50 | `python measure/compare_outputs.py …` → **TƯƠNG ĐƯƠNG** | “759 trên 759 alert, không alert nào chỉ có ở một bên, lệch điểm số 0.” |
| 1:50 – 2:10 | `ls /checkpoints/spark/rq0_spark_demo_r1` + đọc 3 alert từ topic `alerts` | “State nằm trên RocksDB, checkpoint mỗi 10 giây; alert ra topic dạng JSON đúng schema.” |
| 2:10 – 2:25 | `cat results/raw/rq0_spark_demo_r1/meta.json` | “`n_alert = 759`, `n_probe = 100`, `n_bad = 0` — khớp bản tham chiếu.” |
| 2:25 – 2:30 | Xoá run demo + câu chốt | “Toàn bộ bằng chứng đã có trong repo; em xoá run demo để repo sạch.” |

### Lệnh chính xác dùng trong video

```bash
# (A) chạy một lần thí nghiệm rq0 của Spark — run_id = rq0_spark_demo_r1
bash scripts/run_experiment.sh rq0 spark demo 1
#    lần quay thứ 2 trở đi: thêm FORCE=1 ở đầu lệnh (script từ chối ghi đè kết quả cũ)

# (B) cổng tương đương: bản tham chiếu offline vs alert Spark vừa chạy
python measure/compare_outputs.py \
  --ref-file data/processed/ref_alerts.jsonl \
  --a-file   data/processed/ref_alerts.jsonl \
  --b-file   results/raw/rq0_spark_demo_r1/alerts.jsonl \
  --ref offline --a offline --b spark
#  -> "offline vs spark: n=759/759 only_offline=0 only_spark=0 score_mismatch=0 max_diff=0.00e+00 OK"
#  -> "=> TƯƠNG ĐƯƠNG"

# (C) bằng chứng state + output
docker compose exec -T spark-master ls /checkpoints/spark/rq0_spark_demo_r1
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic alerts --from-beginning --max-messages 3

# (D) số trong meta.json (không cần docker)
cat results/raw/rq0_spark_demo_r1/meta.json

# (E) dọn dẹp sau khi quay — run demo KHÔNG thuộc bộ bằng chứng nộp bài
rm -rf results/raw/rq0_spark_demo_r1
git status --short        # phải sạch sau bước này
```

**Ba lỗi hay gặp khi quay (đã gặp thật, xem `docs/handover_B.md` mục 3.8):**

1. Quên `export COMPOSE_FILE` / `SPARK_*_MEMORY` ⇒ `spark-master` chết vì vượt `mem_limit 1300m`; script
   dừng ở bước chờ job với thông báo “job spark không chạy sau 180 s”.
2. Git Bash tự đổi `/opt/...` thành `C:/Program Files/Git/opt/...` ⇒ script đã `export MSYS_NO_PATHCONV=1`;
   nếu tự gõ lệnh `docker compose exec` thì thêm biến này hoặc dùng `-T`.
3. Quay khi Flink còn đang chạy ⇒ script **tự stop** engine kia (bước 2), nhưng video sẽ thừa 20–40 s chờ;
   kiểm tra `docker compose ps` trước khi bấm quay.

**Kế hoạch dự phòng:** nếu Docker/Kafka lỗi ngay lúc quay, dùng bản quay nháp để đối chiếu rồi quay lại sau
15 phút; **không** dựng lại số bằng tay. Nếu không kịp quay lại, phát bản nháp và nói rõ “đây là lần chạy ngày
…”; mọi con số trình bày vẫn lấy từ `results/summary/`, không lấy từ màn hình video.

---

## Checklist trước khi nộp (phần B)

- [ ] Slide 8–14 đã dán vào file trình chiếu, **không còn chữ “slide …”** trong nội dung hiển thị.
- [ ] Mọi bảng trên slide khớp `results/summary/` (đối chiếu 4 nhóm file: `rq0.csv`, `rq1.csv`,
      `rq2_*_quality.csv`, `rq3_detail.csv`).
- [ ] Hình đã chèn đúng 5 file trong `results/plots/`: `rq1_throughput_latency.png`, `rq1_rates.png`,
      `rq2_quality_comparison.png`, `rq3_timeline.png`, `rq3_breakdown.png`.
- [ ] Mỗi slide có **một câu chốt**; tổng thời lượng nói của slide 8–14 ≤ **7 phút**.
- [ ] Video demo ≤ **2'30"**, có tiếng, thấy rõ dòng `=> TƯƠNG ĐƯƠNG`.
- [ ] `rm -rf results/raw/rq0_spark_demo_r1` và `git status --short` sạch sau khi quay.
- [ ] Tập nói thử 1 lần với đồng hồ bấm giây; cắt bớt bullet nếu quá giờ.

### Bảng tra nhanh: slide → mục báo cáo → file số

| Slide | Mục báo cáo | File số | Hình |
| --- | --- | --- | --- |
| 8 (RQ0) | §5.4, §8.4 | `results/summary/rq0.csv`, `results/raw/rq0_spark_multibatch_late/check.json` | — |
| 9 (RQ1) | §5.6, §5.1, §8.2 | `results/summary/rq1.csv` | `rq1_throughput_latency.png`, `rq1_rates.png` |
| 10 (RQ2 chất lượng) | §5.5, §4.7 | `rq2_offline_quality.csv`, `rq2_{flink,spark}_replay_quality.csv` | `rq2_quality_comparison.png` |
| 11 (RQ2 độ trễ + SOTA) | §5.5, §4.5–4.7, §8.7, §8.10 | như trên (cột `detection_delay_ms_*`) | `rq2_quality_comparison.png` |
| 12 (RQ3) | §5.3, §5.7, §8.8 | `results/summary/rq3_detail.csv` | `rq3_timeline.png`, `rq3_breakdown.png` |
| 13 (giới hạn) | §8.1 – §8.11 | — | — |
| 14 (kết luận) | §9 | tổng hợp slide 8–12 | — |
