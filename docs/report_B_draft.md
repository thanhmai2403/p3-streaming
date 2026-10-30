# Bản thảo Báo cáo IEEE — phần của Thành viên B (Mẫn)

> Bản nháp, viết theo văn phong IEEE 2 cột, tiếng Việt. Số liệu bám `config.yaml`, code trong repo và
> `results/summary/*.csv` — **mọi con số đã điền từ đợt đo cuối** (RQ0–RQ3 của cả hai engine).
> Quy tắc nội bộ: mỗi số phải chỉ được **file + cột** nguồn (bảng đối chiếu ở README mục 10.6).
> Mỗi trích dẫn phải mở bài gốc kiểm tra trước khi đưa vào bản nộp (README mục 11).

---

## 0. Tóm tắt (Abstract)

Bài báo so sánh **Apache Flink** và **Spark Structured Streaming** khi cùng thực hiện phát hiện bất
thường trực tuyến trên luồng dữ liệu cảm biến môi trường, với **Kafka** làm lớp vận chuyển. Hai engine
được cấu hình **giống nhau tuyệt đối** về tài nguyên (4 core xử lý, 8 GB, parallelism 4, state backend
RocksDB, checkpoint 10 giây) và dùng **cùng một đặc tả thuật toán**: rolling z-score cửa sổ 24 mẫu,
EWMA với λ = 0,3 và L = 3, cùng một quy tắc bỏ dữ liệu đến trễ 2 giờ **tự cài trong state** thay vì
dựa vào watermark của engine. Dữ liệu thực nghiệm là PM2.5 của **3 trạm Hà Nội** trong 12 tháng, được
làm sạch và gộp về 1 mẫu/giờ (**19 568 mẫu**), sau đó chèn **90 bất thường** theo ba dạng `spike`,
`level_shift`, `drift` với `seed = 42`. Hai phương pháp SOTA chạy ngoài luồng — **Half-Space Trees**
và **Matrix Profile** — được chấm bằng cùng một bộ đánh giá trên cùng nhãn.

Kết quả thực nghiệm gồm bốn phần, mỗi đợt đo chạy trên **một máy và một phiên Docker** cho cả hai
engine. **RQ0 — tương đương:** trên 10 000 sự kiện thật, Flink và Spark sinh **đúng cùng tập alert** với
bản tham chiếu độc lập: **759/759** alert, `only = 0/0`, `score_mismatch = 0`, lệch điểm số lớn nhất
**3,55 × 10⁻¹⁵** (ngưỡng chấp nhận 10⁻⁶); kết quả giữ nguyên khi Spark đọc dữ liệu thành **20
micro-batch** trên RocksDB và 5 % sự kiện bị đảo vị trí (**544 sự kiện bị bỏ vì trễ, bằng đúng** bản
tham chiếu). **RQ1 — hiệu năng:** Flink đạt p99 **705–1 236 ms** ở 1 000 sự kiện/giây và **1 049–1 975
ms** ở 2 000 sự kiện/giây, nhưng **không theo kịp** 4 000 sự kiện/giây (thông lượng trung bình **3,5–3,9
nghìn sự kiện/giây**, p99 **27–66 s**) và còn mất probe ở 8 000 sự kiện/giây; Spark giữ được nhịp
**8 000 sự kiện/giây** (thông lượng **8,4–9,1 nghìn sự kiện/giây**, không mất probe) nhưng p50 **7–9 s**
và p99 **24–35 s**. **RQ2 — chất
lượng:** vì dùng chung một đặc tả, F1 của hai engine **giống nhau tuyệt đối** (z-score **0,4236**, EWMA
**0,3377**); chỉ **detection delay** khác nhau: trung vị **1,03 s** (Flink) so với **28,65 s** (Spark).
Ngoài luồng, Half-Space Trees đạt **F1 = 0,4476** với **Precision = 0,7328** (cao nhất), z-score 0,4306,
EWMA 0,3398, Matrix Profile **0,0635** — trường hợp sau do **cách chọn ngưỡng** (phân vị 99 % trên 20 %
dữ liệu đầu) chứ không do bộ chấm. **RQ3 — chịu lỗi:** kill một worker ở giây 60, Spark mất kết nối
**13 s** và phục hồi **25 s**, **không trùng, không mất** probe; Flink AT_LEAST_ONCE mất kết nối **24,8 s**
và phục hồi **145 s** (một run chưa hồi phục trong 156 s quan sát) với **40–1 786 bản ghi trùng**; Flink
EXACTLY_ONCE mất kết nối **38,9 s**, phục hồi **120 s** với **0 trùng**. Cả **9/9** run RQ3 không mất
probe nào.

---

## 4. Phương pháp phát hiện bất thường và dữ liệu

Phần này trình bày hai thành phần tách biệt nhưng phải khớp nhau: **đặc tả thuật toán** (dùng chung
cho Flink, Spark và bản tham chiếu) và **quy trình dữ liệu** (dữ liệu OpenAQ đã gán nhãn, dùng cho cả
đánh giá streaming và SOTA). Nguyên tắc xuyên suốt là *mọi ngữ nghĩa thời gian do bài toán quyết định,
không do engine quyết định*: nếu để Flink và Spark tự xử lý watermark thì hai engine sẽ bỏ những bản
ghi khác nhau và mọi so sánh về sau mất hiệu lực.

### 4.1 Dữ liệu OpenAQ Hà Nội

Dữ liệu được lấy từ **kho lưu trữ OpenAQ trên AWS S3** (`openaq-data-archive`), đọc theo đường dẫn
`locationid/year/month` thay vì gọi API, nhằm tránh giới hạn tần suất và bảo đảm tái lập được. Phạm vi
là **12 tháng** (`2025-10-23` → `2026-10-22`), chỉ số **PM2.5**, ba trạm: `hn_01`
(location `2161292`, tần suất gốc 60 phút), `hn_02` (`4946813`, **5 phút**) và `hn_03` (`6123215`,
60 phút). Trạm `hn_01` được chọn lại từ `2161292` thay cho `2161306` vì location sau chỉ có 1 525 mẫu
(≈ 9 tháng), không đủ chỗ để chèn 30 bất thường cách nhau ≥ 48 giờ.

Quy trình làm sạch, theo đúng thứ tự cài đặt:

1. Lọc `parameter = pm25`: 455 512 → 97 118 dòng.
2. Loại giá trị null (0 dòng) và giá trị âm (57 dòng) → 97 061 dòng.
3. Đổi timestamp ISO-8601 (UTC) sang `event_time_ms` (epoch ms), sắp xếp theo (`station_id`, thời gian).
4. Loại timestamp trùng, giữ bản ghi đầu tiên (0 dòng).
5. **Gộp về 1 mẫu/giờ**: `event_time_ms ← ⌊event_time_ms / 3 600 000⌋ × 3 600 000` rồi lấy **trung
   bình cộng** các mẫu hợp lệ trong giờ đó. Mỗi giờ cần **≥ 1 mẫu**; **không nội suy** khoảng trống.

Kết quả: **19 568 mẫu** (3 439 / 8 055 / 8 074), `median_gap = 60 phút` ở cả ba trạm. Việc **không nội
suy** là có chủ đích: nó giữ nguyên tính "dữ liệu thật" của chuỗi (kể cả các lỗ hổng do cảm biến mất
tín hiệu) và buộc detector phải dùng **số mẫu** trong state thay vì khoảng thời gian — nhờ vậy cùng một
đoạn code chạy được cho cả dữ liệu theo giờ và dữ liệu tổng hợp có tần suất khác.

Gọi σ_s là độ lệch chuẩn của chuỗi gốc trạm s. Với mỗi trạm, 30 bất thường được chèn (tổng **90 nhãn**:
`level_shift` 33, `spike` 32, `drift` 25), cách nhau **≥ 48 mẫu** và không rơi vào 48 mẫu đầu:

| Loại | Công thức chèn | Độ dài |
| --- | --- | --- |
| `spike` | x_t ← x_t + k·σ_s, k ~ U(4, 6) | 1 mẫu |
| `level_shift` | x_t ← x_t + 3σ_s trên cả đoạn | 6–24 mẫu |
| `drift` | x_t ← x_t + 3σ_s · j/ℓ, j = 1…ℓ | 24–48 mẫu |

Nhãn ghi khoảng `[start_ms, end_ms]` của **sự kiện**, không ghi khoảng thời gian mà detector cần để
phát hiện. Vì vậy bước chấm dùng cửa sổ `[start_ms, end_ms + 1 giờ]`, tức cho detector thêm **một mẫu
kế tiếp** — đúng bằng độ trễ tối thiểu mà một bộ lọc dựa trên cửa sổ trượt cần để "thấy" một thay đổi
mức, đồng thời bù cho việc gộp giờ có thể đẩy mẫu cuối của bất thường sang giờ kế tiếp.

### 4.2 Rolling z-score

Với mỗi khóa (`station_id`, `param`), state gồm bộ đệm `buf` (24 giá trị gần nhất) và bộ đếm `n`.
Khi nhận giá trị x_t (sau khi qua quy tắc dữ liệu trễ ở 4.4):

μ_t = (1/W)·Σ_{i=0}^{W−1} x_{t−i}, σ_t = sqrt( (1/W)·Σ_{i=0}^{W−1} (x_{t−i} − μ_t)² ), W = 24

Cảnh báo nếu `n ≥ W` **và** `σ_t > 10⁻⁹` **và** `|x_t − μ_t| / σ_t > 3,0`, với điểm số
`score = (x_t − μ_t)/σ_t`. Độ lệch chuẩn là **quần thể** (ddof = 0) để không phụ thuộc quy ước của
từng thư viện, và ngưỡng `σ > 10⁻⁹` tránh chia cho 0 trên đoạn tín hiệu phẳng. Sau khi kiểm tra mới
cập nhật bộ đệm, nên giá trị bất thường vẫn tham gia vào cửa sổ sau — đây là hành vi mong muốn: nếu
không, một `level_shift` sẽ tự "xoá dấu vết" của chính nó sau 24 mẫu.

### 4.3 EWMA (trung bình mũ có phương sai mũ)

EWMA ở đây là dạng **kiểm soát chất lượng** (control chart) chứ không chỉ là làm trơn: nó theo dõi cả
mức và độ bất định của chuỗi. State gồm `m` (trung bình mũ), `v` (phương sai mũ) và `n`:

- Khởi tạo: `m ← x_1`, `v ← 0`.
- Khi nhận x_t: đặt `d = x_t − m_{t−1}`; cảnh báo nếu `n ≥ 24` **và** `v_{t−1} > 10⁻⁹` **và**
  `|d| > L·sqrt(v_{t−1})` với `L = 3,0`, điểm số `score = d / sqrt(v_{t−1})`.
- Sau đó cập nhật: `m_t = m_{t−1} + λ·d`, `v_t = (1 − λ)·(v_{t−1} + λ·d²)`, `λ = 0,3`.

Thứ tự **kiểm tra trước, cập nhật sau** là bắt buộc: nếu cập nhật trước, mỗi bất thường sẽ kéo `m`
theo nó và làm giảm chính điểm số của mình. Dạng phương sai ở trên là dạng mũ của `E[(x−μ)²]` và tiến
tới phương sai mẫu khi chuỗi dừng; hệ số `λ = 0,3` tương ứng "tuổi thọ trung bình" ≈ 1/λ ≈ 3,3 mẫu,
đủ nhạy với `level_shift` nhưng không rung với nhiễu giờ–giờ của PM2.5.

### 4.4 Quy tắc dữ liệu đến trễ (không dùng watermark)

Hai engine xử lý watermark khác nhau (Flink theo event-time và timer, Spark theo micro-batch), nên nếu
để engine quyết định "dữ liệu nào là trễ" thì hai bên sẽ bỏ hai tập bản ghi khác nhau và F1 sẽ khác
nhau một cách giả tạo. Nhóm vì vậy cài quy tắc trễ **trong state của detector**:

```
max_et ← event_time_ms lớn nhất đã thấy của khóa
nếu event_time_ms < max_et − 7 200 000  →  BỎ (không cập nhật state, không phát bản ghi)
ngược lại                                →  max_et ← max(max_et, event_time_ms), chạy 4.2 và 4.3
```

Hệ quả kỹ thuật: Spark **không** gọi `withWatermark` và dùng `GroupStateTimeout.NoTimeout`; Flink
**không** gán timestamp/watermark (`flink.assign_timestamps: false`). Nhờ vậy hai engine có **cùng một
tập alert đầu ra** — điều đã được kiểm chứng bằng cổng tương đương (mục 5.4) và là tiền đề để so sánh
chỉ còn lại khác biệt về **độ trễ**, không phải về **kết quả**.

### 4.5 SOTA 1: Half-Space Trees (HST)

HST là phương pháp phát hiện bất thường **không giám sát, một lượt** cho luồng dữ liệu, dựa trên ý
tưởng: trong không gian nhiều chiều, mật độ dữ liệu giảm nhanh theo khoảng cách tới gốc cây, nên có thể
ước lượng "độ bất thường" bằng số điểm rơi vào các lá của một **rừng cây không gian nửa (half-space
trees)** với ngưỡng chia ngẫu nhiên. Với mỗi cây, mỗi nút chọn ngẫu nhiên một chiều và một ngưỡng; mỗi
lá lưu hai bộ đếm (cửa sổ hiện tại và cửa sổ tham chiếu). Điểm bất thường của một quan sát là tổng
mass của các lá mà nó đi qua, chuẩn hoá theo độ sâu; điểm **càng cao càng bất thường** [R1].

Cấu hình dùng trong bài (thư viện `river` [R2]):
`MinMaxScaler` → `HalfSpaceTrees(n_trees = 25, height = 15, window_size = 250, seed = 42)`, chạy
**độc lập theo từng khóa** (`station_id`, `param`). Với mỗi mẫu: gọi **`score_one` trước**, rồi mới
`learn_one` — cùng nguyên tắc "kiểm tra trước, cập nhật sau" như 4.2/4.3, để mô hình không học chính
mẫu đang cần chấm. Ngưỡng cảnh báo là **phân vị 99 %** của điểm số trên **20 % dữ liệu đầu** của mỗi
trạm (không dùng nhãn), và 24 mẫu đầu chỉ cập nhật state (warm-up). Đầu ra dùng đúng schema bản ghi
`alert` của hệ thống với `engine = "offline"`, `detector = "hst"`.

Chi phí: `height = 15` nghĩa là mỗi cây có tới 2¹⁶ − 1 nút, và 25 cây cho mỗi khóa; lần `learn_one` đầu
tiên mất ≈ 10,7 s/trạm trên máy thử. Đây là chi phí cố hữu của tham số đã chốt trong README, không phải
lỗi cài đặt.

### 4.6 SOTA 2: Matrix Profile (MP)

Matrix Profile của một chuỗi độ dài n với độ dài đoạn con m là vector `P ∈ ℝ^{n−m+1}`, trong đó `P[i]`
là khoảng cách **z-normalized** nhỏ nhất giữa đoạn con thứ i và **mọi** đoạn con khác (bỏ các đoạn
chồng lấp "tầm thường"). Giá trị `P[i]` lớn nghĩa là đoạn con thứ i **không giống bất kỳ đoạn nào
khác** — một *discord*, ứng viên bất thường mạnh [R3]. Cài đặt dùng `stumpy.stump` [R4] với `m = 24`
(đúng bằng cửa sổ z-score để hai phương pháp "nhìn" cùng một độ dài ngữ cảnh), z-normalized theo đúng
chuẩn của Yeh và cộng sự.

Vì matrix profile là phép tính **offline toàn chuỗi**, nó không có khái niệm "trước/sau" và do đó không
dùng được trực tiếp cho streaming; nó đóng vai **baseline mạnh chạy ngoài luồng** để trả lời câu hỏi
"nếu có toàn bộ dữ liệu trong tay thì chất lượng trần là bao nhiêu". Hai hậu xử lý được áp dụng:
(i) ngưỡng = phân vị 99 % của điểm số trên **20 % dữ liệu đầu**, giống hệt HST để so sánh công bằng;
(ii) **refractory = m = 24 mẫu** sau mỗi alert, vì một discord làm cho m đoạn con liên tiếp đều có điểm
cao và nếu không chặn sẽ sinh ra một chùm alert cho cùng một sự kiện.

### 4.7 Chấm chất lượng

Mỗi alert có `event_time_ms` được so với các cửa sổ nhãn `[start_ms, end_ms + 3 600 000]` của **cùng**
(`station_id`, `param`):

- **Precision** = số alert rơi vào ít nhất một cửa sổ / tổng số alert.
- **Recall** = số nhãn có ≥ 1 alert trong cửa sổ / tổng số nhãn (90).
- **F1** = 2PR/(P+R), tính **riêng cho từng cặp** (`engine`, `detector`).
- **Detection delay** = **nhỏ nhất** của `recv_ts_ms − ingest_ts_ms` trên các alert nằm trong cửa sổ, tức
  độ trễ của **alert đến sớm nhất** đo được, chỉ tính trên các bất thường đã phát hiện
  (`detection_delay_ms_median/mean/p95`, `detection_delay_n` = số cửa sổ đo được). Cả hai mốc đều lấy từ
  **chính bản ghi alert**: `recv_ts_ms` do `measure_perf.py` gắn lúc nhận từ Kafka, `ingest_ts_ms` do
  engine copy từ sự kiện gốc — nên không phụ thuộc file `sent.csv`. Với SOTA offline (không có giờ máy lúc
  nhận) cột này để trống; thay vào đó bộ chấm ghi `detection_delay_hours_event_median` =
  (event_time của alert đầu tiên − `start_ms`)/1 giờ, dùng được cho **cả bốn** phương pháp.

Một chi tiết quan trọng về cách đếm: alert trùng chỉ tính **một lần**, và khoá dedupe **không** dùng
`seq`, vì SOTA để `seq = null` và pandas coi mọi NaN là bằng nhau — lỗi này từng làm 162 alert Matrix
Profile bị gộp còn 1 dòng. Khoá đúng là
`(engine, detector, station_id, param, event_time_ms, round(score, 9))`.

---

## 5. Thiết kế thí nghiệm và độ đo

### 5.1 Phiên bản và môi trường

| Thành phần | Phiên bản | Ghi chú |
| --- | --- | --- |
| Apache Kafka | 4.2.2 (`apache/kafka:4.2.2`) | KRaft, **1 broker**, 6 partition/topic, RF = 1 |
| Apache Flink | 2.2.1 + PyFlink 2.2.1 | Kafka connector 5.0.0; Python 3.11 (`--enable-shared`) |
| Apache Spark | 4.1.3 + PySpark 4.1.3 | Scala 2.13, Java 17, Kafka connector `spark-sql-kafka-0-10_2.13:4.1.3` |
| Python | 3.11 | giống nhau ở container và máy chạy script |
| pandas / pyarrow | **2.2.3 / 19.0.0** | cặp đã kiểm chứng với pandas UDF của Spark 4.1.3 |
| `river` / `stumpy` | 0.26.1 / 1.14.1 | HST và Matrix Profile |
| Máy B (RQ0/RQ2, `docker-compose.lowmem.yml`) | AMD Ryzen 5 5600U, **6 core / 12 luồng**, 13,84 GB RAM | Windows 11 + WSL2 Ubuntu; Docker Desktop; trong WSL `nproc = 8`, `MemTotal ≈ 8,73 GiB` (`.wslconfig`: `memory=9GB`, `processors=8`, `swap=4GB`) |
| Máy đo RQ1/RQ3 ("máy A") | yêu cầu tối thiểu **8 vCPU** và **~12 GB** cấp cho WSL (đủ ~11,2 GB của `docker-compose.yml` gốc) | Windows 11 + WSL2 Ubuntu + Docker Desktop, `docker-compose.yml` gốc; **cả hai engine chạy cùng máy, cùng phiên Docker, mỗi lần chỉ một engine** |

Mọi so sánh **giữa hai engine** trong bài đều lấy từ **cùng một máy và cùng một phiên Docker**: RQ0 trên
máy B, RQ1/RQ3 trên máy đo. Số của các đợt khác máy **không** được trộn vào cùng một bảng (mục 8.1,
8.9); RQ1/RQ3 dùng số **tuyệt đối** nên chỉ so được trong cùng một máy, còn RQ0/RQ2 dùng đại lượng
**xác định** (tập alert, P/R/F1) nên không phụ thuộc máy.

### 5.2 Các biện pháp giữ công bằng tuyệt đối giữa hai engine

Đây là phần quan trọng nhất về phương pháp luận: nếu hai engine khác nhau ở bất kỳ mục nào dưới đây thì
chênh lệch đo được **không** còn là "Flink nhanh hơn Spark" mà là "cấu hình A nhanh hơn cấu hình B".

| Hạng mục | Flink | Spark |
| --- | --- | --- |
| Tài nguyên điều phối | JobManager **0,5 CPU / 1,2 GB** | Master **0,5 CPU / 1,2 GB** |
| Tài nguyên xử lý | 2 TaskManager × **2 CPU / 3,5 GB** (2 slot mỗi cái) | 2 Worker × **2 CPU / 3,5 GB** (`SPARK_WORKER_CORES=2`, `3072m` cho JVM) |
| Tổng core xử lý | **4** | **4** (`spark.cores.max = 4`, `executor.cores = 2`) |
| Parallelism | `parallelism.default = 4`, `taskmanager.numberOfTaskSlots = 2` | `spark.sql.shuffle.partitions = 4` |
| State backend | RocksDB | RocksDB (`RocksDBStateStoreProvider`) |
| Checkpoint | 10 s, `file:///checkpoints/flink/<run_id>` | 10 s, `/checkpoints/spark/<run_id>` |
| Watermark | **không** (`assign_timestamps: false`) | **không** (`withWatermark` không được gọi) |
| Thứ tự bản ghi | tự nhiên theo key (xử lý từng bản ghi) | `sort_values("seq", kind="stable")` trong mỗi nhóm |
| Input/Output | `sensor` → `alerts`, cùng Kafka, cùng 6 partition, đọc từ `earliest` | như bên trái |
| Đảm bảo giao vận | `AT_LEAST_ONCE` (RQ1, RQ2); `EXACTLY_ONCE` chỉ ở RQ3 | at-least-once (Kafka sink, `outputMode append`) |
| Warm-up bị loại | 30 s đầu (`measure.warmup_s`) | như bên trái |
| Chế độ chạy | **mỗi lần chỉ 1 engine**: `docker compose stop` engine kia trước khi `up` engine mình | như bên trái |

Ngoài ra, cả hai engine dùng **chung một file** `common/detectors_ref.py` cho các hằng số và **chung**
`config.yaml`; job Spark import trực tiếp `process_event` của bản tham chiếu, còn job Flink cài lại đúng
công thức đó trong `KeyedProcessFunction`. Tương đương được **kiểm chứng bằng máy** (mục 5.4) chứ không
chỉ bằng đọc code.

### 5.3 Định nghĩa các độ đo

Mọi độ đo về thời gian dùng **giờ máy** (wall clock) theo epoch ms và chỉ được tính trên bản ghi
`probe` (mỗi sự kiện có `seq mod 100 = 0`), trừ khi ghi chú khác.

| Độ đo | Công thức / cách tính | Vì sao định nghĩa như vậy |
| --- | --- | --- |
| Độ trễ end-to-end | `recv_ts_ms − ingest_ts_ms` trên probe; báo **p50, p95, p99** và max | `recv_ts_ms` do consumer đo gắn ⇒ đo được **cả** engine lẫn Kafka mà không cần engine tự báo cáo |
| Thông lượng | số probe nhận được mỗi giây × 100, theo `recv_ts_ms` | đo giống nhau cho hai engine, **không** dựa vào consumer lag (Spark không commit offset lên Kafka) |
| Thông lượng bền vững | mức tải cao nhất có **p99 < 1 000 ms** *và* trung vị độ trễ 30 s cuối ≤ **1,5 ×** trung vị 30 s đầu | điều kiện thứ hai phát hiện "dồn việc": p99 có thể còn thấp trong khi hàng đợi đang phình |
| Thời gian phục hồi (RQ3) | kill 1 worker ở giây 60; tính từ lúc kill đến **hết cửa sổ 5 s xấu cuối cùng**, với "xấu" = không có probe hoặc p99 ≥ 2 × p99 của 30 s trước kill. **Ngoại lệ Flink `EXACTLY_ONCE`: cửa sổ 15 s** vì sink chỉ commit theo checkpoint 10 s nên probe về theo đợt 10 s, cửa sổ 5 s xen kẽ rỗng và bị tính "xấu" sai | lấy cửa sổ xấu **cuối** (không phải cửa sổ tốt đầu) vì ngay sau kill vẫn còn worker sống gửi probe bình thường |
| Mất điện / xử lý bù (RQ3) | `outage_s` = khoảng trống **dài nhất** không nhận được probe sau kill; `resume_s` = thời điểm nhận lại probe; `catchup_s = recovery_s − resume_s` | tách "thời gian sập" (không phụ thuộc độ trễ nền, so trực tiếp được giữa hai engine) khỏi "thời gian xử lý bù backlog"; run chưa hồi phục trong lúc quan sát bị đánh dấu `censored` và báo là cận dưới (`> X s`) |
| Trùng / mất (RQ3) | theo `seq` của probe: số `seq` xuất hiện > 1 lần; số `seq` chia hết cho 100 đã gửi mà không thấy | kiểm tra tính đúng đắn, không chỉ tốc độ |
| Precision / Recall / F1 | mục 4.7 | nhãn do B chèn, dùng chung cho streaming và SOTA |
| Detection delay | `recv_ts_ms` (alert sớm nhất trong cửa sổ) − `ingest_ts_ms` (sự kiện gốc) | phản ánh độ trễ **có ích** cho người dùng, không chỉ độ trễ hạ tầng |

### 5.4 Ma trận thí nghiệm và kết quả chất lượng đã có

| Mã | Câu hỏi | Dữ liệu vào | Cấu hình | Số lần |
| --- | --- | --- | --- | --- |
| **RQ0** | Hai engine có cho **cùng kết quả** không? | `equiv_10k.parquet` (10 000 sự kiện thật, không có dữ liệu trễ) | replay 2 000 sự kiện/giây, so **tập (`seq`, `detector`)** với bản tham chiếu | 1–2 mỗi engine (+1 run nhiều micro-batch) |
| **RQ1** | Thông lượng và độ trễ đuôi khác nhau thế nào? | 1 000 trạm tổng hợp (random walk) | 4 mức **1 000 / 2 000 / 4 000 / 8 000** sự kiện/giây × 2 engine, `--mode rate` (đợt thử 5 000 của Flink bị loại, xem 8.2) | **3** mỗi mức |
| **RQ2** | Chất lượng phát hiện trên dữ liệu thật? | `openaq_hn_injected.parquet` + 5 % sự kiện bị làm trễ 1–4 vị trí | replay; chấm bằng `labels.csv` | 3 mỗi engine |
| **RQ3** | Chịu lỗi và tính đúng đắn khi có sự cố? | dữ liệu tổng hợp | kill 1 worker ở giây 60, start lại sau 10 s; Flink thêm **3 run `EXACTLY_ONCE`** | 3 mỗi engine (+3) |

**Cổng chặn (gate) trước khi đo hiệu năng.** Trước RQ1/RQ3 phải qua RQ0: so **tập (`seq`, `detector`)**
của các bản ghi `alert` giữa bản tham chiếu, Flink và Spark, yêu cầu trùng khít và `score` lệch
< 10⁻⁶. Kết quả: **đạt tuyệt đối ở cả hai engine** — **759/759** alert, `only = 0/0`,
`score_mismatch = 0`, lệch điểm số lớn nhất **3,55 × 10⁻¹⁵** khi so ba bên (riêng run Spark qua Kafka
`rq0_spark_equiv_r1`: `max_diff = 0,00 × 10⁰`), tức cách ngưỡng 10⁻⁶ mười bậc. Cổng còn được kiểm tra
theo đường khó hơn: Spark đọc dữ liệu thành **20 file** với `maxFilesPerTrigger = 1` (20 micro-batch
thật, state phải lưu/đọc lại từ **RocksDB** giữa các batch) và 5 % sự kiện bị đảo 1–4 vị trí → vẫn
`late_dropped = 544` **bằng đúng** bản tham chiếu, `only = 0/0`, `score_mismatch = 0`,
`max_diff = 0,00 × 10⁰` (`results/raw/rq0_spark_multibatch_late/check.json`, có trường `state_store =
rocksdb`). Đây là bằng chứng mạnh hơn "cùng số alert": nó loại trừ khả năng hai engine phát hai tập alert
khác nhau nhưng tình cờ bằng số lượng.

**Kiểm soát đồng hồ.** Vì toàn bộ độ trễ đo bằng hiệu hai mốc thời gian trên các tiến trình/container
khác nhau, mỗi run chạy `scripts/check_clock.sh` **trước và sau** khi đo, so đồng hồ WSL – container –
Windows với ngưỡng **50 ms**. Run nào có `clock_ok = false` trong `summary.json` bị **loại** và chạy lại
với số thứ tự mới (không sửa tay kết quả). Trong đợt thử của Flink, đồng hồ WSL2 nhảy ≈ 66 s giữa lúc
đo, nên cả 4 run `try5000` đều bị loại: `rq1_flink_try5000_r4` ghi `clock_ok = false` với **19 probe có
`emit_ts_ms < ingest_ts_ms`** và `sent.csv` nhảy **31 s**, ba run còn lại thiếu trường kiểm tra. Đây là
lý do bảng RQ1 chỉ có 4 mức 1 000 / 2 000 / 4 000 / 8 000; chi tiết được ghi vào phần Giới hạn (8.2)
thay vì che đi.

**Kết quả chất lượng đã có** (`results/summary/rq2_offline_quality.csv`, 90 nhãn):

| Phương pháp | Precision | Recall | F1 | Nhận xét |
| --- | --- | --- | --- | --- |
| Half-Space Trees (`hst`) | **0,7328** | 0,3222 | **0,4476** | Precision cao nhất: 85/116 alert đúng cửa sổ |
| z-score (luồng) | 0,2852 | 0,8778 | 0,4306 | 168/589 alert đúng cửa sổ; chênh HST chỉ 0,017 |
| EWMA (luồng) | 0,2047 | **1,0000** | 0,3398 | Recall tuyệt đối (90/90) nhưng alert lặp trên `level_shift` dài ⇒ precision thấp |
| Matrix Profile (`mp`) | 0,0494 | 0,0889 | 0,0635 | xem phân tích nguyên nhân bên dưới |

Bảng chấm trên **toàn bộ 19 568 mẫu** (mỗi phương pháp một lần chạy, `seed = 42`). Hai điều cần đọc đúng:
(i) HST hơn z-score **0,017 F1** với chỉ **90 nhãn** và **một** lần chạy, nên bài **không** kết luận HST
tốt hơn — chỉ nói HST cho precision cao nhất trong cấu hình đã chốt; (ii) xếp hạng theo F1 ở đây khác
xếp hạng theo precision, và cả hai đều phụ thuộc **cách chọn ngưỡng** (8.7).

Trường hợp MP thấp là một **phát hiện có giá trị**, không phải lỗi: 162 alert nhưng chỉ 8 rơi vào cửa
sổ nhãn, và trung vị vị trí alert là **123 giờ trước** nhãn tương ứng. Nguyên nhân là ngưỡng phân vị
99 % lấy trên 20 % dữ liệu **đầu năm** (mùa khô, nền PM2.5 cao) nên sang mùa mưa ngưỡng trở nên quá
thấp và MP cảnh báo trên các *discord tự nhiên*; cộng thêm `refractory = 24` không đủ chặn các đoạn
`level_shift` dài tới 24 mẫu. Đây là minh chứng cho luận điểm: **với phương pháp dựa trên ngưỡng, cách
hiệu chỉnh ngưỡng quan trọng ngang với bản thân thuật toán**, và một ngưỡng hiệu chỉnh trên cửa sổ dữ
liệu không đại diện sẽ phá huỷ chất lượng dù thuật toán mạnh.

### 5.5 Kết quả RQ2 (chất lượng phát hiện, 3 lần lặp mỗi engine)

Ba lần `rq2 <engine> replay` cho mỗi engine (6 partition, 2 worker × 2 core; Kafka + engine trong cùng
phiên Docker), đều qua `check_clock.sh` **trước và sau** khi đo (`clock_ok = true`) và `dup_seq = 0`.
Số dưới đây là **trung bình 3 lần**, độ lệch chuẩn mẫu ghi sau dấu ± (`results/summary/
rq2_<engine>_replay_quality.csv`, các cột `*_std`; từng lần ở `rq2_<engine>_replay_r<n>_quality.csv`):

| engine | detector | Precision | Recall | F1 | delay trung vị (ms) | delay p95 (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| `flink` | `zscore` | 0,2815 | 0,8556 | **0,4236** | **1 030 ± 1 215** | 2 033 |
| `flink` | `ewma` | 0,2032 | **1,0000** | 0,3377 | **1 009 ± 1 189** | 1 995 |
| `spark` | `zscore` | 0,2815 | 0,8556 | **0,4236** | 28 652 ± 2 624 | 32 597 |
| `spark` | `ewma` | 0,2032 | **1,0000** | 0,3377 | 28 940 ± 2 695 | 32 595 |

**Kết quả quan trọng nhất của RQ2 là một bất biến, không phải một chênh lệch.** Bốn dòng P/R/F1 ở trên
chỉ có **hai** bộ giá trị: mỗi detector cho **đúng cùng** P/R/F1 ở hai engine, vì hai engine dùng
**cùng đặc tả** (mục 4.2–4.4) và **cùng** bộ chấm (4.7) — đổi engine **không** đổi kết quả phát hiện.
Engine chỉ thay đổi **độ trễ**: trung vị **1,03 s** (Flink) so với **28,65 s** (Spark) cho z-score, tức
chênh **≈ 28 lần**, cùng chiều với RQ1 và giải thích được bằng mô hình thực thi: Spark xử lý theo
**micro-batch** (mỗi bản ghi phải chờ trigger và xếp hàng trong batch), Flink xử lý **từng bản ghi**.
Vì Spark RQ2 chạy với `docker-compose.lowmem.yml` (driver 512 MB, executor 1 024 MB) trong khi cấu hình
của Flink RQ2 khác, con số 28 lần **chỉ nên đọc là định hướng**; kết luận hiệu năng của bài dựa trên
RQ1, nơi hai engine chạy **cùng máy, cùng 4 core** (8.9).

Ba điểm đáng chú ý:

1. **Chất lượng là đại lượng xác định.** Cả 3 lần lặp của **cả hai** engine cho **cùng** P/R/F1
   (`f1_std = 0` trong cả hai file gộp); chỉ độ trễ đổi — Spark 27 740 / 25 584 / 30 987 ms, Flink
   303 / 354 / 2 433 ms (z-score) — vì luồng sự kiện và quy tắc trễ không phụ thuộc thời điểm
   chạy. Ở RQ2, lặp lại có ý nghĩa với **phân bố độ trễ**, không phải với chất lượng phát hiện —
   khác với RQ1/RQ3 nơi cả hai đại lượng đều biến thiên.
2. **Streaming ≈ offline, phần chênh có nguyên nhân.** So cùng bộ chấm, z-score giảm F1 0,4306 →
   **0,4236** (phát hiện **77** thay vì **79** trong **90** cửa sổ nhãn; alert rơi vào cửa sổ giảm
   168 → 161 trên tổng 589 → 572 alert), EWMA 0,3398 → **0,3377** (vẫn 90/90 cửa sổ). Nguyên nhân là
   **544 sự kiện bị bỏ vì trễ** theo quy tắc 4.4: chuỗi trạng thái (cửa sổ 24 mẫu của z-score, EWMA)
   lệch đi đúng ở các vị trí đó, và số alert giảm nhẹ vì alert cho sự kiện bị bỏ không bao giờ sinh ra.
   Đây là **giá phải trả của quy tắc không chờ**, không phải sai số đo — và nó **giống hệt nhau** ở
   hai engine, bằng chứng thêm cho tính xác định của phép chấm.
3. **`missing_seq = 8/196` probe (4,1 %) không phải lỗi.** Probe rơi vào sự kiện bị bỏ trễ thì không
   bao giờ tới sink; con số này **trùng bản chất** với `late_dropped` nên ở RQ2 không dùng để đánh
   giá tính đúng đắn (chỉ dùng ở RQ0/RQ3, nơi dữ liệu không bị làm trễ).

**Cách đọc độ trễ ở RQ2 (quan trọng).** Producer phát lại ở 2 000 sự kiện/giây nên **toàn bộ 19 568
sự kiện vào Kafka trong ~11–13 s**, trong khi cửa sổ đo dài 180 s và bắt đầu sau 30 s warm-up. Vì vậy
độ trễ ở đây là **thời gian xả hàng đợi**, **không** phải độ trễ trạng thái ổn định: Spark 26–31 s
(tương đương ~640–750 sự kiện/giây thực tế, dưới nhịp gửi nên hàng đợi dài ra), Flink 0,30–2,43 s
(xử lý kịp nhịp gửi nên hàng đợi gần như không hình thành; lần lặp 3 cao hơn hẳn — 2,43 s — vì có
một đợt nghẽn ngắn). Chênh lệch này vì thế **vừa phản ánh mô hình thực thi vừa phản ánh khả năng theo
kịp nhịp gửi**, và bài báo không dùng nó để suy ra độ trễ p50 ổn định. Cũng vì cửa sổ gần như rỗng sau
warm-up mà `median_last30_ms = NaN` ⇒ `drift_ok = False` ⇒ `sustainable = False` và `sent_rate_ev_s`
trống ở **cả hai** engine; các trường này **không dùng được cho RQ2** và chỉ được lấy từ RQ1. Muốn đo
độ trễ ổn định ở RQ2 phải giảm `REPLAY_SPEED` để replay trải hết cửa sổ đo.

### 5.6 Kết quả RQ1 (thông lượng và độ trễ đuôi, 3 lần lặp mỗi mức tải)

Bảng dưới là **min–max của 3 lần lặp** ở mỗi mức tải (24 run hợp lệ trong `results/summary/rq1.csv`;
mỗi mức 3 run, số thứ tự có thể không liền vì run bị loại được chạy lại với số mới). Đơn vị: ms, trừ cột
thông lượng.

| Tải gửi (sự kiện/giây) | Engine | p50 | p95 | p99 | Thông lượng thực (sự kiện/giây) | Probe mất |
| --- | --- | --- | --- | --- | --- | --- |
| 1 000 | Flink | 329 – 438 | 563 – 1 032 | **705 – 1 236** | 998 – 1 002 | 0 |
| 1 000 | Spark | 5 713 – 7 507 | 10 072 – 24 628 | 16 048 – 32 696 | 1 073 – 1 135 | 0 |
| 2 000 | Flink | 386 – 499 | 815 – 1 227 | **1 049 – 1 975** | 1 997 – 2 004 | 0 |
| 2 000 | Spark | 3 970 – 7 491 | 5 764 – 27 377 | 7 489 – 35 424 | 2 059 – 2 236 | 0 |
| 4 000 | Flink | 4 519 – 8 305 | 19 017 – 53 838 | 27 402 – 66 386 | **3 465 – 3 884** | 0 |
| 4 000 | Spark | 3 175 – 7 654 | 5 467 – 23 779 | 8 098 – 31 167 | 4 150 – 4 503 | 0 |
| 8 000 | Flink | 4 465 – 34 273 | 46 440 – 111 653 | 57 949 – 136 142 | **4 584 – 7 413** | **0 – 2 952** |
| 8 000 | Spark | 7 263 – 8 659 | 17 477 – 27 518 | 24 246 – 34 852 | **8 377 – 9 096** | 0 |

Bốn kết luận, kèm điều kiện đọc:

1. **Flink có đuôi độ trễ dưới 2 s khi tải ≤ 2 000 sự kiện/giây.** p99 = 705 – 1 236 ms ở 1 000 và
   1 049 – 1 975 ms ở 2 000, tức còn **cách ngưỡng 2 s** khá xa. Đây là chế độ "bình thường" của Flink:
   xử lý từng bản ghi nên hàng đợi gần như không hình thành.
2. **Flink bão hoà giữa 2 000 và 4 000 sự kiện/giây.** Ở nhịp gửi 4 000, thông lượng thực chỉ còn
   **3 465 – 3 884** trong khi p99 nhảy lên **27 – 66 s** và p50 lên 4,5 – 8,3 s: hàng đợi tích luỹ liên
   tục chứ không phải một đỉnh ngắn. Ở 8 000 thì **2/3 run mất probe** (1 218 và 2 952 trên 16 800 probe)
   — mất dữ liệu thật, không chỉ chậm. Với 4 core, trần thực tế của Flink trong bài là
   **≈ 3,5 – 3,9 nghìn sự kiện/giây**.
3. **Spark giữ được nhịp 8 000 sự kiện/giây nhưng trả giá bằng độ trễ.** Thông lượng thực 8 377 – 9 096
   và **không mất probe nào**, nhưng **p50 đã là 7,3 – 8,7 s, p99 24 – 35 s**; ngay ở 1 000 sự kiện/giây
   p50 đã 5,7 – 7,5 s. Độ trễ gần như **không phụ thuộc tải** — đúng dấu hiệu của micro-batch: mỗi bản
   ghi chờ ít nhất một chu kỳ trigger, cộng thời gian nằm trong hàng đợi của batch.
4. **Hai engine tối ưu cho hai thứ khác nhau.** Flink thắng ở **đuôi độ trễ** (p99 < 2 s so với
   7,5 – 35 s ở cùng tải thấp), Spark thắng ở **thông lượng bền vững**. Câu trả lời cho RQ1 vì thế không
   phải "engine nào nhanh hơn" mà là **đánh đổi latency–throughput**: cần phản hồi dưới 2 s thì Flink phù
   hợp tới ~2 000 sự kiện/giây; phải giữ 8 000 sự kiện/giây và chấp nhận cảnh báo trễ hàng chục giây thì
   Spark mới khả thi.

**Vì sao độ trễ của Spark lớn: tách theo chặng.** `sink_p50_ms` = trung vị `recv_ts_ms − emit_ts_ms`,
tức chặng **từ lúc engine phát ra sink đến lúc sink thấy** (không gồm thời gian chờ trong Kafka):

| Engine | `sink_p50_ms` (mọi mức tải) | p50 end-to-end ở 1 000 sự kiện/giây |
| --- | --- | --- |
| Flink | 256 – 272 ms | 329 – 438 ms |
| Spark | 713 – 1 456 ms | 5 713 – 7 507 ms |

Với Flink hai con số gần nhau ⇒ phần lớn độ trễ là thời gian chờ hợp lý trên đường đi. Với Spark, chặng
nội bộ tuyến chỉ ~1 s nhưng end-to-end 6 – 8 s ⇒ **phần lớn độ trễ nằm trước `emit`**: bản ghi đã vào
Kafka nhưng còn nằm chờ trong chu kỳ micro-batch. Đây là bằng chứng định lượng cho mô hình thực thi nêu
ở 5.5, không phải suy đoán.

**Ba điều không được đọc sai từ bảng RQ1.** (i) `throughput_mean` của Spark ở 8 000 là 8 377 – 9 096,
**cao hơn** nhịp gửi, vì thông lượng tính trên số probe **nhận được trong cửa sổ đo** sau warm-up chứ
không phải công suất xử lý ổn định — không được đọc thành "Spark xử lý được 9 000 sự kiện/giây".
(ii) Cột `sustainable` = `False` ở gần như **mọi** run (kể cả Flink 1 000 sự kiện/giây với p99 705 ms),
vì điều kiện thứ hai của định nghĩa (trung vị 30 s cuối ≤ 1,5 × trung vị 30 s đầu) hiếm khi thoả trong
cửa sổ 180 s có warm-up; bài vì thế dùng **`throughput_mean` + `missing_seq`** làm tiêu chí, không dùng
cờ này. (iii) Mọi số trong bảng là **số tuyệt đối của máy đo** (8.9); so sánh chỉ có hiệu lực giữa hai
engine trên cùng máy, không suy ra được cho cluster thật.

Hai hình đi kèm: `results/plots/rq1_rates.png` (p50/p95/p99 theo mức tải, hai engine) và
`results/plots/rq1_throughput_latency.png` (thông lượng thực so với nhịp gửi).

### 5.7 Kết quả RQ3 (chịu lỗi: kill 1 worker ở giây 60)

Chín run, ba chế độ giao vận; số dưới là **trung vị của 3 lần lặp** (`results/summary/rq3_detail.csv`,
dựng bởi `scripts/rq3_report.py`). Run bị `censored` được tính bằng **thời gian quan sát** nên là **cận
dưới**, ghi rõ trong bảng.

| Chế độ | Mất kết nối `outage_s` | Thời gian phục hồi | Xử lý bù `catchup_s` | Trùng (`dup_seq`) | Mất (`missing_seq`) |
| --- | --- | --- | --- | --- | --- |
| Spark (`spark-ckpt`, cửa sổ 5 s) | **13,0** (8,3 – 15,0) | **25 s** (20 – 25) | 9,7 (6,0 – 16,7) | **0** | **0** |
| Flink `AT_LEAST_ONCE` (cửa sổ 5 s) | 24,8 (23,0 – 29,0) | **145 s** (130 – `> 156`) | 110,9 (106,3 – 115,4) | **40 – 1 786** | **0** |
| Flink `EXACTLY_ONCE` (cửa sổ 15 s) | 38,9 (26,5 – 48,6) | **120 s** (60 – 135) | 86,4 (21,1 – 93,5) | **0** | **0** |

Đọc bảng này theo ba tầng:

1. **Không mất dữ liệu ở cả 9/9 run** (`missing_seq = 0`, đối chiếu với `sent.csv`): cả ba chế độ đều
   **không đánh mất** probe nào sau sự cố. Đây là kết quả quan trọng nhất của RQ3 — nó cho phép so sánh
   thời gian phục hồi mà không phải kèm điều kiện "nhưng đã mất dữ liệu".
2. **Trùng bản ghi là cái giá của `AT_LEAST_ONCE`, và `EXACTLY_ONCE` trả giá bằng thời gian.** Flink ALO
   sinh **40 / 178 / 1 786** probe trùng trong 3 lần lặp (`dup_extra` tới 2 015): sau restart, source phát
   lại từ checkpoint gần nhất trong khi sink đã ghi một phần ⇒ bản ghi được xử lý và ghi lại. Flink EO
   giữ **0 trùng** ở cả 3 run, đổi lại `catchup_s` biến thiên rất mạnh (21 – 94 s) và thời gian phục hồi
   **không thấp hơn** ALO (120 s so với 145 s — nhưng xem lưu ý (ii) bên dưới về cửa sổ đo).
3. **`outage_s` mới là đại lượng so sánh được giữa hai engine.** Spark mất kết nối **13 s**, Flink
   **24,8 s** (ALO) và **38,9 s** (EO): Spark giữ driver sống và chỉ khởi động lại worker, còn Flink phải
   khởi động lại TaskManager, nạp lại state từ RocksDB và replay từ checkpoint. `recovery_s` thì **không**
   so sánh trực tiếp được, vì ngưỡng của nó là `p99 ≥ 2 × p99 nền` và **nền khác nhau**: p99 nền 30 s
   trước kill của Spark là 6,36 – 9,31 s, của Flink ALO chỉ 1,54 – 3,44 s ⇒ ngưỡng của Spark cao hơn
   ~3 lần (12,7 – 18,6 s so với 3,1 – 6,9 s). Hệ thống có nền cao hơn "hồi phục" sớm hơn **theo đúng
   định nghĩa**, không hẳn vì nó khoẻ hơn.

**Ba lưu ý bắt buộc khi trích dẫn RQ3.** (i) Run `rq3_flink_rate2000_r2` **chưa hồi phục** trong 156 s
quan sát (`censored = True`, `observed_s = 156`), nên giá trị 145 s ở cột phục hồi là **trung vị có cận
dưới**, không phải "cả 3 run đều phục hồi sau 145 s". (ii) `EXACTLY_ONCE` đo bằng **cửa sổ 15 s** thay vì
5 s vì sink chỉ commit theo checkpoint 10 s nên probe về theo đợt và cửa sổ 5 s xen kẽ rỗng; nếu tính
bằng 5 s thì `recovery_s` của EO bị **thổi phồng giả tạo** (run `…-eo_r1`: 150 s thay vì 120 s). Hai chế
độ vì thế không so được với nhau bằng một con số duy nhất — so bằng cặp **`outage_s` + `catchup_s`**.
(iii) Kill ở **giây 60**, worker được start lại sau **10 s**; với 2 000 sự kiện/giây, backlog tích luỹ
trong lúc sập là 50 – 100 nghìn sự kiện, nên `catchup_s` phản ánh **tốc độ xử lý bù**, không phải "thời
gian khởi động lại" — đây chính là lý do phải tách `outage_s` khỏi `recovery_s` như mục 5.3.

Hai hình đi kèm: `results/plots/rq3_timeline.png` (p99 trượt quanh thời điểm kill) và
`results/plots/rq3_breakdown.png` (outage / catch-up theo từng run). Hình dạng "p99 dựng đứng rồi tụt
từ từ" của Flink so với "một đỉnh hẹp" của Spark là hình ảnh trực quan của hai mô hình phục hồi.

---

## 8. Giới hạn của nghiên cứu

**8.1. Máy cá nhân, chạy trong ảo hoá.** Thí nghiệm chạy trên **máy cá nhân** với Docker Desktop trên
**WSL2**, không phải cluster thật; và trên **hai máy** (chi tiết ở 8.9): RQ0/RQ2 trên **máy B** (AMD
Ryzen 5 5600U, 6 core / 12 luồng, 13,84 GB RAM, `docker-compose.lowmem.yml`), RQ1/RQ3 trên **máy đo**
riêng dùng `docker-compose.yml` gốc (8 vCPU và ~12 GB cấp cho WSL — đủ cho ~11,2 GB mà compose yêu cầu).
CPU/RAM của container chia sẻ với Windows và với chính WSL2, nên con số tuyệt đối (sự kiện/giây, ms)
**không** suy ra được cho môi trường phân tán nhiều máy. Kết quả chỉ có ý nghĩa **so sánh tương đối**
giữa hai engine trong cùng điều kiện — và điều kiện đó được giữ đúng: mỗi so sánh chỉ dùng số của **một**
máy.

**8.2. Đồng hồ có thể trôi.** Mọi độ trễ được tính bằng hiệu hai mốc giờ máy lấy từ các tiến trình và
container khác nhau. Trong đợt thử của Flink, đồng hồ WSL2 nhảy ≈ **66 s** giữa lúc đo, làm hỏng số liệu
của 4 run `try5000` (`rq1_flink_try5000_r4` ghi `clock_ok = false`, **19 probe có `emit_ts_ms <
ingest_ts_ms`**, `sent.csv` nhảy **31 s**; 3 run còn lại thiếu trường kiểm tra nên cũng bị loại). Nhóm
đã xử lý bằng cách chạy `check_clock.sh` (ngưỡng 50 ms) **trước và sau** mỗi run và loại mọi run có
`clock_ok = false`; bảng RQ1 vì thế chỉ còn 4 mức 1 000 / 2 000 / 4 000 / 8 000 với 3 lần lặp mỗi mức.
Tuy vậy, một đợt trôi nhỏ trong lúc đo (dưới ngưỡng phát hiện) vẫn có thể làm p50 lệch vài chục ms — sai
số này ảnh hưởng **như nhau** cho hai engine nên không đảo ngược kết luận, nhưng không nên đọc các chênh
lệch p50 quá nhỏ.

**8.3. Số lần lặp ít.** Mỗi cấu hình chạy **3 lần**; bài báo báo cáo trung bình kèm **min–max** chứ
không dùng kiểm định thống kê. Với 3 mẫu, chênh lệch dưới ~10–15 % giữa hai engine nên coi là **chưa
phân biệt được**; chỉ nên kết luận khi khoảng min–max của hai bên **không chồng lấn**.

**8.4. PyFlink ≠ Flink.** Job Flink ở đây là **PyFlink chạy ở thread mode**
(`python.execution_mode: thread`) và **không gán timestamp/watermark**, cấu hình được chọn vì output
không đổi mà nhanh hơn khoảng 2,5 lần. Hệ quả: mỗi bản ghi vẫn phải qua **cầu nối Java–Python**, trong
khi PySpark `applyInPandasWithState` xử lý theo **lô pandas** trong mỗi micro-batch. Vì vậy kết quả
"Flink có thông lượng cao nhất" trong ShuffleBench [R9] — đo trên **cài đặt JVM** của bốn framework —
**không** dùng để dự đoán cho thí nghiệm này; ngược lại, ở đây Spark đạt nhịp gửi cao hơn hẳn
(8 000 sự kiện/giây so với 3,5–3,9 nghìn của Flink ở cùng 4 core). Bài báo chỉ kết luận trong phạm vi
**Python API của hai engine**, và coi chênh lệch này là chi phí của **cầu nối ngôn ngữ**, không phải của
engine.

**8.5. Bất thường là tự chèn.** 90 nhãn do nhóm sinh ra theo công thức bội số của σ (mục 4.1), nên độ
khó khác với bất thường thật. Dữ liệu OpenAQ gốc **có thể đã chứa bất thường thật nhưng không được gắn
nhãn** (ví dụ các đợt ô nhiễm thật), khi đó alert đúng vẫn bị tính là sai ⇒ **Precision bị đánh giá
thấp hơn thực tế**. Ngoài ra nhãn không phân biệt mùa, nên không đánh giá được độ ổn định của detector
theo mùa.

**8.6. Phạm vi dữ liệu hẹp.** Ba trạm, một tham số (PM2.5), một năm, tần suất 1 mẫu/giờ; không có thử
nghiệm với dữ liệu tần suất cao (5 phút) hay dữ liệu thiếu theo cụm. Quy tắc "không nội suy" giữ được
tính thật của dữ liệu nhưng cũng làm các khoảng trống dài trở thành bước nhảy lớn trong chuỗi.

**8.7. Hiệu chỉnh ngưỡng SOTA không dùng nhãn.** Cả HST và MP đều lấy ngưỡng là phân vị 99 % trên 20 %
dữ liệu đầu — lựa chọn **công bằng** giữa hai phương pháp nhưng **không tối ưu** cho từng phương pháp,
và nhạy với phân bố mùa vụ (mục 5.4). Vì vậy so sánh SOTA trong bài mang tính **cấu hình cụ thể**, không
phải "giới hạn trên" của từng thuật toán.

**8.8. Chỉ một điểm lỗi, một broker.** RQ3 chỉ kill **một worker** của engine; không khảo sát mất
JobManager/master, không khảo sát mất broker (Kafka cấu hình **1 broker, RF = 1**). Vì vậy phần chịu lỗi
chỉ trả lời được câu hỏi "mất một phần năng lực xử lý thì sao", không trả lời câu hỏi về tính sẵn sàng
của toàn hệ thống.

**8.9. Hai đợt đo, hai máy — cái gì so sánh được.** RQ0/RQ2 chạy trên **máy B** với
`docker-compose.lowmem.yml` (Kafka 1,5 GB, driver 512 MB, executor 1 024 MB) vì WSL của máy B chỉ có
8,73 GiB; RQ1/RQ3 chạy trên **máy đo** với `docker-compose.yml` gốc. Hệ quả cần nêu rõ: (a) số **tuyệt
đối** của RQ1/RQ3 (sự kiện/giây, ms, giây phục hồi) **không** được so với số độ trễ RQ2 của máy B —
riêng chênh lệch "Flink 1,03 s so với Spark 28,65 s" ở RQ2 vì thế chỉ là **định hướng**; (b) ngược lại,
các đại lượng **xác định** — tập alert RQ0, P/R/F1 và số alert RQ2 — **không phụ thuộc máy** (đã kiểm
chứng: F1 trùng khít tuyệt đối qua 3 lần lặp trên cả hai engine), nên các kết luận về **chất lượng phát
hiện** và **tương đương ngữ nghĩa** là an toàn; (c) mỗi so sánh **giữa hai engine** luôn nằm trong **một
máy**: RQ0/RQ2 cùng máy B, RQ1/RQ3 cùng máy đo, và trong mỗi phiên chỉ **một** engine chạy. Bảng đối
chiếu số → file nguồn ở README mục 10.6 ghi kèm máy đo cho từng dòng.

**8.10. Cách phát lại dữ liệu ở RQ2 tạo ra "burst".** Producer phát lại theo **sự kiện/giây**, không
theo khoảng cách thời gian gốc, nên ở 2 000 sự kiện/giây toàn bộ 19 568 sự kiện của một năm dữ liệu
giờ dồn vào Kafka trong **~11 s** thay vì trải trên cửa sổ đo 180 s. Hệ quả: (a) độ trễ đo được là
**thời gian xả hàng đợi**, không phải độ trễ trạng thái ổn định; (b) các trường `throughput` và
`sustainable` của `rq2.csv` **không dùng được** (cửa sổ gần rỗng sau warm-up); (c) độ trễ RQ2 **không
so sánh trực tiếp** được với RQ1 vốn dùng `--mode rate`. Ba lần lặp cùng tốc độ nên vẫn so sánh được
với nhau. Muốn đo độ trễ ổn định cho RQ2 phải chọn `REPLAY_SPEED` sao cho thời gian phát lại xấp xỉ
cửa sổ đo (ví dụ ~110 sự kiện/giây cho 180 s) — việc này chưa thực hiện trong đợt đo này.

**8.11. Cắt giảm so với kế hoạch ban đầu.** Đề xuất 4 tuần dự kiến có thêm: cài đặt Java để so với bản
Python, chạy trên Kubernetes nhiều node, đo autoscaling, và dashboard giám sát trực tiếp. Các mục này bị
cắt để kịp tiến độ; chúng được nêu ở phần Hướng phát triển thay vì được đo trong bài.

---

## 9. Kết luận và hướng phát triển

Bài báo đã xây dựng một **khung so sánh có thể tái lập** giữa Flink và Spark Structured Streaming cho
bài toán phát hiện bất thường trên luồng dữ liệu môi trường, với bốn đặc điểm phương pháp luận. Thứ
nhất, **ngữ nghĩa thuật toán được tách khỏi engine**: quy tắc dữ liệu trễ 2 giờ và ngưỡng phát hiện được
cài trong state, không dựa vào watermark, nên hai engine cho **cùng tập alert** — chứng minh bằng cổng
tương đương **759/759** alert, `only = 0/0`, lệch điểm số lớn nhất **3,55 × 10⁻¹⁵**, và bền cả khi Spark
đọc dữ liệu thành **20 micro-batch** trên RocksDB với 5 % sự kiện bị đảo vị trí (**544 sự kiện bị bỏ vì
trễ, bằng đúng** bản tham chiếu). Thứ hai, **so sánh được kiểm soát**: cùng 4 core, 2 × 3,5 GB cho phần
xử lý, cùng parallelism 4, cùng checkpoint 10 s, cùng RocksDB, mỗi lần chỉ bật một engine — và mỗi so
sánh chỉ dùng số của **một máy** (8.9). Thứ ba, **số đo độ trễ được bảo vệ**: kiểm tra đồng hồ trước/sau
mỗi run và loại run có đồng hồ lệch (`rq1_flink_try5000_r4`), thay vì báo cáo những con số không kiểm
chứng được. Thứ tư, **chất lượng phát hiện được đo bằng đại lượng xác định**: cùng một bộ chấm chạy trên
cùng nhãn, nên lặp lại chỉ làm rõ **phân bố độ trễ**, không làm đổi P/R/F1.

Trả lời trực tiếp bốn câu hỏi nghiên cứu. **RQ0:** hai engine **tương đương tuyệt đối** với bản tham
chiếu độc lập — không chỉ "cùng số alert" mà **cùng tập** `(seq, detector)`, sai khác điểm số ở mức
**3,55 × 10⁻¹⁵**, tức cách ngưỡng chấp nhận 10⁻⁶ mười bậc. **RQ1:** không có engine "nhanh hơn" toàn diện mà
là **đánh đổi latency–throughput** — Flink giữ p99 **dưới 2 s** tới 2 000 sự kiện/giây nhưng bão hoà ở
**3,5 – 3,9 nghìn sự kiện/giây** (và mất probe ở 8 000), trong khi Spark giữ được **8 000 sự kiện/giây**
với cái giá p50 **7 – 9 s**, p99 **24 – 35 s**. **RQ2:** chất lượng phát hiện **bất biến theo engine**
(F1 **0,4236** cho z-score và **0,3377** cho EWMA ở **cả hai**), chỉ **detection delay** khác: trung vị
**1,03 s** so với **28,65 s**; và chất lượng streaming bám sát bản ngoài luồng (0,4306 → 0,4236), với
Half-Space Trees đạt **F1 = 0,4476** và **Precision = 0,7328** — cao nhất trong bốn phương pháp.
**RQ3:** cả **9/9** run **không mất dữ liệu**; `AT_LEAST_ONCE` sinh **40 – 1 786** bản ghi trùng và phục
hồi **145 s**, `EXACTLY_ONCE` **0 trùng** với **120 s**, Spark phục hồi **25 s**, **0 trùng**; thời gian
**mất kết nối** (13 s so với 24,8 s và 38,9 s) là đại lượng so sánh được giữa hai engine.

Về SOTA ngoài luồng, **Matrix Profile** chỉ đạt **F1 = 0,0635**, và phân tích cho thấy nguyên nhân nằm ở
**ngưỡng hiệu chỉnh trên 20 % dữ liệu đầu** cùng `refractory` ngắn — một bài học về vai trò của hiệu
chỉnh ngưỡng trong các phương pháp không giám sát, đồng thời là gợi ý trực tiếp cho bước tinh chỉnh tiếp
theo. Cần nhấn mạnh rằng chênh lệch HST – z-score chỉ **0,017 F1** trên **90 nhãn** và **một** lần chạy,
nên bài **không** tuyên bố phương pháp nào "tốt hơn" nói chung.

Hướng phát triển: (i) **hiệu chỉnh ngưỡng thích nghi** (phân vị trượt, hoặc học ngưỡng trên khoảng dữ
liệu đại diện cả năm) cho MP và HST, kèm ablation `--causal` cho MP; (ii) **cài đặt Java** cho cả hai
engine để tách chi phí cầu nối Java–Python khỏi chi phí engine — với p99 của Flink chỉ 705 – 1 236 ms ở
1 000 sự kiện/giây, chi phí Python UDF là ứng viên hàng đầu cho phần bị mất khi tăng tải; (iii) chạy
trên **cluster nhiều máy, Kafka nhiều broker**, khảo sát thêm kịch bản mất coordinator — vì `outage_s`
của Flink chịu ảnh hưởng trực tiếp của việc khởi động lại TaskManager; (iv) **hiệu chỉnh `REPLAY_SPEED`
ở RQ2** để phép replay trải hết cửa sổ đo, nhờ đó đo được **detection delay ở trạng thái ổn định** thay
vì thời gian xả hàng đợi (8.10); (v) mở rộng sang nhiều tham số và nhiều trạm hơn, đánh giá độ ổn định
theo mùa; (vi) đưa **detection delay** vào so sánh có trọng số cùng chi phí tính toán, vì với người dùng
cuối, một cảnh báo sớm hơn vài phút có thể quan trọng hơn vài phần trăm F1.

---

## Tài liệu tham khảo (nháp — phải mở bài gốc kiểm tra trước khi nộp)

- [R1] S. Tan, K. M. Ting, T. F. Liu, "Fast Anomaly Detection for Streaming Data," *Proc. IJCAI*, 2011,
  pp. 1511–1516. (Half-Space Trees)
- [R2] G. Montiel *et al.*, "River: Machine Learning for Streaming Data in Python," *JMLR*, vol. 22,
  no. 110, pp. 1–8, 2021. (thư viện `river`)
- [R3] C.-C. M. Yeh *et al.*, "Matrix Profile I: All Pairs Similarity Joins for Time Series — A Unifying
  View That Includes Motifs, Discords and Shapelets," *Proc. IEEE ICDM*, 2016, pp. 1317–1322.
- [R4] S. M. Law, "STUMPY: A Powerful and Scalable Python Library for Time Series Data Mining,"
  *JOSS*, vol. 4, no. 39, p. 1504, 2019.
- [R5] J. Kreps, N. Narkhede, J. Rao, "Kafka: a Distributed Messaging System for Log Processing,"
  *Proc. NetDB*, 2011.
- [R6] P. Carbone *et al.*, "Apache Flink: Stream and Batch Processing in a Single Engine," *IEEE Data
  Engineering Bulletin*, vol. 38, no. 4, pp. 28–38, 2015.
- [R7] M. Zaharia *et al.*, "Structured Streaming: A Declarative API for Real-Time Applications in
  Apache Spark," *Proc. ACM SIGMOD*, 2018, pp. 601–613.
- [R8] S. W. Roberts, "Control Chart Tests Based on Geometric Moving Averages," *Technometrics*, vol. 1,
  no. 3, pp. 239–250, 1959. (nguồn gốc EWMA)
- [R9] **ShuffleBench** (benchmark so sánh các stream processing framework) — *cần tra lại đúng tên tác
  giả, hội nghị và năm trước khi trích dẫn*; hiện chỉ dùng để nêu luận điểm "kết quả Flink nhanh nhất là
  với code Java" trong mục 8.4.

---

## Ghi chú cho bản nộp (không đưa vào bài)

1. **Chờ số liệu để điền:** bảng RQ1 (thông lượng vs p99 cho `flink`/`spark`) và bảng RQ3 (thời gian phục
   hồi, trùng/mất). Hình đã có script: `python scripts/plot_results.py`.
2. **Đã có sẵn, dùng được ngay:** bảng chất lượng RQ2 (mục 5.4), cổng tương đương 759/759, mô tả dữ liệu
   + quy tắc làm sạch (mục 4.1), toàn bộ mục 8.
3. Các từ "cao nhất/thấp nhất" trong bài chỉ so trong **4 phương pháp của bài này**, không phải kết luận
   chung cho toàn bộ tài liệu.
4. Mọi số trong bài lấy từ `results/summary/*.csv` và `docs/handover_B.md`; nếu chạy lại thí nghiệm phải
   cập nhật cả hai nơi.




