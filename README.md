# P3 – Real-Time Anomaly Detection on Environmental Sensor Streams

So sánh **Apache Flink** và **Spark Structured Streaming** khi nhận dữ liệu qua Kafka. Hai engine chạy cùng một logic phát hiện bất thường trên dữ liệu PM2.5 Hà Nội (OpenAQ). Kết quả được đối chiếu với hai phương pháp SOTA chạy offline.

> README này là **bản quy ước dùng chung** của nhóm (chốt ngày 23/10/2026).
> Mọi thứ trong đây cả hai người phải theo y hệt.
> Muốn đổi thì sửa README hoặc `config.yaml` trong một commit riêng, nhắn người kia **trước khi** đổi code.

| Thành viên | Phụ trách |
| --- | --- |
| **A** | Flink, hạ tầng Docker/Kafka, producer chế độ `rate`, `measure_perf.py`, SOTA Matrix Profile |
| **B** | Spark, dữ liệu OpenAQ + nhãn, producer chế độ `replay`, `measure_quality.py`, SOTA Half-Space Trees |

---

## 1. Phiên bản (đã chốt)

| Thành phần | Phiên bản | Ghi chú |
| --- | --- | --- |
| Apache Kafka | **4.2.2**, image `apache/kafka:4.2.2` | Chỉ chạy KRaft (Kafka 4.x không còn ZooKeeper), 1 broker |
| Apache Flink | **2.2.1** + PyFlink `apache-flink==2.2.1` | Image `flink:2.2.1` (Java 17). Image gốc **không có Python**, phải tự build `docker/Dockerfile.flink` |
| Flink Kafka connector | **5.0.0** (bản cho Flink 2.1/2.2) | Tải jar `flink-sql-connector-kafka` 5.0.0 từ Maven Central vào `docker/jars/` |
| Apache Spark | **4.1.3**, PySpark `pyspark==4.1.3` | Scala 2.13, Java 17. Kafka source: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3` |
| Python | **3.11** | Dùng cùng phiên bản trong mọi container và trên máy chạy script |
| Thư viện Python | `confluent-kafka`, `pandas`, `pyarrow`, `pyyaml`, `numpy`, `stumpy`, `river`, `matplotlib`, `boto3` | Ghim bản cụ thể trong `requirements.txt` sau khi cài xong ngày 1 (`pip freeze`) |

**Vì sao chọn các bản này:**

- Flink 2.3.0 là bản mới nhất, nhưng trang download chưa có Kafka connector cho 2.3. Vì vậy nhóm dùng 2.2.1, bản mới nhất đã có connector (5.0.0).
- Spark 4.1.3 là bản vá của nhánh 4.1. Nhánh này đã ổn định, có `applyInPandasWithState` và state store RocksDB.

**Phương án dự phòng (A quyết trước 12:00 ngày 1):** nếu PyFlink 2.2.1 hoặc connector 5.0.0 không chạy được, chuyển sang Flink **1.20.5 (LTS)** + connector **3.4.0**. Chuyển rồi thì ghi lại vào README.

---

## 2. Cấu trúc repo và người phụ trách

```
p3-streaming/
├── README.md                 # file này
├── config.yaml               # nguồn duy nhất của mọi tham số (mục 4)
├── requirements.txt          # A tạo ngày 1
├── docker-compose.yml        # A: kafka, flink-jm, flink-tm x2 ; B: spark-master, spark-worker x2
├── docker/
│   ├── Dockerfile.flink      # A: flink:2.2.1 + python3.11 + apache-flink + jar connector
│   ├── Dockerfile.spark      # B
│   └── jars/
├── common/
│   ├── schema.py             # A: parse/serialize sự kiện + alert, kiểm tra trường
│   ├── config.py             # A: đọc config.yaml
│   └── detectors_ref.py      # B: bản cài đặt THAM CHIẾU bằng Python thuần (mục 6)
├── data/
│   ├── download_openaq.py    # B
│   ├── inject_anomalies.py   # B -> data/processed/openaq_hn_injected.parquet + labels.csv
│   └── processed/            # không commit file > 50 MB
├── producer/producer.py      # A: --mode rate ; B: --mode replay
├── flink_job/job.py          # A
├── spark_job/job.py          # B
├── sota/
│   ├── matrix_profile.py     # A
│   └── half_space_trees.py   # B
├── measure/
│   ├── measure_perf.py       # A
│   ├── measure_quality.py    # B
│   └── compare_outputs.py    # B: so output Flink, Spark, bản tham chiếu
├── scripts/
│   ├── create_topics.sh      # A
│   └── run_experiment.sh     # B
├── results/                  # mục 10.4
├── figures/
├── report/  slides/
└── docs/ai-log.md            # mục 11
```

---

## 3. Kafka

| Mục | Quy ước |
| --- | --- |
| Topic vào | `sensor`, 6 partition, RF = 1 |
| Topic ra | `alerts`, 6 partition, RF = 1 |
| Key của message | `station_id` (UTF-8). Mọi sự kiện của một trạm vào cùng một partition, nên **giữ đúng thứ tự gửi** |
| Định dạng | JSON UTF-8, mỗi message một object |
| Địa chỉ | Trong mạng docker dùng `kafka:9092`; từ máy host dùng `localhost:29092` |
| Mỗi lần chạy | `run_experiment.sh` **xóa rồi tạo lại** cả hai topic và xóa thư mục checkpoint |
| Offset bắt đầu | Cả hai engine đọc từ `earliest` |
| Consumer group | `flink-job`, `measure-<run_id>`. Spark **không** commit offset lên Kafka, vì nó lưu offset trong checkpoint. Do đó metric không được dựa vào consumer lag (xem 10.2) |

---

## 4. Tham số dùng chung

Toàn bộ tham số nằm trong **`config.yaml`**. Code không được hard-code các giá trị đó. Các giá trị chính:

| Tham số | Giá trị |
| --- | --- |
| Key của state | (`station_id`, `param`) |
| Warm-up mỗi key | 24 mẫu đầu chỉ cập nhật state, không phát cảnh báo |
| Z-score | cửa sổ 24 giá trị trước, ngưỡng \|z\| > 3.0, std quần thể (ddof = 0) |
| EWMA | λ = 0.3, L = 3.0 |
| Ngưỡng trễ | 2 giờ event-time (`tolerance_ms = 7 200 000`) |
| Probe | mỗi sự kiện có `seq % 100 == 0` |
| Parallelism | 4 (Flink: 2 TM × 2 slot; Spark: 2 worker × 2 core, `shuffle.partitions = 4`) |
| Checkpoint | 10 giây, state backend RocksDB ở cả hai engine |
| Tài nguyên | Mỗi engine tổng cộng 4 core + 8 GB. **Mỗi lần chỉ bật một engine** |

---

## 5. Schema dữ liệu

### 5.1 Sự kiện vào (topic `sensor`)

```json
{"v": 1, "station_id": "hn_01", "param": "pm25", "value": 87.4,
 "event_time_ms": 1767600000000, "ingest_ts_ms": 1767600000123, "seq": 1000000012345}
```

| Trường | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `v` | int | Phiên bản schema, hiện là 1 |
| `station_id` | string | `hn_01`, `hn_02`… (OpenAQ) hoặc `syn_0000`…`syn_0999` (dữ liệu tổng hợp) |
| `param` | string | `pm25` |
| `value` | float | µg/m³. **Không có giá trị null**: dữ liệu thiếu đã bị bỏ từ bước làm sạch |
| `event_time_ms` | int | Thời điểm đo, epoch ms UTC |
| `ingest_ts_ms` | int | Giờ máy (wall clock) lúc producer gửi, epoch ms. Dùng để đo độ trễ |
| `seq` | int | Duy nhất toàn cục: `producer_id * 10^12 + bộ đếm`. Mỗi producer có `producer_id` riêng (0, 1, 2…). **Gán lúc GỬI**, nên sự kiện bị giữ lại để gửi trễ sẽ nhận `seq` lớn hơn. Spark sắp xếp theo `seq`, vì vậy cách gán này đảm bảo Spark xử lý đúng thứ tự đến như Flink |

> **Thay đổi so với kế hoạch:** trường `event_time` (chuỗi ISO) đổi thành `event_time_ms` (số nguyên). Hai engine so sánh số nguyên thì không lệch nhau do cách parse múi giờ.

### 5.2 Bản ghi ra (topic `alerts`)

Có hai loại bản ghi, phân biệt bằng trường `kind`:

```json
{"v": 1, "kind": "alert", "engine": "flink", "detector": "zscore",
 "station_id": "hn_01", "param": "pm25", "event_time_ms": 1767600000000, "value": 312.0,
 "score": 4.71, "seq": 1000000012345, "ingest_ts_ms": 1767600000123, "emit_ts_ms": 1767600000188}

{"v": 1, "kind": "probe", "engine": "spark", "detector": null,
 "station_id": "syn_0042", "param": "pm25", "event_time_ms": 1767600000000, "value": 51.2,
 "score": null, "seq": 1000000012300, "ingest_ts_ms": 1767600000123, "emit_ts_ms": 1767600000950}
```

| Trường | Ý nghĩa |
| --- | --- |
| `kind` | `alert`: có bất thường. `probe`: bản ghi chỉ để đo độ trễ |
| `engine` | `flink`, `spark` hoặc `offline` (dùng cho SOTA và bản tham chiếu) |
| `detector` | `zscore`, `ewma`, `mp`, `hst`. Probe để `null` |
| `score` | z-score: giá trị z. EWMA: `(x - m) / sqrt(v)`. SOTA: điểm số của mô hình |
| `emit_ts_ms` | Giờ máy lúc engine tạo bản ghi |
| các trường còn lại | Chép nguyên từ sự kiện vào |
| `recv_ts_ms` | **Không do engine phát.** `measure_perf.py` gắn giờ máy lúc consumer nhận bản ghi, rồi ghi vào `latency.csv` (probe) và `alerts.jsonl` (alert). Dùng cho độ trễ end-to-end và detection delay (mục 10.2) |

Quy tắc phát bản ghi:

- Một sự kiện bị **cả hai** detector cảnh báo thì phát **2** bản ghi `alert`.
- Một sự kiện có `seq % 100 == 0` thì phát thêm **1** bản ghi `probe`, kể cả khi nó cũng có alert.
- Sự kiện trễ bị bỏ (mục 6.3) thì không phát gì, kể cả probe. Engine ghi số sự kiện bị bỏ vào log.

---

## 6. Định nghĩa thuật toán (phải giống hệt nhau)

`common/detectors_ref.py` là bản cài đặt tham chiếu bằng Python thuần, chạy tuần tự. Output của Flink và Spark phải **khớp** với output của bản này (mục 9). Có lệch thì là bug.

Mọi phép tính dùng `float64`. Với mỗi key (`station_id`, `param`), các sự kiện được xử lý **theo đúng thứ tự đến** (thứ tự `seq` trong partition).

### 6.1 Rolling z-score

```
state: buf = 24 giá trị gần nhất (deque), n = số mẫu đã nhận

khi nhận x:
  nếu n >= 24:
      mean = trung_bình(buf);  std = sqrt(trung_bình((buf - mean)^2))   # ddof = 0
      nếu std > 1e-9 và |x - mean| / std > 3.0  ->  alert(detector="zscore", score=(x-mean)/std)
  buf.append(x)   # bỏ phần tử cũ nhất nếu đã đủ 24
  n += 1
```

### 6.2 EWMA

```
state: m (trung bình mũ), v (phương sai mũ), n

khi nhận x:
  nếu n == 0:  m = x; v = 0
  ngược lại:
      nếu n >= 24 và v > 1e-9 và |x - m| > 3.0 * sqrt(v)  ->  alert(detector="ewma", score=(x-m)/sqrt(v))
      d = x - m
      m = m + 0.3 * d
      v = (1 - 0.3) * (v + 0.3 * d^2)
  n += 1
```

Lưu ý: **kiểm tra trước, cập nhật sau**. State vẫn được cập nhật bằng cả những giá trị bị đánh dấu bất thường.

### 6.3 Xử lý dữ liệu đến trễ

Nhóm tự cài quy tắc này trong state của mình, **không dựa vào watermark của engine**. Lý do: Flink và Spark xử lý watermark khác nhau, nên dựa vào engine thì hai bên sẽ bỏ những bản ghi khác nhau.

```
state: max_et = event_time_ms lớn nhất đã thấy của key

nếu event_time_ms < max_et - 7 200 000  ->  BỎ sự kiện (không cập nhật state, không phát gì), tăng bộ đếm late_dropped
ngược lại: max_et = max(max_et, event_time_ms), rồi chạy 6.1 và 6.2
```

Hệ quả cho các engine:

- **Spark:** **không** gọi `withWatermark` trước bước stateful. Dùng `GroupStateTimeout.NoTimeout`.
- **Flink:** không dùng timer event-time, nên **không gán timestamp/watermark** (`flink.assign_timestamps: false`). Đo thử: bỏ bước này + chạy Python ở thread mode (`flink.python_execution_mode: thread`) giúp PyFlink nhanh khoảng 2,5 lần mà output không đổi. Muốn bật lại thì đặt `assign_timestamps: true`.

### 6.4 Gợi ý cài đặt trên từng engine

| | Flink (A) | Spark (B) |
| --- | --- | --- |
| API | PyFlink DataStream: `KafkaSource` → `key_by(station_id, param)` → `KeyedProcessFunction` → `KafkaSink` | `readStream.format("kafka")` → parse JSON → `groupBy(station_id, param).applyInPandasWithState(...)` → `writeStream.format("kafka")` |
| State | `ValueState`: buf (list), m, v, n, max_et | Một hàng state gồm buf (chuỗi JSON), m, v, n, max_et |
| Thứ tự trong batch | Tự đúng thứ tự (xử lý từng bản ghi) | **Bắt buộc `sort_values("seq")`** trong mỗi nhóm trước khi xử lý |
| Sink | Mặc định `AT_LEAST_ONCE`. Riêng RQ3 chạy thêm `EXACTLY_ONCE`, transactional id prefix `p3-flink` | Kafka sink (at-least-once), `outputMode("append")` |
| Checkpoint | `/checkpoints/flink/<run_id>` | `/checkpoints/spark/<run_id>` |

> ⚠️ **Bẫy về độ trễ:** ở chế độ `EXACTLY_ONCE`, Flink chỉ commit giao dịch Kafka khi checkpoint xong. Consumer `read_committed` vì vậy thấy bản ghi trễ tới khoảng 10 giây. **RQ1 và RQ2 đo ở `AT_LEAST_ONCE`** để công bằng với Spark. `EXACTLY_ONCE` chỉ dùng ở RQ3, để đếm bản ghi trùng hoặc mất.

---

## 7. Dữ liệu và nhãn

### 7.1 OpenAQ Hà Nội (B)

- Nguồn: kho lưu trữ OpenAQ trên AWS S3 (`openaq-data-archive`). Dùng S3 thay vì API để tránh bị giới hạn tần suất.
- Phạm vi: 1–3 trạm PM2.5 ở Hà Nội, 12 tháng gần nhất có đủ dữ liệu, tần suất theo giờ.
- Làm sạch: bỏ giá trị null và giá trị âm, bỏ timestamp trùng (giữ bản đầu), sắp xếp theo `event_time_ms`. Ghi lại số dòng đã bỏ.
- Đổi tên trạm thành `hn_01`, `hn_02`…, lưu bảng tra mã gốc ở `data/processed/stations.csv`.

### 7.2 Chèn bất thường: `inject_anomalies.py` (B, seed 42)

Gọi σ là độ lệch chuẩn của chuỗi gốc theo từng trạm. Mỗi trạm được chèn khoảng **30 bất thường**, các bất thường cách nhau ít nhất 48 mẫu:

| Loại | Cách chèn | Độ dài |
| --- | --- | --- |
| `spike` | x + k·σ, k lấy ngẫu nhiên trong [4, 6] | 1 mẫu |
| `level_shift` | x + 3σ trong cả đoạn | 6–24 mẫu |
| `drift` | cộng dốc tuyến tính từ 0 lên 3σ | 24–48 mẫu |

File `labels.csv`:

```
anomaly_id,station_id,param,type,start_ms,end_ms
1,hn_01,pm25,spike,1767600000000,1767600000000
```

Dữ liệu gốc có thể đã có sẵn bất thường thật nhưng không được gắn nhãn. Điều này làm Precision thấp hơn thực tế, nên phải ghi vào mục Giới hạn của báo cáo.

### 7.3 Dữ liệu tổng hợp cho RQ1 và RQ3 (A)

- Có 1000 trạm `syn_0000`…`syn_0999`, mỗi trạm là một random walk: `x_t = x_{t-1} + N(0, 1)`, giá trị đầu 50.
- `event_time_ms` lấy theo giờ máy lúc gửi. Dữ liệu này không có nhãn và chỉ dùng để tạo tải.

---

## 8. Producer: `producer/producer.py`

```
python producer/producer.py --mode rate   --rate 20000 --duration 210 --producer-id 0
python producer/producer.py --mode replay --file data/processed/openaq_hn_injected.parquet \
                            --speed 200 --late-fraction 0.05 --late-shift 1 4 --producer-id 0
```

| Tham số | Ý nghĩa |
| --- | --- |
| `--mode rate` | Gửi dữ liệu tổng hợp với tốc độ cố định `--rate` sự kiện/giây trong `--duration` giây. Không đạt nổi tốc độ yêu cầu thì in cảnh báo |
| `--mode replay` | Gửi dữ liệu OpenAQ theo thứ tự thời gian, `--speed` sự kiện/giây |
| `--late-fraction`, `--late-shift a b` | Chọn ngẫu nhiên 5% sự kiện, giữ lại rồi gửi sau a–b sự kiện kế tiếp **cùng trạm**. Với ngưỡng 2 giờ, sự kiện bị lùi ≥ 3 vị trí sẽ bị bỏ |
| `--producer-id`, `--n-producers` | Dùng để tạo `seq`. Chạy nhiều producer song song thì mỗi producer một id và **chỉ gửi các trạm có `chỉ_số_trạm % n_producers == producer_id`**. Nhờ vậy mỗi trạm chỉ do một producer gửi, và `seq` của một trạm luôn tăng theo thứ tự đến |

Cấu hình `confluent-kafka` dùng chung: `linger.ms=5`, `batch.size=65536`, `acks=1`, `compression.type=none`.

---

## 9. Kiểm tra tương đương (cổng ngày 2)

```
python measure/compare_outputs.py --ref offline --a flink --b spark --input data/processed/equiv_10k.parquet
```

- Dùng file 10 000 sự kiện lấy từ OpenAQ đã chèn bất thường, **không có dữ liệu trễ**.
- So tập (`seq`, `detector`) của các alert giữa: tham chiếu, Flink và Spark.
- **Đạt** khi ba tập trùng nhau hoàn toàn, và `score` lệch nhau dưới `1e-6`.

---

## 10. Đo đạc và lưu kết quả

### 10.1 Quy trình một lần chạy (`run_experiment.sh`)

1. Dừng engine kia. Xóa và tạo lại topic. Xóa checkpoint.
2. Khởi động job, đợi job ở trạng thái RUNNING.
3. Bật `measure_perf.py` (consumer `alerts`) và `docker stats` (mỗi giây ghi một dòng).
4. Bật producer.
5. Bỏ 30 giây đầu (warm-up), đo 180 giây.
6. Dừng producer và job, rồi lưu kết quả.

### 10.2 Định nghĩa metric

| Metric | Định nghĩa |
| --- | --- |
| Độ trễ end-to-end | `recv_ts_ms − ingest_ts_ms` của các bản ghi `probe`. `recv_ts_ms` là giờ máy lúc consumer đo nhận được. Báo p50, p95, p99 |
| Thông lượng | Số `probe` nhận được mỗi giây × 100, tính theo `recv_ts_ms`. Cách này đo giống nhau cho cả hai engine, không phụ thuộc consumer lag |
| Thông lượng bền vững | Mức tải cao nhất thỏa cả hai điều kiện: p99 < 1000 ms, **và** trung vị độ trễ của 30 giây đo cuối ≤ 1,5 lần trung vị của 30 giây đo đầu. Độ trễ tăng dần nghĩa là engine đang bị dồn việc |
| Thời gian phục hồi (RQ3) | Kill TaskManager/worker ở giây 60. Tính từ lúc kill đến khi p99 của cửa sổ 5 giây về lại dưới 2 lần p99 trước khi kill |
| Trùng/mất (RQ3) | Theo `seq` của probe: số `seq` xuất hiện nhiều hơn 1 lần, và số `seq` chia hết cho 100 mà không xuất hiện |
| Precision | Số alert rơi vào một cửa sổ [`start_ms`, `end_ms` + 1 giờ] của nhãn, chia cho tổng số alert |
| Recall | Số bất thường có ít nhất một alert trong cửa sổ, chia cho tổng số bất thường |
| F1 | 2PR / (P + R). Tính riêng cho từng (engine, detector) |
| Detection delay | `recv_ts_ms` của alert đầu tiên trong cửa sổ, trừ `ingest_ts_ms` của mẫu bất thường đầu tiên. Tính bằng giờ máy, chỉ trên các bất thường đã phát hiện được |

Với dữ liệu trễ, quy tắc ở 6.3 giống hệt nhau ở hai engine, nên **F1 của Flink và Spark phải bằng nhau**. Khác biệt chính phải nằm ở detection delay, do micro-batch hay từng bản ghi. F1 khác nhau thì kiểm tra lại code trước khi viết vào báo cáo.

### 10.3 SOTA (chạy offline, dùng cùng `labels.csv`)

| | Matrix Profile (A) | Half-Space Trees (B) |
| --- | --- | --- |
| Thư viện | `stumpy.stump`, m = 24 | `river.anomaly.HalfSpaceTrees(n_trees=25, height=15, window_size=250, seed=42)`, đặt sau `MinMaxScaler` |
| Điểm số | Khoảng cách matrix profile. Gán cho mẫu **cuối** của đoạn con | `score_one` trước, rồi mới `learn_one` |
| Ngưỡng | Phân vị 99% của điểm số trên 20% dữ liệu đầu, giống nhau cho cả hai, không dùng nhãn | như bên trái |
| Output | `alerts.jsonl` cùng schema mục 5.2, `engine="offline"`. Chạy offline nên `seq`, `ingest_ts_ms`, `emit_ts_ms` = `null`, không có `recv_ts_ms` | như bên trái |
| Hậu xử lý | Sau mỗi alert bỏ qua m = 24 mẫu (`sota.mp.refractory`), vì một discord làm m đoạn con liên tiếp cùng có điểm cao | không áp dụng |

### 10.4 Cách đặt tên và lưu file

```
run_id = <rq>_<engine>_<cấu hình>_r<lần>       ví dụ: rq1_flink_rate20000_r2, rq3_spark_kill_r1

results/raw/<run_id>/
    meta.json          # commit git, phiên bản, toàn bộ config.yaml, giờ bắt đầu/kết thúc
    latency.csv        # seq, ingest_ts_ms, emit_ts_ms, recv_ts_ms
    alerts.jsonl       # mọi bản ghi kind=alert, THÊM recv_ts_ms (measure_perf.py gắn lúc nhận)
    sent.csv           # ts_ms, số sự kiện producer đã gửi (mỗi giây) — kiểm tra producer đạt tốc độ
    docker_stats.csv   # ts_ms, container, cpu_pct, mem_mb
    engine.log         # có số late_dropped
results/summary/rq1.csv, rq2.csv, rq3.csv   # mỗi dòng một run_id
figures/                                    # PNG + script vẽ trong measure/
```

Không sửa tay file trong `results/raw/`. Thấy lần chạy nào hỏng thì chạy lại với số `r` mới và ghi lý do vào `meta.json`.

---

## 11. Làm việc với AI và Git

- **Nhánh:** A làm trên `a/...`, B làm trên `b/...`. Đưa vào `main` qua pull request, **người kia duyệt**.
- **Commit:** `[A] flink: thêm EWMA state`, `[B] data: chèn bất thường drift`.
- **`docs/ai-log.md`:** mỗi lần dùng AI cho một việc đáng kể thì thêm một mục theo mẫu:

```
## 2026-10-24 – [A] – Flink KeyedProcessFunction
- Công cụ: Claude / ChatGPT / Gemini
- Yêu cầu chính (rút gọn): ...
- Đã kiểm tra bằng: chạy compare_outputs.py trên equiv_10k
- Đã sửa gì so với AI: ...
```

- Trích dẫn do AI đưa ra phải **mở bài gốc** kiểm tra rồi mới đưa vào báo cáo.

---

## 12. Lịch sync

Hai người sync 15 phút vào **8:00, 13:00, 21:00** mỗi ngày. Nội dung: cổng hôm nay đã qua chưa, đang vướng gì, có cần đổi quy ước nào không.

## 13. Chạy nhanh

```bash
docker compose build
docker compose up -d kafka
bash scripts/create_topics.sh
bash scripts/run_experiment.sh rq1 flink 20000 1     # <rq> <engine> <cấu hình> <lần>
```
