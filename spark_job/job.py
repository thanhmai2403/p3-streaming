"""Job Spark Structured Streaming: Kafka `sensor` -> z-score + EWMA -> Kafka `alerts`.

README mục 6: đọc Kafka -> parse JSON -> groupBy("station_id", "param")
.applyInPandasWithState(...) -> xuất topic `alerts` với outputMode("append").
KHÔNG gọi withWatermark; dùng GroupStateTimeout.NoTimeout; trigger mặc định;
state store RocksDB (tự fallback về provider mặc định nếu thiếu native lib,
ví dụ chạy test local trên Windows).

Cách chạy:

    # 1) Streaming (trong container Spark, hoặc local với Kafka đang chạy):
    python spark_job/job.py --run-id rq2_spark_replay_r1 \
        --master spark://spark-master:7077 --bootstrap-servers kafka:9092
    # (mặc định bootstrap-servers lấy từ config.yaml kafka.bootstrap_internal)

    # 2) Kiểm thử offline (không cần Spark/Kafka): chạy ĐÚNG hàm xử lý state
    #    mà applyInPandasWithState dùng, trên parquet, rồi so với bản tham chiếu:
    python spark_job/job.py --test-parquet data/processed/equiv_10k.parquet \
        --output data/processed/spark_alerts.jsonl
    python measure/compare_outputs.py \
        --ref-file data/processed/ref_alerts.jsonl \
        --a-file data/processed/ref_alerts.jsonl \
        --b-file data/processed/spark_alerts.jsonl \
        --ref offline --a offline --b spark

    # 3) Kiểm thử tương đương bằng applyInPandasWithState THẬT (Spark
    #    Structured Streaming đọc parquet, trigger once, sink JSON file):
    python spark_job/job.py --test-parquet-spark data/processed/equiv_10k.parquet \
        --output data/processed/spark_alerts.jsonl
    python measure/compare_outputs.py \
        --ref-file data/processed/ref_alerts.jsonl \
        --a-file data/processed/ref_alerts.jsonl \
        --b-file data/processed/spark_alerts.jsonl \
        --ref offline --a offline --b spark

    # 4) Kiểm thử NHIỀU micro-batch + DỮ LIỆU TRẾ (README 6.3, handover_B mục 6 việc 3):
    #    --test-parquet-spark nhận 1 THƯ MỤC nhiều file part-*.parquet đã có cột seq
    #    (mỗi file = 1 khối seq liên tiếp) + --max-files-per-trigger => mỗi micro-batch
    #    chỉ đọc N file, nên state phải được lưu/đọc lại giữa các batch.
    #    Không cần gõ tay: xem tests/test_spark_multibatch_late.py
    #      (sinh 20 file từ producer.replay_order 5 % trễ, chạy Spark,
    #       so với common/detectors_ref.py và so số late_dropped).
"""
import argparse
import json
import os
import sys
import threading
import time
from collections import deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.config import load_config
from common.detectors_ref import (EPS, PROBE_EVERY, WINDOW, KeyState,
                                  _alert, process_event)

ENGINE = "spark"
ROCKSDB_PROVIDER = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass


# ---------------------------------------------------------------- state helpers

def _int_or_none(v):
    """null (long) trong pandas UDF tới dạng NaN float; trả None thay vì crash."""
    if v is None:
        return None
    if isinstance(v, float) and v != v:  # NaN
        return None
    return int(v)


def _new_state():
    return KeyState()


def _state_to_row(st):
    """KeyState -> bộ giá trị 1 hàng state của Spark (buf JSON, n, m, v, max_et)."""
    return (json.dumps(list(st.buf)), int(st.n), float(st.m), float(st.v),
            None if st.max_et is None else int(st.max_et))


def _row_to_state(row):
    """Giá trị state của Spark -> KeyState.

    pyspark 4.1.3: `state.get` là property trả về TUPLE đúng thứ tự STATE_STRUCT
    (buf, n, m, v, max_et) - KHÔNG phải Row nên đọc theo chỉ số, không theo tên.
    """
    buf, n, m, v, max_et = row
    st = KeyState()
    st.buf = deque(json.loads(str(buf)), maxlen=WINDOW)
    st.n = int(n)
    st.m = float(m)
    st.v = float(v)
    st.max_et = None if max_et is None else int(max_et)
    return st


def _copy_state(st):
    """Bản sao state qua ĐÚNG biểu diễn state của Spark (tuple -> tuple),
    mô phỏng lưu/đọc state giữa các micro-batch (dùng cho chế độ --test-parquet)."""
    return _row_to_state(_state_to_row(st))


def process_rows(rows, st, with_probes, emit_ms_func=None):
    """Xử lý các sự kiện ĐÃ SẮP theo seq của một key (README 6.4: bắt buộc sort).

    Dùng chung cho hàm applyInPandasWithState và chế độ --test-parquet.
    Trả về (records, late_dropped).
    """
    records = []
    late_dropped = 0
    for ev in rows:
        alerts, late = process_event(st, ev, ENGINE)
        if late:
            # 6.3: bỏ hẳn - không cập nhật state, không phát gì (kể cả probe)
            late_dropped += 1
            continue
        if emit_ms_func is not None:
            now = emit_ms_func()
            for r in alerts:
                r["emit_ts_ms"] = now
        records.extend(alerts)
        if with_probes and int(ev["seq"]) % PROBE_EVERY == 0:
            p = _alert(ev, None, None, ENGINE)
            p["kind"] = "probe"
            if emit_ms_func is not None:
                p["emit_ts_ms"] = emit_ms_func()
            records.append(p)
    return records, late_dropped


# ------------------------------------------------- applyInPandasWithState

STATE_STRUCT = None  # StructType dựng khi có pyspark (tránh import pyspark cho chế độ test)


def _build_state_struct():
    global STATE_STRUCT
    if STATE_STRUCT is None:
        from pyspark.sql.types import (DoubleType, LongType, StringType,
                                       StructField, StructType)
        STATE_STRUCT = StructType([
            StructField("buf", StringType(), False),
            StructField("n", LongType(), False),
            StructField("m", DoubleType(), False),
            StructField("v", DoubleType(), False),
            StructField("max_et", LongType(), True),
        ])
    return STATE_STRUCT


def make_state_func(late_acc=None):
    """Trả về hàm applyInPandasWithState. late_acc: Accumulator đếm sự kiện trễ."""

    def state_func(key, pdf_iter, state):
        station_id, param = str(key[0]), str(key[1])
        st = _row_to_state(state.get) if state.exists else _new_state()
        out_rows = []
        dropped = 0
        for pdf in pdf_iter:
            if pdf is None or len(pdf) == 0:
                continue
            # BẮT BUỘC: sort theo seq trong mỗi micro-batch (README 6.4)
            pdf = pdf.sort_values("seq", kind="stable")
            rows = [
                {
                    "station_id": station_id,
                    "param": param,
                    "value": float(r.value),
                    "event_time_ms": int(r.event_time_ms),
                    "ingest_ts_ms": _int_or_none(r.ingest_ts_ms),
                    "seq": int(r.seq),
                }
                for r in pdf.itertuples(index=False)
            ]
            records, late = process_rows(
                rows, st, with_probes=True,
                emit_ms_func=lambda: time.time_ns() // 1_000_000)
            out_rows.extend(records)
            dropped += late
        if dropped:
            # ghi số sự kiện bị bỏ vì trễ vào log executor (README 5.2)
            sys.stderr.write(f"[spark] late_dropped += {dropped} (key={station_id},{param})\n")
            sys.stderr.flush()
        if late_acc is not None and dropped:
            late_acc.add(dropped)
        state.update(_state_to_row(st))
        import pandas as pd
        if not out_rows:
            return iter(())
        return [pd.DataFrame(out_rows, columns=[f.name for f in _OUTPUT_COLS()])]

    return state_func


def _OUTPUT_COLS():
    from common.schema import spark_record_struct
    return spark_record_struct()


# ------------------------------------------------------- Spark session + query

def _rocksdb_available(spark):
    """Kiểm tra native lib RocksDB có nạp được không (fallback nếu thiếu)."""
    try:
        spark._jvm.org.rocksdb.RocksDB.loadLibrary()
        return True
    except Exception:
        return False


def build_spark(cfg, run_id, master=None, packages=None, force_no_rocksdb=False):
    from pyspark.sql import SparkSession

    # Pandas UDF (applyInPandasWithState) chạy Python worker riêng: phải trỏ PYSPARK_PYTHON
    # về Python CÓ pandas/pyarrow. Trong Docker image apache/spark đã đặt python3 sẵn;
    # ngoài Docker (venv local) thì sys.executable là Python đang chạy job.
    # setdefault: không ghi đè nếu người dùng tự đặt biến này.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(f"p3-spark-{run_id}")
    if master:
        builder = builder.master(master)
    builder = builder.config("spark.sql.shuffle.partitions",
                             int(cfg["spark"]["shuffle_partitions"]))
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    spark = builder.getOrCreate()

    if force_no_rocksdb:
        print("[spark] --no-rocksdb: dùng state store provider mặc định.", flush=True)
    elif _rocksdb_available(spark):
        spark.conf.set("spark.sql.streaming.stateStore.providerClass", ROCKSDB_PROVIDER)
        print(f"[spark] state store: RocksDB ({ROCKSDB_PROVIDER})", flush=True)
    else:
        # Fallback khi chạy test local (ví dụ Windows thiếu native lib RocksDB).
        print("[spark] CẢNH BÁO: không nạp được native lib RocksDB -> "
              "dùng state store provider mặc định.", flush=True)
    return spark


def build_query(spark, cfg, run_id, bootstrap_servers, checkpoint_dir):
    from pyspark.sql import functions as F
    from pyspark.sql.streaming.state import GroupStateTimeout

    from common.schema import spark_event_struct, spark_record_struct

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap_servers)
           .option("subscribe", cfg["kafka"]["topic_in"])
           .option("startingOffsets", "earliest")  # README mục 3
           .load())

    events = (raw
              .selectExpr("CAST(value AS STRING) AS json")
              .select(F.from_json("json", spark_event_struct()).alias("ev"))
              .select("ev.*")
              .filter(F.col("station_id").isNotNull()
                      & F.col("value").isNotNull()
                      & F.col("seq").isNotNull()))

    grouped = events.groupBy("station_id", "param")
    late_acc = spark.sparkContext.accumulator(0)

    results = grouped.applyInPandasWithState(
        make_state_func(late_acc),
        spark_record_struct(),
        _build_state_struct(),
        "Append",                          # outputMode("append") (README 6.4)
        GroupStateTimeout.NoTimeout,       # KHÔNG dùng watermark (README 6.3) - pyspark 4.1.3: giá trị chuỗi, không gọi ()
    )

    out = (results
           .selectExpr(
               "CAST(station_id AS STRING) AS key",
               "to_json(struct(*), map('ignoreNullFields', 'false')) AS value")
           .writeStream
           .format("kafka")
           .option("kafka.bootstrap.servers", bootstrap_servers)
           .option("topic", cfg["kafka"]["topic_out"])
           .outputMode("append")
           .option("checkpointLocation", checkpoint_dir)
           # trigger mặc định (README 6.4: không đặt trigger)
           )

    return out, late_acc


# ------------------------------------------------------------ chế độ test offline

def run_test_parquet(cfg, input_path, output_path, with_probes, batch_size):
    """Chạy ĐÚNG hàm xử lý state của applyInPandasWithState trên parquet (không cần Spark).

    Mô phỏng micro-batch: chia mỗi key thành các khối `batch_size` dòng, state được
    (de)serialize giữa các khối như giữa các micro-batch thật của Spark.
    """
    import pandas as pd

    df = pd.read_parquet(input_path)
    if "seq" not in df.columns:
        df = df.reset_index(drop=True)
        df["seq"] = df.index  # y hệt common/detectors_ref.py main()
    if "ingest_ts_ms" not in df.columns:
        df["ingest_ts_ms"] = None
    df = df.sort_values("seq", kind="stable")

    states = {}
    records = []
    late_total = 0
    for (station_id, param), g in df.groupby(["station_id", "param"], sort=False):
        g = g.sort_values("seq", kind="stable")
        st = states.setdefault((station_id, param), _new_state())
        for start in range(0, len(g), batch_size):
            chunk = g.iloc[start:start + batch_size]
            rows = chunk.to_dict("records")
            for r in rows:
                r["station_id"] = str(station_id)
                r["param"] = str(param)
                r["value"] = float(r["value"])
                r["event_time_ms"] = int(r["event_time_ms"])
                r["seq"] = int(r["seq"])
                r["ingest_ts_ms"] = (None if r["ingest_ts_ms"] is None
                                     else int(r["ingest_ts_ms"]))
            st = _copy_state(st)  # mô phỏng lưu/đọc state giữa các micro-batch
            recs, late = process_rows(rows, st, with_probes=with_probes,
                                      emit_ms_func=None)
            records.extend(recs)
            late_total += late
        states[(station_id, param)] = st

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    n_alert = sum(1 for r in records if r["kind"] == "alert")
    n_probe = sum(1 for r in records if r["kind"] == "probe")
    print(f"[test-parquet] {len(df)} sự kiện ({len(states)} key) -> "
          f"{n_alert} alert + {n_probe} probe, late_dropped={late_total}")
    print(f"[test-parquet] ghi {out_path}")
    return records, late_total


def run_test_parquet_spark(cfg, input_path, output_path, master=None, packages=None,
                           force_no_rocksdb=False, max_files_per_trigger=None):
    """Chạy applyInPandasWithState THẬT (Spark Structured Streaming) trên parquet.

    File source (trigger once = xử lý hết dữ liệu có sẵn rồi dừng) ->
    groupBy("station_id", "param") -> applyInPandasWithState (y hệt build_query)
    -> sink JSON file (append) -> gộp part-*.json thành file .jsonl (thứ tự seq).

    `input_path` nhận 1 trong 2 dạng:
      - 1 file parquet (như equiv_10k.parquet): file không có cột seq/ingest_ts_ms,
        seq được sinh bằng thứ tự dòng vật lý trong file (xem chú thích bên dưới);
      - 1 THƯ MỤC nhiều file part-*.parquet đã có sẵn cột seq, mỗi file là một khối
        sự kiện LIÊN TIẾP theo seq (thứ tự file = thứ tự seq). Dùng chung với
        `max_files_per_trigger` để kiểm tra nhiều micro-batch + dữ liệu trễ
        (tests/test_spark_multibatch_late.py).
    """
    import shutil

    from pyspark.sql import functions as F
    from pyspark.sql.streaming.state import GroupStateTimeout
    from pyspark.sql.types import (DoubleType, LongType, StringType,
                                   StructField, StructType)

    from common.schema import spark_record_struct

    src = Path(input_path).resolve()
    dir_input = src.is_dir()
    if dir_input:
        src_files = sorted(src.glob("*.parquet"))
        if not src_files:
            raise SystemExit(f"Thư mục không có file parquet nào: {src}")
    elif not src.is_file():
        raise SystemExit(f"Không tìm thấy parquet đầu vào: {src}")
    else:
        src_files = [src]
    # checkpoint dùng riêng thư mục local (cfg["spark"]["checkpoint_dir"] là đường
    # dẫn trong container, không dùng được khi chạy ngoài Docker trên Windows).
    work = Path(__file__).resolve().parent.parent / "checkpoints" / "_equiv_check"
    if work.exists():
        shutil.rmtree(work, ignore_errors=True)
    in_dir = work / "in"
    out_dir = work / "out"
    ckpt_dir = work / "ckpt"
    spark = build_spark(cfg, "equiv_check", master=master, packages=packages,
                        force_no_rocksdb=force_no_rocksdb)
    # Ghi lại state store THỰC TẾ đang dùng (RocksDB hay provider mặc định) vào bằng
    # chứng: test nhiều micro-batch chỉ có nghĩa nếu state được lưu/đọc lại qua state store.
    provider = spark.conf.get("spark.sql.streaming.stateStore.providerClass", "default")
    state_store = "rocksdb" if "RocksDB" in provider else "default"
    print(f"[test-parquet-spark] state store: {state_store} ({provider})", flush=True)
    try:
        if dir_input:
            # Đầu vào đã chuẩn bị sẵn: MỖI file là một khối sự kiện liên tiếp theo seq.
            # Chép NGUYÊN tên file (part-00000, part-00001, ...) vào thư mục nguồn của
            # query để thứ tự đọc trùng thứ tự seq (Spark đọc file theo thứ tự tên).
            cols = set(spark.read.parquet(str(src)).columns)
            thieu = {"seq", "ingest_ts_ms", "event_time_ms"} - cols
            if thieu:
                raise SystemExit(f"thư mục {src} thiếu cột {sorted(thieu)}")
            in_dir.mkdir(parents=True, exist_ok=True)
            for f in src_files:
                shutil.copy2(f, in_dir / f.name)
        else:
            # equiv_10k.parquet không có cột seq/ingest_ts_ms; bản tham chiếu tạo
            # seq = thứ tự dòng vật lý trong file, ingest_ts_ms = null. File 121KB,
            # 1 row group -> Spark đọc 1 partition -> thứ tự dòng giữ nguyên, vậy
            # monotonically_increasing_id() == chỉ số dòng của bản tham chiếu.
            df = (spark.read.parquet(str(src))
                  .withColumn("seq", F.monotonically_increasing_id())
                  .withColumn("ingest_ts_ms", F.lit(None).cast(LongType())))
            df.coalesce(1).write.mode("overwrite").parquet(str(in_dir))

        in_schema = StructType([
            StructField("station_id", StringType(), True),
            StructField("param", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("event_time_ms", LongType(), True),
            StructField("ingest_ts_ms", LongType(), True),
            StructField("seq", LongType(), True),
        ])
        reader = spark.readStream.schema(in_schema)
        if max_files_per_trigger:
            # Nhiều micro-batch: mỗi trigger chỉ đọc tối đa N file -> state phải được
            # lưu xuống / đọc lại từ state store giữa các micro-batch (README 6.3).
            reader = reader.option("maxFilesPerTrigger", int(max_files_per_trigger))
        events = (reader.parquet(str(in_dir))
                  .filter(F.col("station_id").isNotNull()
                          & F.col("value").isNotNull()
                          & F.col("seq").isNotNull()))
        late_acc = spark.sparkContext.accumulator(0)
        results = (events.groupBy("station_id", "param")
                   .applyInPandasWithState(make_state_func(late_acc),
                                           spark_record_struct(),
                                           _build_state_struct(),
                                           "Append", GroupStateTimeout.NoTimeout))
        q = (results.coalesce(1)
             .writeStream.format("json")
             .outputMode("append")
             .option("path", str(out_dir))
             .option("checkpointLocation", str(ckpt_dir)))
        if max_files_per_trigger:
            # trigger once chỉ chạy 1 micro-batch (mà mỗi batch bị giới hạn N file) ->
            # dùng trigger theo thời gian + processAllAvailable() để chạy HẾT các file.
            q = q.trigger(processingTime="1 second").start()
            q.processAllAvailable()
            n_batch = len(q.recentProgress)
            q.stop()
        else:
            q = q.trigger(once=True).start()
            q.awaitTermination()
            n_batch = len(q.recentProgress)
        exc = q.exception()
        if exc is not None:
            raise RuntimeError(f"query lỗi: {exc}")
        records = []
        for part in sorted(out_dir.glob("part-*.json")):
            for line in part.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    records.append(json.loads(line))
        records.sort(key=lambda r: (int(r["seq"]), r["kind"],
                                    r.get("detector") or ""))
        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        n_alert = sum(1 for r in records if r["kind"] == "alert")
        n_probe = sum(1 for r in records if r["kind"] == "probe")
        info = {"n_records": len(records), "n_alert": n_alert, "n_probe": n_probe,
                "late_dropped": int(late_acc.value), "n_batch": int(n_batch),
                "n_input_files": len(src_files), "state_store": state_store,
                "state_store_provider": provider}
        print(f"[test-parquet-spark] {len(src_files)} file đầu vào -> {n_batch} micro-batch: "
              f"{len(records)} bản ghi ({n_alert} alert + {n_probe} probe), "
              f"late_dropped={info['late_dropped']}")
        print(f"[test-parquet-spark] ghi {out_path}")
        return info
    finally:
        spark.stop()
        shutil.rmtree(work, ignore_errors=True)


# ------------------------------------------------------------------------ main

def main():
    ap = argparse.ArgumentParser(description="Spark Structured Streaming job (README mục 6)")
    ap.add_argument("--run-id", default=None,
                    help="<rq>_<engine>_<config>_r<rep>; dùng cho checkpoint")
    ap.add_argument("--bootstrap-servers", default=None,
                    help="mặc định: P3_KAFKA_BROKERS hoặc config.yaml kafka.bootstrap_internal")
    ap.add_argument("--master", default=None,
                    help="ví dụ spark://spark-master:7077; mặc định local[*]")
    ap.add_argument("--packages", default=None,
                    help="gói Maven (vd org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3) "
                         "khi chạy local ngoài Docker")
    ap.add_argument("--checkpoint", default=None,
                    help="mặc định: <spark.checkpoint_dir>/<run_id> = /checkpoints/spark/<run_id>")
    ap.add_argument("--no-rocksdb", action="store_true",
                    help="ép dùng state store provider mặc định")
    ap.add_argument("--config", default=None, help="đường dẫn config.yaml khác")
    ap.add_argument("--test-parquet", default=None,
                    help="kiểm thử offline: chạy hàm state trên parquet rồi thoát")
    ap.add_argument("--test-parquet-spark", default=None,
                    help="kiểm thử tương đương: chạy applyInPandasWithState THẬT "
                         "(Spark Structured Streaming, trigger once) trên parquet "
                         "hoặc trên 1 THƯ MỤC nhiều file parquet đã có cột seq")
    ap.add_argument("--max-files-per-trigger", type=int, default=None,
                    help="(--test-parquet-spark) số file tối đa đọc mỗi micro-batch; "
                         "đặt 1 để kiểm tra nhiều micro-batch + dữ liệu trễ (README 6.3)")
    ap.add_argument("--output", default=None, help="file .jsonl cho --test-parquet")
    ap.add_argument("--with-probes", action="store_true",
                    help="(--test-parquet) phát thêm bản ghi probe cho seq % 100 == 0")
    ap.add_argument("--batch-size", type=int, default=500,
                    help="(--test-parquet) kích thước micro-batch mô phỏng")
    args = ap.parse_args()

    cfg = load_config(args.config)

    # ---- chế độ kiểm thử tương đương bằng applyInPandasWithState THẬT
    if args.test_parquet_spark:
        if not args.output:
            raise SystemExit("Cần --output khi dùng --test-parquet-spark.")
        run_test_parquet_spark(cfg, args.test_parquet_spark, args.output,
                               master=args.master, packages=args.packages,
                               force_no_rocksdb=args.no_rocksdb,
                               max_files_per_trigger=args.max_files_per_trigger)
        return

    # ---- chế độ kiểm thử offline
    if args.test_parquet:
        if not args.output:
            raise SystemExit("Cần --output khi dùng --test-parquet.")
        run_test_parquet(cfg, args.test_parquet, args.output,
                         args.with_probes, args.batch_size)
        return

    # ---- chế độ streaming
    if not args.run_id:
        raise SystemExit("Cần --run-id (vd: rq2_spark_replay_r1). "
                         "Hoặc dùng --test-parquet để kiểm thử offline.")
    bootstrap = (args.bootstrap_servers
                 or os.environ.get("P3_KAFKA_BROKERS")
                 or cfg["kafka"]["bootstrap_internal"])
    checkpoint_dir = (args.checkpoint
                      or f"{cfg['spark']['checkpoint_dir']}/{args.run_id}")
    master = args.master or os.environ.get("P3_SPARK_MASTER")
    packages = args.packages or os.environ.get("P3_SPARK_PACKAGES")

    print(f"[spark] run_id={args.run_id} bootstrap={bootstrap} "
          f"checkpoint={checkpoint_dir} master={master or 'local[*]'}", flush=True)

    spark = build_spark(cfg, args.run_id, master=master, packages=packages,
                        force_no_rocksdb=args.no_rocksdb)
    out, late_acc = build_query(spark, cfg, args.run_id, bootstrap, checkpoint_dir)

    stop_flag = threading.Event()

    def log_late():
        while not stop_flag.is_set():
            print(f"[spark] late_dropped tổng cộng: {late_acc.value}", flush=True)
            stop_flag.wait(10)

    threading.Thread(target=log_late, daemon=True).start()
    try:
        out.start().awaitTermination()
    finally:
        stop_flag.set()


if __name__ == "__main__":
    main()

