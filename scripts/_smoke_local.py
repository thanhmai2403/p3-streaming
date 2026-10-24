"""SMOKE TEST CỤC BỘ (tạm, xóa sau khi pass): chạy applyInPandasWithState của
spark_job/job.py trên nguồn `rate` (không cần Kafka).
- Pha 1 (checkpoint mới): kiểm tra state round-trip, alert zscore+ewma, probe.
- Pha 2 (checkpoint cũ): kiểm tra khôi phục state + bỏ dữ liệu trễ.
- Dùng RocksDB state store nếu native lib nạp được (y hệt build_spark()).
"""
import shutil
import sys
import time
from pathlib import Path
try:  # Windows: ghi log utf-8 khi chạy nền (cp1252 sẽ fail với tiếng Việt)
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass
import os
import subprocess
def _fix_java():
    """Spark 4.1.3 (Hadoop 3.4.1) KHÔNG chạy được trên Java 25 local
    (UnsupportedOperationException: getSubject) - tự chuyển sang JDK 17/21."""
    try:
        out = subprocess.run(["java", "-version"], capture_output=True,
                             text=True).stderr
        major = int(out.split('"')[1].split(".")[0])
    except Exception:
        return
    if major < 23:
        return
    for cand in (os.path.expanduser("~/jdk-21"),
                 r"C:\Program Files\Android\Android Studio\jbr",
                 r"C:\Program Files\Java\jdk-21",
                 r"C:\Program Files\Eclipse Adoptium\jdk-21.0.8.10-hotspot"):
        if os.path.isdir(os.path.join(cand, "bin", "java.exe")) or \
           os.path.isdir(os.path.join(cand, "bin")):
            os.environ["JAVA_HOME"] = cand
            print(f"[smoke] Java {major} không hợp -> JAVA_HOME={cand}")
            return
_fix_java()
def _fix_winutils():
    """Spark local trên Windows cần winutils.exe khi tạo checkpoint (Hadoop FS).
    Tự tải từ repo cộng đồng cdarlint/winutils nếu chưa có."""
    if os.environ.get("HADOOP_HOME") and os.path.isfile(
            os.path.join(os.environ["HADOOP_HOME"], "bin", "winutils.exe")):
        return
    home = os.path.expanduser("~/hadoop")
    exe = os.path.join(home, "bin", "winutils.exe")
    dll = os.path.join(home, "bin", "hadoop.dll")
    if not os.path.isfile(exe):
        print("[smoke] thiếu winutils.exe -> tải về (cdarlint/winutils)...")
        import urllib.request
        os.makedirs(os.path.dirname(exe), exist_ok=True)
        req = urllib.request.Request(
            "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as r, open(exe, "wb") as f:
            f.write(r.read())
    if not os.path.isfile(dll):
        # FileUtil.canRead -> NativeIO.Windows.access0 cần hadoop.dll
        # (thiếu thì UnsatisfiedLinkError khi Spark liệt kê checkpoint).
        print("[smoke] thiếu hadoop.dll -> tải về (cdarlint/winutils)...")
        import urllib.request
        os.makedirs(os.path.dirname(dll), exist_ok=True)
        req = urllib.request.Request(
            "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=60) as r:
            data = r.read()
        assert data[:2] == b"MZ", "hadoop.dll tải về không phải file PE hợp lệ"
        with open(dll, "wb") as f:
            f.write(data)
    os.environ["HADOOP_HOME"] = home
    # JVM trên Windows dùng PATH làm java.library.path mặc định ->
    # phải đưa hadoop.dll vào PATH thì NativeIO.Windows.access0 mới nạp được.
    bin_dir = os.path.join(home, "bin")
    os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
    print(f"[smoke] HADOOP_HOME={home} (PATH += {bin_dir})")
_fix_winutils()
# Pandas UDF worker phải chạy đúng Python của venv (có pandas/pyarrow).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from common.schema import spark_record_struct
from spark_job.job import (ROCKSDB_PROVIDER, _build_state_struct,
                           make_state_func)
CHECK = Path("checkpoints/_smoke")
shutil.rmtree(CHECK, ignore_errors=True)
OUT = Path("out_smoke")
shutil.rmtree(OUT, ignore_errors=True)
_RESULT = Path("smoke_result.txt")
def _mark(msg):
    with open(_RESULT, "a", encoding="utf-8") as f:
        f.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
def _mark_exc():
    import traceback
    _mark("TRACEBACK:\n" + traceback.format_exc())
def _excepthook(t, v, tb):
    import traceback
    try:
        _mark("UNCAUGHT: " + "".join(traceback.format_exception(t, v, tb)))
        # Chép đuôi smoke_err.log (stderr của JVM) vào result file để chẩn đoán
        for _name in ("smoke_err.log", "smoke.log"):
            _log = Path(_name)
            if _log.exists():
                try:
                    lines = _log.read_text(encoding="utf-8",
                                           errors="replace").splitlines()
                except Exception:
                    continue
                _mark(f"--- ĐUÔI {_name} ({min(60, len(lines))} dòng cuối) ---")
                _mark("\n".join(lines[-60:]))
                _mark(f"--- HẾT {_name} ---")
                break
    except Exception:
        pass
    sys.__excepthook__(t, v, tb)
sys.excepthook = _excepthook
with open(_RESULT, "w", encoding="utf-8") as f:
    f.write(f"[{time.strftime('%H:%M:%S')}] BẮT ĐẦU smoke test\n")
def _create_spark(retries=3, wait_s=8):
    last = None
    for i in range(retries):
        try:
            return (SparkSession.builder.master("local[2]")
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate())
        except Exception as e:
            last = e
            _mark(f"getOrCreate lần {i + 1} LỖI: {type(e).__name__}: {e}")
            if i + 1 < retries:
                time.sleep(wait_s)
    raise last
spark = _create_spark()
spark.sparkContext.setLogLevel("WARN")
try:
    spark._jvm.org.rocksdb.RocksDB.loadLibrary()
    spark.conf.set("spark.sql.streaming.stateStore.providerClass", ROCKSDB_PROVIDER)
    print("[smoke] state store: RocksDB")
except Exception as e:
    print(f"[smoke] RocksDB không nạp được, dùng provider mặc định ({e})")
# Sanity check pandas UDF worker (phi streaming) để tách lỗi môi trường nhanh
_mark("sanity: pandas UDF tĩnh...")
try:
    from pyspark.sql.types import LongType
    _pdf = spark.createDataFrame([(1,)], ["x"])
    _out = (_pdf.coalesce(1)
            .mapInPandas(lambda it: (pdf.assign(y=pdf["x"] + 1) for pdf in it),
                         "x long, y long").collect())
    _mark(f"sanity OK: {_out}")
except Exception:
    _mark("sanity FAIL")
    _mark_exc()
    raise
def rate_events():
    return (spark.readStream.format("rate").option("rowsPerSecond", "100").load()
            .select(
                F.lit("S001").alias("station_id"),
                F.lit("pm25").alias("param"),
                F.when(F.col("value") % 100 == 0, F.lit(1000.0))
                 .otherwise((F.col("value") % 200).cast("double")).alias("value"),
                (F.lit(1700000000000) + F.col("value") * 1000).alias("event_time_ms"),
                F.lit(None).cast("long").alias("ingest_ts_ms"),
                F.col("value").cast("long").alias("seq")))
def run_phase(seconds, phase, base_rows=0):
    out = (rate_events().groupBy("station_id", "param")
           .applyInPandasWithState(make_state_func(), spark_record_struct(),
                                    _build_state_struct(), "Append",
                                    GroupStateTimeout.NoTimeout))
    # Parquet file sink: memory sink KHONG cho restart tu checkpoint (Spark 4.x)
    q = (out.writeStream.format("parquet")
         .option("path", str(OUT))
         .outputMode("append")
         .option("checkpointLocation", str(CHECK))
         .start())
    end = time.time() + seconds
    last = None
    while time.time() < end:
        time.sleep(5)
        qe = q.exception() if hasattr(q, "exception") else None
        if qe is not None:
            _mark(f"pha {phase}: QUERY LỖI: {qe}")
            raise RuntimeError(f"pha {phase} query failed: {qe}")
        lp = q.lastProgress
        _mark(f"pha {phase}: status={q.status} numInputRows="
              f"{lp['numInputRows'] if lp else None}")
        try:
            n = spark.read.parquet(str(OUT)).count()
            if n and n != last:
                _mark(f"pha {phase}: rows={n}")
                last = n
        except Exception:
            pass
    df = spark.read.parquet(str(OUT)).orderBy("seq", "kind")
    n = df.count()
    a = df.filter(F.col("kind") == "alert").count()
    p = df.filter(F.col("kind") == "probe").count()
    z = df.filter(F.col("detector") == "zscore").count()
    e = df.filter(F.col("detector") == "ewma").count()
    print(f"[smoke] pha {phase} KẾT: rows={n} (pha này +{n - base_rows}) "
          f"alerts={a} (z={z}, ewma={e}) probes={p}")
    _mark(f"pha {phase} KẾT: rows={n} (pha này +{n - base_rows}) "
          f"alerts={a} (z={z}, ewma={e}) probes={p}")
    if phase == 1:
        df.show(10, truncate=False)
    q.stop()
    return n - base_rows
try:
    n1 = run_phase(90, 1, 0)
    assert n1 > 0, "pha 1 không có bản ghi nào"
    # Pha 2: restart từ checkpoint cũ -> phải khôi phục state + offset và tiếp tục chạy
    n2 = run_phase(30, 2, n1)
except Exception as e:
    _mark(f"FAIL: {type(e).__name__}: {e}")
    _mark_exc()
    raise
spark.stop()
print(f"[smoke] OK: pha1={n1} pha2={n2}")
_mark(f"PASS: pha1={n1} pha2={n2}")
