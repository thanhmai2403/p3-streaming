"""Chạy job Flink (phần build) trên MiniCluster, parallelism 4, RocksDB + checkpoint. Không cần Kafka.
Giả lập Kafka bằng 4 file (mỗi trạm luôn nằm trong 1 file = 1 partition, giữ thứ tự gửi), đọc bằng
FileSource parallelism 4. So output với tests.test_detector.run_detector.
python -m tests.test_flink_local"""
import sys, tempfile, zlib
from pathlib import Path
from pyflink.common import Configuration, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from common import config, schema
from flink_job.job import build
from flink_job.detector import Params
from tests.make_synth import make
from tests.test_detector import run_detector, compare

P = 4
cfg = config.load()
ev = make(n_stations=6)
tmp = Path(tempfile.mkdtemp())
files = [open(tmp / f"part{i}.jsonl", "w") for i in range(P)]
for e in ev:
    files[zlib.crc32(e["station_id"].encode()) % P].write(schema.dumps(e) + "\n")
files[0].write('{"broken": 1}\nnot json\n')            # 2 bản ghi hỏng phải bị bỏ qua
for f in files: f.close()

conf = Configuration()
conf.set_string("python.executable", sys.executable)
conf.set_string("restart-strategy.type", "none")
import os
conf.set_string("python.execution-mode", os.environ.get("MODE", "process"))
if os.environ.get("TS") is not None:
    cfg["flink"]["assign_timestamps"] = os.environ["TS"] == "1"
conf.set_string("state.backend.type", "rocksdb")
conf.set_string("execution.checkpointing.dir", "file://" + tempfile.mkdtemp())
env = StreamExecutionEnvironment.get_execution_environment(conf)
env.set_parallelism(P)
env.enable_checkpointing(1000)
fs = FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                         *[str(tmp / f"part{i}.jsonl") for i in range(P)]).build()
src = env.from_source(fs, WatermarkStrategy.no_watermarks(), "files")
out = [schema.parse_output(s) for s in build(src, cfg).execute_and_collect()]

al = [(r["seq"], r["detector"], r["score"]) for r in out if r["kind"] == "alert"]
pr = sorted(r["seq"] for r in out if r["kind"] == "probe")
a, pr2, dropped = run_detector(ev, Params.from_config(cfg))
diff = compare(al, a)
assert pr == pr2, (pr, pr2)
assert not (set(dropped) & {r["seq"] for r in out}), "sự kiện trễ bị bỏ mà vẫn có output"
assert all(r["engine"] == "flink" and r["emit_ts_ms"] > 0 for r in out)
print(f"flink outputs={len(out)} alerts={len(al)} probes={len(pr)} late_dropped={len(dropped)} "
      f"max|score diff|={diff:.2e}")
print("FLINK_LOCAL_OK")
