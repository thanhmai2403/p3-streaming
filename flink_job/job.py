"""Job Flink (A): Kafka `sensor` -> parse -> key_by(station_id, param) -> z-score + EWMA -> Kafka `alerts`.

Cài đúng README mục 5–6. Logic phát hiện nằm ở flink_job/detector.py (Python thuần, test riêng được).

Chạy (trong container JobManager):
  docker compose exec flink-jobmanager flink run -d -py /opt/p3/flink_job/job.py \
      --run-id rq1_flink_rate20000_r1 [--delivery EXACTLY_ONCE]

- Không dùng timer event-time (README 6.3) nên mặc định không gán timestamp/watermark
  (flink.assign_timestamps). Python chạy ở thread mode (flink.python_execution_mode).
- Sự kiện trễ quá 2 giờ event-time so với max của key: bỏ, không phát gì, tăng bộ đếm
  `late_dropped` (metric của Flink + dòng log trong log TaskManager).
- Checkpoint: /checkpoints/flink/<run_id>, state backend RocksDB, mỗi 10 s.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import FlatMapFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from common import config, schema
from flink_job.detector import Params, is_probe, new_state, step

ENGINE = "flink"
log = logging.getLogger("p3.flink")

# (station_id, param, value, event_time_ms, ingest_ts_ms, seq)
EVENT_TYPE = Types.TUPLE([Types.STRING(), Types.STRING(), Types.DOUBLE(),
                          Types.LONG(), Types.LONG(), Types.LONG()])


class ParseEvent(FlatMapFunction):
    """JSON -> tuple. Bản ghi hỏng thì bỏ qua + ghi log, không làm chết job."""

    def open(self, ctx: RuntimeContext):
        self.bad = ctx.get_metrics_group().counter("bad_records")

    def flat_map(self, raw):
        try:
            e = schema.parse_event(raw)
        except Exception as ex:  # noqa: BLE001
            self.bad.inc()
            log.warning("bỏ bản ghi hỏng: %s (%s)", str(raw)[:200], ex)
            return
        yield (e["station_id"], e["param"], e["value"], e["event_time_ms"],
               e["ingest_ts_ms"], e["seq"])


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[3]


class Detect(KeyedProcessFunction):
    def __init__(self, params: Params):
        self.p = params

    def open(self, ctx: RuntimeContext):
        self.state = ctx.get_state(ValueStateDescriptor("det", Types.PICKLED_BYTE_ARRAY()))
        self.late_counter = ctx.get_metrics_group().counter("late_dropped")
        self.late_local = 0
        try:                                   # thread mode (pemja) không có hàm này
            self.subtask = ctx.get_index_of_this_subtask()
        except Exception:  # noqa: BLE001
            self.subtask = -1

    def process_element(self, value, ctx):
        station_id, param, x, et, ingest, seq = value
        st = self.state.value() or new_state()
        dropped, alerts = step(st, x, et, self.p)
        if dropped:
            self.late_counter.inc()
            self.late_local += 1
            log.info("late_dropped subtask=%d total=%d seq=%d key=%s|%s",
                     self.subtask, self.late_local, seq, station_id, param)
            return
        self.state.update(st)

        emit = schema.now_ms()
        ev = {"station_id": station_id, "param": param, "value": x, "event_time_ms": et,
              "ingest_ts_ms": ingest, "seq": seq}
        for det, score in alerts:
            yield schema.dumps(schema.make_alert(ev, ENGINE, det, score, emit))
        if is_probe(seq, self.p):
            yield schema.dumps(schema.make_probe(ev, ENGINE, emit))

    def close(self):
        log.info("late_dropped_final subtask=%d total=%d", self.subtask, self.late_local)


def build(raw: DataStream, cfg: dict) -> DataStream:
    """Phần xử lý, tách khỏi Kafka để test được.

    QUAN TRỌNG về thứ tự: mọi toán tử trước key_by (parse, gán timestamp, và cả toán tử Python
    tách key mà PyFlink tự chèn khi key_by) phải CÙNG parallelism với source để nối thẳng (forward),
    nhờ đó giữ nguyên thứ tự trong từng partition Kafka. Vì vậy KHÔNG đặt set_parallelism riêng
    cho các bước này. Nếu source có parallelism khác (vd from_collection = 1), Flink sẽ chia
    round-robin và làm xáo trộn thứ tự của từng key -> sai z-score/EWMA và bỏ nhầm sự kiện "trễ".
    """
    params = Params.from_config(cfg)
    s = raw.flat_map(ParseEvent(), output_type=EVENT_TYPE).name("parse")
    if cfg["flink"].get("assign_timestamps", False):
        # Không bắt buộc: quy tắc trễ (README 6.3) tự cài trong state, không dùng timer event-time.
        # Bỏ bước này giúp PyFlink nhanh hơn khoảng 40% (thêm một toán tử Python cho mỗi sự kiện).
        wm = (WatermarkStrategy
              .for_bounded_out_of_orderness(Duration.of_millis(int(cfg["flink"]["watermark_ms"])))
              .with_timestamp_assigner(EventTime()))
        s = s.assign_timestamps_and_watermarks(wm)
    return (s
            .key_by(lambda e: e[0] + "|" + e[1], key_type=Types.STRING())
            .process(Detect(params), output_type=Types.STRING()).name("detect"))


def make_env(cfg: dict, run_id: str) -> StreamExecutionEnvironment:
    conf = Configuration()
    conf.set_string("state.backend.type", cfg["engine"]["state_backend"])
    conf.set_string("execution.checkpointing.dir", f'{cfg["flink"]["checkpoint_dir"]}/{run_id}')
    conf.set_string("python.execution-mode", cfg["flink"].get("python_execution_mode", "process"))
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(int(cfg["engine"]["parallelism"]))
    env.enable_checkpointing(int(cfg["engine"]["checkpoint_interval_ms"]))
    return env


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True, help="vd rq1_flink_rate20000_r1 (README 10.4)")
    ap.add_argument("--delivery", choices=["AT_LEAST_ONCE", "EXACTLY_ONCE"], default=None,
                    help="mặc định lấy flink.delivery_guarantee trong config.yaml")
    args = ap.parse_args(argv)

    cfg = config.load()
    kc, fc = cfg["kafka"], cfg["flink"]
    delivery = args.delivery or fc["delivery_guarantee"]
    env = make_env(cfg, args.run_id)

    source = (KafkaSource.builder()
              .set_bootstrap_servers(kc["bootstrap_internal"])
              .set_topics(kc["topic_in"])
              .set_group_id(fc["consumer_group"])
              .set_starting_offsets(KafkaOffsetsInitializer.earliest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    sink_b = (KafkaSink.builder()
              .set_bootstrap_servers(kc["bootstrap_internal"])
              .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                     .set_topic(kc["topic_out"])
                                     .set_value_serialization_schema(SimpleStringSchema())
                                     .build()))
    if delivery == "EXACTLY_ONCE":
        sink_b = (sink_b.set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
                  .set_transactional_id_prefix(f'{fc["transactional_id_prefix"]}-{args.run_id}')
                  .set_property("transaction.timeout.ms", str(fc["transaction_timeout_ms"])))
    else:
        sink_b = sink_b.set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)

    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-sensor")
    build(raw, cfg).sink_to(sink_b.build()).name("kafka-alerts")

    print(f"[p3] run_id={args.run_id} delivery={delivery}", file=sys.stderr)
    env.execute(f"p3-flink-{args.run_id}")


if __name__ == "__main__":
    main()
