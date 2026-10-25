"""Smoke test hạ tầng (bước 5): Kafka `sensor` -> Flink (keyed state, RocksDB, checkpoint) -> Kafka `alerts`.

Không phải job thật. Mục đích: chứng minh 5 thứ chạy được trước khi viết detector:
  1. PyFlink 2.2.1 + Python 3.11 chạy trong TaskManager
  2. Kafka connector 5.0.0-2.2 đọc/ghi được
  3. key_by + ValueState (RocksDB) hoạt động
  4. checkpoint ghi được vào /checkpoints/flink
  5. import được module dùng chung `common` (PYTHONPATH=/opt/p3)

Mỗi sự kiện vào sinh 1 bản ghi ra: {"station_id", "param", "seq", "count"}
trong đó count = số sự kiện đã thấy của key đó (lấy từ state).

Chạy:  docker compose exec flink-jobmanager flink run -d -py /opt/p3/flink_job/smoke_job.py
"""
import json

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from common import config


class CountPerKey(KeyedProcessFunction):
    def open(self, ctx: RuntimeContext):
        self.n = ctx.get_state(ValueStateDescriptor("n", Types.LONG()))

    def process_element(self, value, ctx):
        n = (self.n.value() or 0) + 1
        self.n.update(n)
        ev = json.loads(value)
        yield json.dumps({"kind": "smoke", "engine": "flink", "station_id": ev["station_id"],
                          "param": ev["param"], "seq": ev["seq"], "count": n})


def main():
    bootstrap = config.get("kafka.bootstrap_internal")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.get("engine.parallelism"))
    env.enable_checkpointing(config.get("engine.checkpoint_interval_ms"))

    source = (KafkaSource.builder()
              .set_bootstrap_servers(bootstrap)
              .set_topics(config.get("kafka.topic_in"))
              .set_group_id("flink-smoke")
              .set_starting_offsets(KafkaOffsetsInitializer.earliest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    sink = (KafkaSink.builder()
            .set_bootstrap_servers(bootstrap)
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic(config.get("kafka.topic_out"))
                                   .set_value_serialization_schema(SimpleStringSchema())
                                   .build())
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build())

    (env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-sensor")
        .key_by(lambda s: (lambda e: f'{e["station_id"]}|{e["param"]}')(json.loads(s)),
                key_type=Types.STRING())
        .process(CountPerKey(), output_type=Types.STRING())
        .sink_to(sink))

    env.execute("p3-smoke")


if __name__ == "__main__":
    main()
