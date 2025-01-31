import json
import logging
import sys
import inspect

from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.common import Types, SimpleStringSchema, WatermarkStrategy

KAFKA_BROKERS = "localhost:9092"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    jar_path = Path('/opt/flink/connectors/flink-sql-connector-kafka-3.3.0-1.20.jar').absolute()
    env.add_jars(f'file://{jar_path}')

    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("raw-events") \
        .set_group_id("chapter-4") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "raw-events")

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
                               .set_topic("enriched-events")
                               .set_value_serialization_schema(SimpleStringSchema())
                               .build()
                               ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    def enrich(json_event: str) -> str:
        e = json.loads(json_event)
        e["tag"] = "flink"
        return json.dumps(e)

    stream = stream.map(enrich, Types.STRING())
    stream.sink_to(sink)

    stream.print()
    env.execute("Chapter 4")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()
