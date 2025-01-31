# Chapter 3 - Connectors

Flink provides built-in data sources (files, directories, sockets, collections) and data sinks (files, stdout/stderr,
sockets) for easy integration.

## Apache Kafka Connector

Flink connector for Apache Kafka enabled reading data from and writing data to Kafka topics with exactly-once
guarantees.

In order to use the Apache Kafka connector in PyFlink jobs, we need to download and add the dependency to our execution
environment:

```bash
mkdir -p opt
wget -P opt https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
```

In our application code we need to add Kafka connector JAR:

```python
from pathlib import Path

jar_path = Path('/opt/flink/connectors/flink-sql-connector-kafka-3.3.0-1.20.jar').absolute()
env.add_jars(f'file://{jar_path}')
```

## Setup Kafka source

Let's define a source: we will read from `raw-events`, on our local Kafka server.
We should assign a `group.id` which defines a consumer group that will read in
parallel from this topic.

```python
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import SimpleStringSchema

KAFKA_BROKERS = "localhost:9092"

source = KafkaSource.builder() \
    .set_bootstrap_servers(KAFKA_BROKERS) \
    .set_topics("raw-events") \
    .set_group_id("chapter-4") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "raw-events")
```

Now let's create the topic and add a sample event using Kafka UI at <http://localhost:8080/> and start the application.

## Setup Kafka Sink

Now let's write some output data to Kafka. We can attach to a stream another operator, that transforms the
value to a JSON and writes the output to Kafka by using `add_sink`.

```python
import json

from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common import Types, SimpleStringSchema

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
```
