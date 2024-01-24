#!/usr/bin/env python3
import os

from time import time_ns

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    StringSerializer, SerializationContext, MessageField
)
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from model import SensorValue


USERNAME = os.environ.get("REDPANDA_SASL_USERNAME", "admin")
PASSWORD = os.environ.get("REDPANDA_SASL_PASSWORD", "")
SASL_MECH = os.environ.get("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256")
BOOTSTRAP = os.environ.get("REDPANDA_BROKERS", "")
SR_URL = os.environ.get("REDPANDA_SCHEMA_REGISTRY", "")
TOPIC = os.environ.get("REDPANDA_TOPIC", "sensor_sample")


sr_client = SchemaRegistryClient({
    "url": SR_URL,
    "basic.auth.user.info": f"{USERNAME}:{PASSWORD}",
})
sensor_schema = sr_client.get_latest_version(f"{TOPIC}-value")

string_serializer = StringSerializer("utf8")
avro_serializer = AvroSerializer(sr_client, sensor_schema.schema.schema_str, SensorValue.to_dict)
producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": SASL_MECH,
    "sasl.username": USERNAME,
    "sasl.password": PASSWORD,
    "linger.ms": 100,
    "batch.size": (1 << 20),
})

for i in range(10_000):
    value = SensorValue(i)

    producer.produce(
        topic=TOPIC,
        key=string_serializer(str(time_ns())),
        value=avro_serializer(
            value,
            SerializationContext(TOPIC, MessageField.VALUE)
        ),
        on_delivery=lambda e,m: print(
            f"produced {m.key()} to {m.topic()}/{m.partition()} @ offset {m.offset()}"
        )
    )

producer.flush(30)
