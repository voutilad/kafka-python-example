#!/usr/bin/env python3
import os

from uuid import uuid4
from time import time_ns

from confluent_kafka import Consumer
from confluent_kafka.serialization import (
    StringSerializer, SerializationContext, MessageField
)
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

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

avro_deserializer = AvroDeserializer(
    sr_client,
    sensor_schema.schema.schema_str,
    SensorValue.from_dict
)

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": SASL_MECH,
    "sasl.username": USERNAME,
    "sasl.password": PASSWORD,
    "auto.offset.reset": "earliest",
    "group.id": "kafka-python-example",
})
consumer.subscribe([TOPIC])


while True:
    try:
        msg = consumer.poll(2)
        if msg is None:
            print("...tick")
            continue
        value = avro_deserializer(
            msg.value(),
            SerializationContext(msg.topic(), MessageField.VALUE)
        )
        print(f"Got value @ {msg.partition()}: key={msg.key()}, {value}")

    except Exception as e:
        print(e)
        break

consumer.close()
