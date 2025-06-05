from kafka import KafkaProducer
import json
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_kafka(topic, data):
    producer.send(topic, data)
    producer.flush()
