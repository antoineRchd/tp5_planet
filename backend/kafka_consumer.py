from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "planet_discoveries",
    bootstrap_servers="localhost:9092",  # localhost car script lancé hors Docker
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("🛰️ En attente de messages Kafka...\n")
for message in consumer:
    print("🔭 Nouvelle découverte reçue :")
    print(json.dumps(message.value, indent=2))
