#!/usr/bin/env python3
"""Script de débogage pour le producer Kafka"""

from kafka import KafkaProducer
import json


def test_kafka_producer():
    """Test du producer Kafka avec différentes configurations"""

    # Test 1: Configuration de base
    print("🔍 Test 1: Configuration de base")
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                "utf-8"
            ),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        )
        print("✅ Producer créé avec succès")

        # Test d'envoi simple
        test_data = {"name": "Test-Planet", "temperature": 20}
        future = producer.send("test_topic", value=test_data, key="test-key")
        record_metadata = future.get(timeout=10)
        print(f"✅ Message envoyé - Offset: {record_metadata.offset}")

        producer.close()

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback

        traceback.print_exc()

    # Test 2: Configuration avec problème de clé
    print("\n🔍 Test 2: Test avec clé potentiellement problématique")
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                "utf-8"
            ),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        )

        # Test avec une clé qui pourrait poser problème
        test_data = {"name": "Test-Planet-2", "temperature": 25}

        # Utiliser le nom comme clé
        message_key = test_data.get("name")
        print(f"Clé utilisée: {message_key} (type: {type(message_key)})")

        future = producer.send("test_topic", value=test_data, key=message_key)
        record_metadata = future.get(timeout=10)
        print(f"✅ Message envoyé - Offset: {record_metadata.offset}")

        producer.close()

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_kafka_producer()
