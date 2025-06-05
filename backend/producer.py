from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import os
import logging
import time

# Configuration des logs
logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
MAX_RETRIES = 3
RETRY_DELAY = 2


def create_producer():
    """
    Crée et configure le producer Kafka avec gestion d'erreurs
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                "utf-8"
            ),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
            # Configuration pour la fiabilité
            acks="all",  # Attendre la confirmation de tous les réplicas
            retries=3,  # Nombre de tentatives en cas d'erreur
            retry_backoff_ms=100,
            # Configuration pour les performances
            batch_size=16384,
            linger_ms=10,
            compression_type="gzip",
        )
        logger.info(f"Producer Kafka créé avec succès - Broker: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.error(f"Erreur lors de la création du producer Kafka: {str(e)}")
        raise


# Initialisation du producer global
producer = None


def get_producer():
    """
    Obtient le producer Kafka (avec lazy initialization)
    """
    global producer
    if producer is None:
        producer = create_producer()
    return producer


def send_to_kafka(topic, data, key=None):
    """
    Envoie des données vers un topic Kafka avec gestion d'erreurs et retry

    Args:
        topic (str): Nom du topic Kafka
        data (dict): Données à envoyer
        key (str, optional): Clé pour le partitioning
    """
    for attempt in range(MAX_RETRIES):
        try:
            current_producer = get_producer()

            # Utilisation de l'ID comme clé si disponible
            message_key = key or data.get("id", None)

            # Envoi du message
            future = current_producer.send(topic=topic, value=data, key=message_key)

            # Attendre la confirmation (synchrone)
            record_metadata = future.get(timeout=10)

            logger.info(
                f"Message envoyé avec succès - Topic: {topic}, "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}"
            )

            # S'assurer que le message est envoyé
            current_producer.flush()
            return True

        except KafkaError as e:
            logger.error(
                f"Erreur Kafka (tentative {attempt + 1}/{MAX_RETRIES}): {str(e)}"
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                # Recréer le producer en cas d'erreur
                global producer
                producer = None
            else:
                raise Exception(
                    f"Échec d'envoi vers Kafka après {MAX_RETRIES} tentatives: {str(e)}"
                )

        except Exception as e:
            logger.error(f"Erreur inattendue lors de l'envoi Kafka: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Erreur d'envoi vers Kafka: {str(e)}")


def close_producer():
    """
    Ferme proprement le producer Kafka
    """
    global producer
    if producer:
        try:
            producer.close()
            logger.info("Producer Kafka fermé proprement")
        except Exception as e:
            logger.error(f"Erreur lors de la fermeture du producer: {str(e)}")
        finally:
            producer = None
