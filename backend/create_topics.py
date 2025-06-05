from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError, KafkaError
import os
import logging
import time

# Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


def wait_for_kafka(max_retries=30, retry_delay=2):
    """
    Attend que Kafka soit disponible
    """
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER, api_version=(0, 10, 1)
            )
            producer.close()
            logger.info("Kafka est disponible")
            return True
        except Exception as e:
            logger.warning(
                f"Attente de Kafka (tentative {attempt + 1}/{max_retries}): {e}"
            )
            time.sleep(retry_delay)

    logger.error("Kafka n'est pas disponible après toutes les tentatives")
    return False


def create_kafka_topics():
    """
    Crée les topics Kafka nécessaires pour l'application
    """

    # Attendre que Kafka soit disponible
    if not wait_for_kafka():
        raise Exception("Impossible de se connecter à Kafka")

    # Configuration du client admin
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER, client_id="topic_creator"
    )

    # Définition des topics à créer
    topics_to_create = [
        NewTopic(
            name="planet_discoveries",
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                "cleanup.policy": "compact,delete",
                "retention.ms": "604800000",  # 7 jours
                "compression.type": "gzip",
            },
        ),
        NewTopic(
            name="dataset_planets",
            num_partitions=2,
            replication_factor=1,
            topic_configs={
                "cleanup.policy": "compact,delete",
                "retention.ms": "604800000",  # 7 jours
                "compression.type": "gzip",
            },
        ),
    ]

    # Création des topics
    for topic in topics_to_create:
        try:
            fs = admin_client.create_topics(new_topics=[topic], validate_only=False)

            # Attendre la création
            for topic_name, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    logger.info(f"✅ Topic '{topic_name}' créé avec succès")
                except TopicAlreadyExistsError:
                    logger.info(f"ℹ️  Topic '{topic_name}' existe déjà")
                except Exception as e:
                    logger.error(
                        f"❌ Erreur lors de la création du topic '{topic_name}': {e}"
                    )

        except Exception as e:
            logger.error(f"❌ Erreur générale lors de la création des topics: {e}")

    # Fermeture du client admin
    admin_client.close()

    # Vérification que les topics existent
    verify_topics(admin_client)


def verify_topics(admin_client=None):
    """
    Vérifie que les topics ont été créés correctement
    """
    if admin_client is None:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER, client_id="topic_verifier"
        )

    try:
        # Récupération de la liste des topics
        metadata = admin_client.list_topics(timeout=10)
        existing_topics = list(metadata.topics.keys())

        logger.info("📋 Topics Kafka existants:")
        for topic in existing_topics:
            logger.info(f"  - {topic}")

        # Vérification des topics requis
        required_topics = ["planet_discoveries", "dataset_planets"]
        for topic in required_topics:
            if topic in existing_topics:
                logger.info(f"✅ Topic requis '{topic}' trouvé")
            else:
                logger.error(f"❌ Topic requis '{topic}' manquant")

    except Exception as e:
        logger.error(f"❌ Erreur lors de la vérification des topics: {e}")
    finally:
        if admin_client:
            admin_client.close()


def main():
    """
    Fonction principale
    """
    logger.info("🚀 Démarrage de la création des topics Kafka")
    logger.info(f"📡 Broker Kafka: {KAFKA_BROKER}")

    try:
        create_kafka_topics()
        logger.info("✅ Création des topics terminée avec succès")
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création des topics: {e}")
        raise


if __name__ == "__main__":
    main()
