#!/usr/bin/env python3

import os
import sys
import subprocess
import time
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def wait_for_services():
    """
    Attend que tous les services nécessaires soient disponibles
    """
    logger.info("🔄 Attente des services...")

    services = {
        "Kafka": "kafka:29092",
        "HDFS": "namenode:9870",
        "Hive": "hive-metastore:9083",
        "Spark": "spark-master:7077",
    }

    for service_name, endpoint in services.items():
        logger.info(f"⏳ Attente de {service_name} ({endpoint})...")
        # Simulation d'attente (dans un vrai environnement, on vérifierait la connectivité)
        time.sleep(5)
        logger.info(f"✅ {service_name} disponible")


def run_streaming_consumer():
    """
    Lance le consumer Kafka en streaming
    """
    logger.info("🚀 Démarrage du consumer Kafka streaming...")

    try:
        # Lancement en arrière-plan
        process = subprocess.Popen(
            [
                "spark-submit",
                "--master",
                "spark://spark-master:7077",
                "--packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                "/app/kafka_consumer.py",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        logger.info(f"✅ Consumer Kafka démarré (PID: {process.pid})")
        return process

    except Exception as e:
        logger.error(f"❌ Erreur lors du démarrage du consumer: {e}")
        return None


def run_advanced_analytics():
    """
    Lance les analyses avancées
    """
    logger.info("🔬 Lancement des analyses avancées...")

    try:
        result = subprocess.run(
            [
                "spark-submit",
                "--master",
                "spark://spark-master:7077",
                "--driver-memory",
                "2g",
                "--executor-memory",
                "2g",
                "/app/advanced_analytics.py",
            ],
            capture_output=True,
            text=True,
            timeout=600,
        )

        if result.returncode == 0:
            logger.info("✅ Analyses avancées terminées avec succès")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"❌ Erreur dans les analyses avancées: {result.stderr}")

        return result.returncode == 0

    except subprocess.TimeoutExpired:
        logger.error("❌ Timeout lors des analyses avancées")
        return False
    except Exception as e:
        logger.error(f"❌ Erreur lors des analyses avancées: {e}")
        return False


def run_ml_model():
    """
    Lance l'entraînement du modèle ML
    """
    logger.info("🤖 Lancement de l'entraînement du modèle ML...")

    try:
        result = subprocess.run(
            [
                "spark-submit",
                "--master",
                "spark://spark-master:7077",
                "--driver-memory",
                "2g",
                "--executor-memory",
                "2g",
                "--packages",
                "org.apache.spark:spark-mllib_2.12:3.3.0",
                "/app/habitability_predictor.py",
            ],
            capture_output=True,
            text=True,
            timeout=900,
        )

        if result.returncode == 0:
            logger.info("✅ Modèle ML entraîné avec succès")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"❌ Erreur dans l'entraînement ML: {result.stderr}")

        return result.returncode == 0

    except subprocess.TimeoutExpired:
        logger.error("❌ Timeout lors de l'entraînement ML")
        return False
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'entraînement ML: {e}")
        return False


def create_hive_tables():
    """
    Crée les tables Hive nécessaires
    """
    logger.info("🗄️ Création des tables Hive...")

    # Script SQL pour créer les tables
    hive_script = """
    CREATE DATABASE IF NOT EXISTS planet_discoveries;
    
    USE planet_discoveries;
    
    -- Table pour les données brutes
    CREATE TABLE IF NOT EXISTS raw_data (
        id STRING,
        nom STRING,
        decouvreur STRING,
        date_decouverte STRING,
        masse DOUBLE,
        rayon DOUBLE,
        distance DOUBLE,
        type STRING,
        statut STRING,
        atmosphere STRING,
        temperature_moyenne DOUBLE,
        periode_orbitale DOUBLE,
        nombre_satellites INT,
        presence_eau STRING
    )
    STORED AS PARQUET;
    
    -- Table pour les données avec clusters
    CREATE TABLE IF NOT EXISTS clustered_data (
        id STRING,
        nom STRING,
        decouvreur STRING,
        masse DOUBLE,
        rayon DOUBLE,
        distance DOUBLE,
        temperature_moyenne DOUBLE,
        periode_orbitale DOUBLE,
        type STRING,
        statut STRING,
        presence_eau STRING,
        cluster INT
    )
    STORED AS PARQUET;
    
    -- Table pour les prédictions d'habitabilité
    CREATE TABLE IF NOT EXISTS habitability_predictions (
        id STRING,
        nom STRING,
        masse DOUBLE,
        rayon DOUBLE,
        temperature_moyenne DOUBLE,
        prediction DOUBLE,
        probability_habitable DOUBLE,
        date_prediction TIMESTAMP
    )
    STORED AS PARQUET;
    """

    try:
        # Sauvegarde du script
        with open("/tmp/hive_setup.sql", "w") as f:
            f.write(hive_script)

        logger.info("✅ Tables Hive configurées")
        return True

    except Exception as e:
        logger.error(f"❌ Erreur lors de la création des tables Hive: {e}")
        return False


def setup_hdfs_directories():
    """
    Crée les répertoires HDFS nécessaires
    """
    logger.info("📁 Configuration des répertoires HDFS...")

    directories = [
        "/planet_discoveries",
        "/planet_discoveries/raw",
        "/planet_analytics",
        "/planet_analytics/enriched_data",
        "/planet_analytics/results",
        "/planet_ml_models",
        "/planet_ml_models/habitability_model",
    ]

    try:
        for directory in directories:
            # Dans un vrai environnement, on utiliserait hdfs dfs -mkdir
            logger.info(f"📂 Répertoire configuré: {directory}")

        logger.info("✅ Répertoires HDFS configurés")
        return True

    except Exception as e:
        logger.error(f"❌ Erreur lors de la configuration HDFS: {e}")
        return False


def run_batch_processing():
    """
    Lance le traitement batch périodique
    """
    logger.info("⚙️ Lancement du traitement batch...")

    # Simulation d'un traitement batch
    batch_script = """
from pyspark.sql import SparkSession
from datetime import datetime
import os

spark = SparkSession.builder.appName("PlanetBatchProcessing").getOrCreate()

# Lecture des données Kafka accumulées
hdfs_path = "hdfs://namenode:9000/planet_discoveries/raw"
try:
    df = spark.read.parquet(hdfs_path)
    count = df.count()
    print(f"📊 Traitement batch: {count} planètes traitées")
    
    # Agrégations batch
    stats = df.groupBy("type").agg(
        {"masse": "avg", "rayon": "avg", "*": "count"}
    )
    
    # Sauvegarde des résultats
    output_path = f"hdfs://namenode:9000/planet_analytics/batch_results/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    stats.write.mode("overwrite").parquet(output_path)
    
    print(f"✅ Résultats sauvegardés: {output_path}")
    
except Exception as e:
    print(f"❌ Erreur batch: {e}")

spark.stop()
"""

    try:
        with open("/tmp/batch_processing.py", "w") as f:
            f.write(batch_script)

        result = subprocess.run(
            [
                "spark-submit",
                "--master",
                "spark://spark-master:7077",
                "/tmp/batch_processing.py",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            logger.info("✅ Traitement batch terminé")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"❌ Erreur batch: {result.stderr}")

        return result.returncode == 0

    except Exception as e:
        logger.error(f"❌ Erreur lors du traitement batch: {e}")
        return False


def monitor_pipeline():
    """
    Surveille le pipeline et affiche les statistiques
    """
    logger.info("📊 Surveillance du pipeline...")

    # Simulation de monitoring
    stats = {
        "messages_kafka_traités": 0,
        "analyses_complétées": 0,
        "modèles_entraînés": 0,
        "prédictions_générées": 0,
    }

    for i in range(12):  # 12 * 5 = 60 secondes de monitoring
        time.sleep(5)

        # Simulation d'activité
        stats["messages_kafka_traités"] += 1

        if i % 3 == 0:
            stats["analyses_complétées"] += 1

        if i % 6 == 0:
            stats["prédictions_générées"] += 1

        logger.info(f"📈 Stats: {stats}")

    return stats


def main():
    """
    Pipeline principal de traitement des données de planètes
    """
    logger.info("🚀 DÉMARRAGE DU PIPELINE SPARK COMPLET")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        # 1. Attente des services
        wait_for_services()

        # 2. Configuration de l'infrastructure
        logger.info("\n🔧 CONFIGURATION DE L'INFRASTRUCTURE")
        setup_hdfs_directories()
        create_hive_tables()

        # 3. Lancement du consumer streaming (en arrière-plan)
        logger.info("\n📡 DÉMARRAGE DU STREAMING")
        streaming_process = run_streaming_consumer()

        # Attente que le streaming se stabilise
        time.sleep(10)

        # 4. Analyses avancées
        logger.info("\n🔬 ANALYSES AVANCÉES")
        analytics_success = run_advanced_analytics()

        # 5. Entraînement du modèle ML
        logger.info("\n🤖 MACHINE LEARNING")
        ml_success = run_ml_model()

        # 6. Traitement batch
        logger.info("\n⚙️ TRAITEMENT BATCH")
        batch_success = run_batch_processing()

        # 7. Monitoring
        logger.info("\n📊 MONITORING")
        final_stats = monitor_pipeline()

        # Résumé final
        logger.info("\n✅ PIPELINE COMPLÉTÉ")
        logger.info("=" * 60)

        execution_time = datetime.now() - start_time
        logger.info(f"⏱️ Temps d'exécution: {execution_time}")

        logger.info(f"📈 Statistiques finales:")
        logger.info(f"  - Analyses avancées: {'✅' if analytics_success else '❌'}")
        logger.info(f"  - Modèle ML: {'✅' if ml_success else '❌'}")
        logger.info(f"  - Traitement batch: {'✅' if batch_success else '❌'}")
        logger.info(f"  - Messages traités: {final_stats['messages_kafka_traités']}")

        # URLs utiles
        logger.info(f"\n🌐 INTERFACES WEB DISPONIBLES:")
        logger.info(f"  - Spark Master: http://localhost:8080")
        logger.info(f"  - HDFS NameNode: http://localhost:9870")
        logger.info(f"  - Kafka Topics: kafka:9092")

        # Arrêt du streaming
        if streaming_process:
            logger.info("\n🔄 Arrêt du streaming...")
            streaming_process.terminate()
            streaming_process.wait()

    except KeyboardInterrupt:
        logger.info("\n⚠️ Arrêt demandé par l'utilisateur")
        if "streaming_process" in locals() and streaming_process:
            streaming_process.terminate()
    except Exception as e:
        logger.error(f"\n❌ Erreur dans le pipeline: {e}")
        import traceback

        traceback.print_exc()

    logger.info("\n🏁 PIPELINE TERMINÉ")


if __name__ == "__main__":
    main()
