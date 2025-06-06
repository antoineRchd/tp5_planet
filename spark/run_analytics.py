#!/usr/bin/env python3
"""
Script d'orchestration pour les analyses avancées et modèles d'IA
Utilise la nouvelle structure de données des planètes
"""

import os
import sys
import time
from subprocess import run, PIPE
import json


def run_command(command, description):
    """
    Exécute une commande et affiche les résultats
    """
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")

    try:
        result = run(command, shell=True, capture_output=True, text=True, timeout=1800)

        if result.returncode == 0:
            print(f"✅ {description} - Succès")
            if result.stdout:
                print("📄 Output:")
                print(result.stdout)
        else:
            print(f"❌ {description} - Erreur (code: {result.returncode})")
            if result.stderr:
                print("🚨 Erreur:")
                print(result.stderr)

        return result.returncode == 0

    except Exception as e:
        print(f"❌ Erreur lors de l'exécution: {e}")
        return False


def check_spark_environment():
    """
    Vérifie que l'environnement Spark est correctement configuré
    """
    print("🔍 Vérification de l'environnement Spark...")

    # Variables d'environnement importantes
    env_vars = ["SPARK_MASTER_URL", "KAFKA_BOOTSTRAP_SERVERS", "HDFS_NAMENODE"]

    for var in env_vars:
        value = os.getenv(var)
        if value:
            print(f"  ✅ {var}: {value}")
        else:
            print(f"  ⚠️ {var}: Non défini")

    # Test de connectivité Spark
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    print(f"\n🔗 Test de connectivité Spark Master: {spark_master}")


def run_advanced_analytics():
    """
    Lance les analyses avancées
    """
    print("\n🔬 LANCEMENT DES ANALYSES AVANCÉES")

    command = """
    spark-submit \
        --master $SPARK_MASTER_URL \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 2g \
        --executor-cores 2 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        /app/advanced_analytics.py
    """

    return run_command(command, "Analyses Avancées des Planètes")


def run_habitability_predictor():
    """
    Lance le modèle de prédiction d'habitabilité
    """
    print("\n🤖 LANCEMENT DU MODÈLE D'HABITABILITÉ")

    command = """
    spark-submit \
        --master $SPARK_MASTER_URL \
        --deploy-mode client \
        --driver-memory 4g \
        --executor-memory 4g \
        --executor-cores 2 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0 \
        /app/habitability_predictor.py
    """

    return run_command(command, "Modèle de Prédiction d'Habitabilité")


def run_kafka_integration():
    """
    Lance l'intégration avec Kafka pour traiter les données en temps réel
    """
    print("\n📡 LANCEMENT DE L'INTÉGRATION KAFKA")

    command = """
    spark-submit \
        --master $SPARK_MASTER_URL \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 2g \
        --executor-cores 2 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0 \
        /app/kafka_spark_processor.py
    """

    return run_command(command, "Intégration Kafka-Spark")


def display_summary():
    """
    Affiche un résumé des analyses effectuées
    """
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ DES ANALYSES TERMINÉES")
    print("=" * 60)

    print("\n🔬 Analyses Avancées:")
    print("  • Statistiques descriptives des planètes")
    print("  • Analyse des corrélations entre caractéristiques")
    print("  • Relation présence d'eau vs autres facteurs")
    print("  • Clustering des planètes similaires")
    print("  • Détection d'anomalies")
    print("  • Analyse des facteurs d'habitabilité")

    print("\n🤖 Modèle d'IA - Prédiction d'Habitabilité:")
    print("  • Entraînement de modèles ML (RandomForest, GBT, LogisticRegression)")
    print("  • Ingénierie de features avancées")
    print("  • Évaluation de performance (AUC, Accuracy, F1-Score)")
    print("  • Analyse d'importance des features")
    print("  • Validation croisée pour robustesse")
    print("  • Prédictions sur nouvelles planètes")

    print("\n💾 Données sauvegardées:")
    print("  • HDFS: /planet_analytics/ et /planet_ml_models/")
    print("  • Hive: Tables planet_discoveries.raw_data et clustered_data")
    print("  • Modèle ML persisté pour réutilisation")

    print("\n📈 Facteurs clés identifiés pour l'habitabilité:")
    print("  • Zone de température habitable (-50°C à 50°C)")
    print("  • Gravité terrestre (0.5 à 2.0)")
    print("  • Présence d'eau")
    print("  • Exposition solaire modérée (8-16h)")
    print("  • Rotation modérée (12-48h)")
    print("  • Richesse minérale équilibrée")


def main():
    """
    Fonction principale d'orchestration
    """
    print("🌍 DÉMARRAGE DU PIPELINE D'ANALYSE PLANÉTAIRE")
    print("=" * 60)
    print("🔄 Utilise la nouvelle structure de données:")
    print("   Name, Num_Moons, Minerals, Gravity, Sunlight_Hours,")
    print("   Temperature, Rotation_Time, Water_Presence, Colonisable")
    print("=" * 60)

    start_time = time.time()

    # Étape 1: Vérification de l'environnement
    check_spark_environment()

    # Étape 2: Analyses avancées
    analytics_success = run_advanced_analytics()

    # Étape 3: Modèle d'habitabilité
    if analytics_success:
        predictor_success = run_habitability_predictor()
    else:
        print("⚠️ Analyses avancées échouées, saut du modèle d'habitabilité")
        predictor_success = False

    # Étape 4: Intégration Kafka (optionnelle)
    if analytics_success and predictor_success:
        print("\n🔄 Intégration Kafka disponible mais non lancée automatiquement")
        print("   Pour l'activer: python /app/kafka_spark_processor.py")

    # Résumé final
    end_time = time.time()
    duration = end_time - start_time

    print(f"\n⏱️ Durée totale d'exécution: {duration:.1f} secondes")

    if analytics_success and predictor_success:
        print("✅ PIPELINE COMPLÉTÉ AVEC SUCCÈS!")
        display_summary()
    else:
        print("❌ PIPELINE PARTIELLEMENT ÉCHOUÉ")
        if not analytics_success:
            print("  • Analyses avancées: ÉCHEC")
        if not predictor_success:
            print("  • Modèle d'habitabilité: ÉCHEC")

    return analytics_success and predictor_success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
