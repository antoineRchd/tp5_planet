#!/usr/bin/env python3
"""
Processeur Kafka-Spark pour traitement en temps réel des découvertes de planètes
Utilise la nouvelle structure de données
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
import os
import json


def create_spark_session():
    """
    Crée une session Spark avec support Kafka
    """
    spark = (
        SparkSession.builder.appName("PlanetKafkaProcessor")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
        )
        .config("spark.sql.warehouse.dir", "/spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_planet_schema():
    """
    Définit le schéma pour les données de planètes depuis Kafka
    """
    return StructType(
        [
            StructField("Name", StringType(), True),
            StructField("Num_Moons", IntegerType(), True),
            StructField("Minerals", IntegerType(), True),
            StructField("Gravity", DoubleType(), True),
            StructField("Sunlight_Hours", DoubleType(), True),
            StructField("Temperature", DoubleType(), True),
            StructField("Rotation_Time", DoubleType(), True),
            StructField("Water_Presence", IntegerType(), True),
            StructField("Colonisable", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )


def engineer_features_streaming(df):
    """
    Applique l'ingénierie des features aux données streaming
    """
    return (
        df.withColumn(
            "temperature_zone",
            when((col("Temperature") >= -50) & (col("Temperature") <= 50), 1).otherwise(
                0
            ),
        )
        .withColumn(
            "earth_like_gravity",
            when((col("Gravity") >= 0.5) & (col("Gravity") <= 2.0), 1).otherwise(0),
        )
        .withColumn(
            "optimal_sunlight",
            when(
                (col("Sunlight_Hours") >= 8) & (col("Sunlight_Hours") <= 16), 1
            ).otherwise(0),
        )
        .withColumn(
            "moderate_rotation",
            when(
                (col("Rotation_Time") >= 12) & (col("Rotation_Time") <= 48), 1
            ).otherwise(0),
        )
        .withColumn(
            "mineral_richness",
            when(col("Minerals") > 200, 1).otherwise(0),
        )
        .withColumn(
            "moon_stability",
            when((col("Num_Moons") >= 1) & (col("Num_Moons") <= 5), 1).otherwise(0),
        )
        .withColumn(
            "habitability_score",
            (
                col("temperature_zone")
                + col("earth_like_gravity")
                + col("optimal_sunlight")
                + col("Water_Presence")
                + col("moderate_rotation")
            )
            / 5.0,
        )
        .withColumn("analysis_timestamp", current_timestamp())
    )


def analyze_streaming_data(df):
    """
    Effectue des analyses en temps réel sur les données streaming
    """
    # Ajout de classifications rapides
    classified_df = (
        df.withColumn(
            "planet_class",
            when(col("habitability_score") >= 0.8, "Highly Habitable")
            .when(col("habitability_score") >= 0.6, "Potentially Habitable")
            .when(col("habitability_score") >= 0.4, "Marginally Habitable")
            .otherwise("Not Habitable"),
        )
        .withColumn(
            "priority",
            when(col("habitability_score") >= 0.8, "HIGH")
            .when(col("habitability_score") >= 0.6, "MEDIUM")
            .when(col("habitability_score") >= 0.4, "LOW")
            .otherwise("NONE"),
        )
        .withColumn(
            "requires_further_study",
            when(
                (col("Water_Presence") == 1) & (col("temperature_zone") == 1), True
            ).otherwise(False),
        )
    )

    return classified_df


def process_kafka_stream():
    """
    Traite le stream Kafka des découvertes de planètes
    """
    print("📡 DÉMARRAGE DU PROCESSEUR KAFKA-SPARK")
    print("=" * 50)

    # Configuration
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")

    print(f"🔗 Kafka Servers: {kafka_servers}")
    print(f"💾 HDFS Namenode: {hdfs_namenode}")

    # Création de la session Spark
    spark = create_spark_session()

    try:
        # Lecture du stream Kafka
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("subscribe", "planet-discoveries")
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Schéma des données de planètes
        planet_schema = define_planet_schema()

        # Parsing des messages JSON
        parsed_df = (
            kafka_df.select(
                from_json(col("value").cast("string"), planet_schema).alias("data")
            )
            .select("data.*")
            .withColumn("processing_time", current_timestamp())
        )

        print("✅ Stream Kafka configuré")

        # Application de l'ingénierie des features
        featured_df = engineer_features_streaming(parsed_df)

        # Analyses en temps réel
        analyzed_df = analyze_streaming_data(featured_df)

        print("✅ Pipeline d'analyse configuré")

        # Configuration de la sortie streaming vers la console
        console_query = (
            analyzed_df.select(
                "Name",
                "Temperature",
                "Gravity",
                "Water_Presence",
                "habitability_score",
                "planet_class",
                "priority",
                "requires_further_study",
                "processing_time",
            )
            .writeStream.outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .trigger(processingTime="10 seconds")
            .start()
        )

        # Configuration de la sortie vers HDFS (checkpoint)
        hdfs_query = (
            analyzed_df.select(
                "Name",
                "Num_Moons",
                "Minerals",
                "Gravity",
                "Sunlight_Hours",
                "Temperature",
                "Rotation_Time",
                "Water_Presence",
                "Colonisable",
                "habitability_score",
                "planet_class",
                "priority",
                "requires_further_study",
                "processing_time",
            )
            .writeStream.outputMode("append")
            .format("parquet")
            .option("path", f"{hdfs_namenode}/streaming/planet_analysis")
            .option("checkpointLocation", f"{hdfs_namenode}/streaming/checkpoints")
            .trigger(processingTime="30 seconds")
            .start()
        )

        print("✅ Streams de sortie configurés")
        print("\n🔄 TRAITEMENT EN COURS...")
        print("📊 Affichage des résultats toutes les 10 secondes")
        print("💾 Sauvegarde HDFS toutes les 30 secondes")
        print("🛑 Ctrl+C pour arrêter")

        # Attendre les streams
        console_query.awaitTermination()
        hdfs_query.awaitTermination()

    except KeyboardInterrupt:
        print("\n🛑 Arrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"❌ Erreur lors du traitement streaming: {e}")
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()
        print("✅ Session Spark fermée")


def process_batch_analytics():
    """
    Effectue des analyses batch sur les données accumulées
    """
    print("\n📊 ANALYSE BATCH DES DONNÉES ACCUMULÉES")
    print("=" * 50)

    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
    spark = create_spark_session()

    try:
        # Lecture des données accumulées
        streaming_path = f"{hdfs_namenode}/streaming/planet_analysis"

        # Vérifier si des données existent
        try:
            df = spark.read.parquet(streaming_path)
            total_planets = df.count()

            if total_planets == 0:
                print("⚠️ Aucune donnée trouvée dans le stream")
                return

            print(f"📊 Total de planètes analysées: {total_planets}")

            # Analyses des classifications
            print("\n🏷️ Distribution des classifications:")
            df.groupBy("planet_class").count().orderBy(desc("count")).show()

            print("\n🎯 Distribution des priorités:")
            df.groupBy("priority").count().orderBy(desc("count")).show()

            # Planètes nécessitant une étude approfondie
            study_required = df.filter(col("requires_further_study") == True)
            study_count = study_required.count()

            print(f"\n🔬 Planètes nécessitant une étude approfondie: {study_count}")
            if study_count > 0:
                study_required.select(
                    "Name",
                    "Temperature",
                    "Water_Presence",
                    "habitability_score",
                    "planet_class",
                ).show()

            # Statistiques temporelles
            print("\n⏰ Analyse temporelle:")
            df.groupBy(
                date_format(col("processing_time"), "yyyy-MM-dd HH").alias("hour")
            ).count().orderBy("hour").show()

            # Top planètes habitables
            print("\n🌟 Top 10 planètes les plus habitables:")
            df.filter(col("habitability_score") > 0.5).orderBy(
                desc("habitability_score")
            ).select(
                "Name",
                "habitability_score",
                "planet_class",
                "Temperature",
                "Water_Presence",
            ).limit(
                10
            ).show()

            # Sauvegarde des résultats batch
            summary_data = [
                (
                    total_planets,
                    df.filter(col("planet_class") == "Highly Habitable").count(),
                    df.filter(col("requires_further_study") == True).count(),
                    df.agg(avg("habitability_score")).collect()[0][0],
                    current_timestamp(),
                )
            ]

            summary_schema = StructType(
                [
                    StructField("total_planets", LongType(), True),
                    StructField("highly_habitable", LongType(), True),
                    StructField("requires_study", LongType(), True),
                    StructField("avg_habitability", DoubleType(), True),
                    StructField("analysis_time", TimestampType(), True),
                ]
            )

            summary_df = spark.createDataFrame(summary_data, summary_schema)
            summary_df.write.mode("append").parquet(
                f"{hdfs_namenode}/streaming/batch_summaries"
            )

            print("✅ Résumé batch sauvegardé")

        except Exception as read_error:
            print(f"⚠️ Impossible de lire les données streaming: {read_error}")
            print("   Assurez-vous que le streaming a été lancé au préalable")

    except Exception as e:
        print(f"❌ Erreur lors de l'analyse batch: {e}")
    finally:
        spark.stop()


def main():
    """
    Fonction principale avec menu de choix
    """
    print("🌍 PROCESSEUR KAFKA-SPARK POUR DÉCOUVERTES DE PLANÈTES")
    print("=" * 60)

    print("\nOptions disponibles:")
    print("1. 📡 Traitement en temps réel (Streaming)")
    print("2. 📊 Analyse batch des données accumulées")
    print("3. 🔄 Les deux (Streaming puis Batch)")

    try:
        choice = input("\nChoisissez une option (1-3): ").strip()

        if choice == "1":
            process_kafka_stream()
        elif choice == "2":
            process_batch_analytics()
        elif choice == "3":
            print("🚀 Lancement du streaming (Ctrl+C pour passer au batch)")
            try:
                process_kafka_stream()
            except KeyboardInterrupt:
                print("\n🔄 Passage à l'analyse batch...")
                process_batch_analytics()
        else:
            print("❌ Option invalide")

    except KeyboardInterrupt:
        print("\n🛑 Programme interrompu")
    except Exception as e:
        print(f"❌ Erreur: {e}")


if __name__ == "__main__":
    main()
