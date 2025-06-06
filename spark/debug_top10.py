#!/usr/bin/env python3
"""
Script de debug pour le top 10 des planètes
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


def main():
    print("🔍 DEBUG TOP 10 PLANÈTES")
    print("=" * 40)

    # Configuration Spark
    spark = SparkSession.builder.appName("DebugTop10").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Configuration Kafka
    kafka_servers = "kafka:29092"
    topic = "planet_discoveries"

    # Schema des planètes
    planet_schema = StructType(
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
            StructField("timestamp_reception", StringType(), True),
            StructField("source", StringType(), True),
        ]
    )

    # Lire les données Kafka
    batch_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    print(f"📊 Messages Kafka: {batch_df.count()}")

    if batch_df.count() > 0:
        # Parser les données
        parsed_batch = batch_df.select(
            from_json(col("value").cast("string"), planet_schema).alias("data")
        ).select("data.*")

        print(f"📋 Planètes parsées: {parsed_batch.count()}")

        # Afficher le schema
        print("\n📝 Schema des données:")
        parsed_batch.printSchema()

        # Afficher quelques données
        print("\n📋 Échantillon des données:")
        parsed_batch.show(5, truncate=False)

        # Calculer le score de colonisation
        print("\n🔬 Calcul du score de colonisation...")

        colonisation_score = parsed_batch.withColumn(
            "score_colonisation",
            (
                # Température favorable (0-40°C)
                when(
                    (col("Temperature") >= 0) & (col("Temperature") <= 40), 20
                ).otherwise(0)
                +
                # Gravité proche de la Terre (0.8-1.2)
                when((col("Gravity") >= 0.8) & (col("Gravity") <= 1.2), 25).otherwise(0)
                +
                # Présence d'eau
                when(col("Water_Presence") == 1, 30).otherwise(0)
                +
                # Heures de soleil adéquates (10-14h)
                when(
                    (col("Sunlight_Hours") >= 10) & (col("Sunlight_Hours") <= 14), 15
                ).otherwise(0)
                +
                # Ressources minérales abondantes (>500)
                when(col("Minerals") > 500, 10).otherwise(5)
            ),
        ).withColumn(
            "potentiel_colonisation",
            when(col("score_colonisation") >= 80, "Excellent")
            .when(col("score_colonisation") >= 60, "Bon")
            .when(col("score_colonisation") >= 40, "Moyen")
            .otherwise("Faible"),
        )

        print("\n📊 Données avec scores:")
        colonisation_score.select(
            "Name", "score_colonisation", "potentiel_colonisation"
        ).show()

        print("\n🔍 Schema après calcul du score:")
        colonisation_score.printSchema()

        # Tester le top 10
        print("\n🏆 Test du Top 10:")
        top_colonisation = colonisation_score.orderBy(desc("score_colonisation")).limit(
            10
        )

        print(f"Nombre de lignes dans le top: {top_colonisation.count()}")

        # Afficher avec toutes les colonnes d'abord
        print("\n📋 Toutes les colonnes du top:")
        top_colonisation.show()

        # Puis seulement les colonnes qui nous intéressent
        print("\n🎯 Colonnes sélectionnées:")
        try:
            top_colonisation.select(
                "Name",
                "score_colonisation",
                "potentiel_colonisation",
                "Temperature",
                "Gravity",
                "Water_Presence",
                "Minerals",
            ).show()
        except Exception as e:
            print(f"❌ Erreur lors de la sélection: {e}")

            # Essayons colonne par colonne
            print("\n🔍 Test colonne par colonne:")
            for col_name in [
                "Name",
                "score_colonisation",
                "potentiel_colonisation",
                "Temperature",
                "Gravity",
                "Water_Presence",
                "Minerals",
            ]:
                try:
                    print(f"   ✅ {col_name} existe")
                    top_colonisation.select(col_name).show(1)
                except Exception as col_error:
                    print(f"   ❌ {col_name} : {col_error}")

    spark.stop()


if __name__ == "__main__":
    main()
