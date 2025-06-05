# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


def create_spark_session():
    """
    Crée une session Spark configurée pour Kafka
    """
    spark = (
        SparkSession.builder.appName("PlanetDiscoveryProcessor")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
        )
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_planet_schema():
    """
    Définit le schéma pour les données de planètes (structure CSV)
    """
    return StructType(
        [
            StructField("name", StringType(), True),
            StructField("num_moons", IntegerType(), True),
            StructField("minerals", IntegerType(), True),
            StructField("gravity", DoubleType(), True),
            StructField("sunlight_hours", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("rotation_time", DoubleType(), True),
            StructField("water_presence", IntegerType(), True),
            StructField("colonisable", IntegerType(), True),
            StructField("timestamp_reception", StringType(), True),
            StructField(
                "source", StringType(), True
            ),  # pour distinguer dataset vs nouvelles découvertes
        ]
    )


def read_kafka_stream(spark, kafka_servers, topic):
    """
    Lit le stream Kafka et parse les données JSON
    """
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON data
    planet_schema = define_planet_schema()

    parsed_df = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), planet_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("data.*", "kafka_timestamp")

    return parsed_df


def calculate_basic_stats(df):
    """
    Calcule des statistiques de base avec PySpark
    """
    print("📊 CALCUL DES STATISTIQUES DE BASE")
    print("=" * 50)

    # Agrégations simples
    stats_df = df.agg(
        count("*").alias("total_planetes"),
        avg("gravity").alias("gravite_moyenne"),
        avg("temperature").alias("temperature_moyenne"),
        avg("sunlight_hours").alias("heures_soleil_moyenne"),
        avg("rotation_time").alias("rotation_moyenne"),
        avg("num_moons").alias("lunes_moyenne"),
        sum("minerals").alias("mineraux_total"),
        min("temperature").alias("temp_min"),
        max("temperature").alias("temp_max"),
    )

    print("Statistiques générales:")
    stats_df.show()

    # Distribution par présence d'eau
    print("\nDistribution par présence d'eau:")
    water_dist = (
        df.groupBy("water_presence")
        .count()
        .withColumn(
            "presence_eau", when(col("water_presence") == 1, "Oui").otherwise("Non")
        )
        .select("presence_eau", "count")
        .orderBy(desc("count"))
    )
    water_dist.show()

    # Distribution par colonisabilité
    print("\nDistribution par colonisabilité:")
    colonisable_dist = (
        df.groupBy("colonisable")
        .count()
        .withColumn(
            "colonisable_label", when(col("colonisable") == 1, "Oui").otherwise("Non")
        )
        .select("colonisable_label", "count")
        .orderBy(desc("count"))
    )
    colonisable_dist.show()

    return stats_df


def analyze_habitability_conditions(df):
    """
    Analyse des conditions d'habitabilité
    """
    print("\n🌍 ANALYSE DES CONDITIONS D'HABITABILITÉ")
    print("=" * 50)

    # Conditions pour l'habitabilité
    habitable_conditions = df.withColumn(
        "conditions_habitables",
        when(
            (col("temperature") >= -50)
            & (col("temperature") <= 50)
            & (col("gravity") >= 0.5)
            & (col("gravity") <= 2.0)
            & (col("water_presence") == 1)
            & (col("sunlight_hours") >= 8)
            & (col("sunlight_hours") <= 16),
            "Potentiellement habitable",
        ).otherwise("Non habitable"),
    )

    # Statistiques par conditions d'habitabilité
    habitability_stats = habitable_conditions.groupBy("conditions_habitables").agg(
        count("*").alias("nombre_planetes"),
        avg("gravity").alias("gravite_moyenne"),
        avg("temperature").alias("temperature_moyenne"),
        avg("sunlight_hours").alias("heures_soleil_moyenne"),
    )

    print("Statistiques par conditions d'habitabilité:")
    habitability_stats.show()

    # Planètes potentiellement habitables
    potentially_habitable = habitable_conditions.filter(
        col("conditions_habitables") == "Potentiellement habitable"
    )

    print("\nPlanètes potentiellement habitables:")
    potentially_habitable.select(
        "name",
        "gravity",
        "temperature",
        "sunlight_hours",
        "water_presence",
        "num_moons",
    ).show()

    return habitable_conditions


def analyze_colonisation_potential(df):
    """
    Analyse du potentiel de colonisation
    """
    print("\n🚀 ANALYSE DU POTENTIEL DE COLONISATION")
    print("=" * 50)

    # Score de colonisation basé sur plusieurs facteurs
    colonisation_score = df.withColumn(
        "score_colonisation",
        (
            # Température favorable (0-40°C)
            when((col("temperature") >= 0) & (col("temperature") <= 40), 20).otherwise(
                0
            )
            +
            # Gravité proche de la Terre (0.8-1.2)
            when((col("gravity") >= 0.8) & (col("gravity") <= 1.2), 25).otherwise(0)
            +
            # Présence d'eau
            when(col("water_presence") == 1, 30).otherwise(0)
            +
            # Heures de soleil adéquates (10-14h)
            when(
                (col("sunlight_hours") >= 10) & (col("sunlight_hours") <= 14), 15
            ).otherwise(0)
            +
            # Ressources minérales abondantes (>500)
            when(col("minerals") > 500, 10).otherwise(5)
        ),
    ).withColumn(
        "potentiel_colonisation",
        when(col("score_colonisation") >= 80, "Excellent")
        .when(col("score_colonisation") >= 60, "Bon")
        .when(col("score_colonisation") >= 40, "Moyen")
        .otherwise("Faible"),
    )

    # Distribution des scores de colonisation
    colonisation_dist = (
        colonisation_score.groupBy("potentiel_colonisation")
        .agg(
            count("*").alias("nombre_planetes"),
            avg("score_colonisation").alias("score_moyen"),
        )
        .orderBy(desc("score_moyen"))
    )

    print("Distribution du potentiel de colonisation:")
    colonisation_dist.show()

    # Top 10 des planètes pour la colonisation
    top_colonisation = colonisation_score.orderBy(desc("score_colonisation")).limit(10)

    print("\nTop 10 des planètes pour la colonisation:")
    top_colonisation.select(
        "name",
        "score_colonisation",
        "potentiel_colonisation",
        "temperature",
        "gravity",
        "water_presence",
        "minerals",
    ).show()

    return colonisation_score


def detect_outliers(df):
    """
    Détection simple d'outliers avec PySpark
    """
    print("\n🚨 DÉTECTION D'ANOMALIES")
    print("=" * 50)

    # Calcul des quartiles pour différentes variables
    variables = [
        "gravity",
        "temperature",
        "sunlight_hours",
        "rotation_time",
        "minerals",
    ]

    for var in variables:
        print("\nAnalyse de {}:".format(var))
        quartiles = df.approxQuantile(var, [0.25, 0.5, 0.75], 0.05)
        if len(quartiles) == 3:
            q1, median, q3 = quartiles
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr

            print("  Q1: {:.2f}, Médiane: {:.2f}, Q3: {:.2f}".format(q1, median, q3))
            print(
                "  Seuils d'anomalie: [{:.2f}, {:.2f}]".format(lower_bound, upper_bound)
            )

            # Planètes avec valeurs anormales
            outliers = df.filter((col(var) < lower_bound) | (col(var) > upper_bound))

            outlier_count = outliers.count()
            if outlier_count > 0:
                print(
                    "  Planètes avec {} anormal ({} trouvées):".format(
                        var, outlier_count
                    )
                )
                outliers.select("name", var).show(5)
            else:
                print("  Aucune anomalie détectée pour {}".format(var))

    return df


def main():
    """
    Fonction principale de traitement des données
    """
    print("🚀 PROCESSEUR DE DONNÉES PLANÉTAIRES")
    print("=" * 60)

    # Configuration
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = "planet_discoveries"

    # Création de la session Spark
    spark = create_spark_session()

    try:
        print("📡 Connexion à Kafka: {}".format(kafka_servers))
        print("📊 Topic: {}".format(topic))

        # Pour le mode batch (traitement des données existantes)
        batch_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        if batch_df.count() > 0:
            print("\n📊 Messages Kafka trouvés: {}".format(batch_df.count()))

            # Parse des données existantes
            planet_schema = define_planet_schema()
            parsed_batch = batch_df.select(
                from_json(col("value").cast("string"), planet_schema).alias("data")
            ).select("data.*")

            print("📋 Planètes parsées: {}".format(parsed_batch.count()))

            if parsed_batch.count() > 0:
                # Analyses sur les données existantes
                calculate_basic_stats(parsed_batch)
                analyze_habitability_conditions(parsed_batch)
                analyze_colonisation_potential(parsed_batch)
                detect_outliers(parsed_batch)

                # Sauvegarde des résultats (si possible)
                try:
                    output_path = "/tmp/planet_analysis_results"
                    parsed_batch.write.mode("overwrite").json(output_path)
                    print("\n💾 Résultats sauvegardés: {}".format(output_path))
                except Exception as e:
                    print("⚠️ Impossible de sauvegarder: {}".format(e))

        else:
            print("\n⚠️ Aucune donnée trouvée dans Kafka")
            print("Envoyez des données via l'API Flask d'abord!")
            print("Exemple:")
            print("curl -X POST http://localhost:5001/discoveries/dataset")

        print("\n✅ ANALYSE TERMINÉE")

    except Exception as e:
        print("❌ Erreur: {}".format(e))
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
