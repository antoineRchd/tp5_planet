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
    Définit le schéma pour les données de planètes (structure JSON API)
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
        avg("Gravity").alias("gravite_moyenne"),
        avg("Temperature").alias("temperature_moyenne"),
        avg("Sunlight_Hours").alias("heures_soleil_moyenne"),
        avg("Rotation_Time").alias("rotation_moyenne"),
        avg("Num_Moons").alias("lunes_moyenne"),
        sum("Minerals").alias("mineraux_total"),
        min("Temperature").alias("temp_min"),
        max("Temperature").alias("temp_max"),
    )

    print("Statistiques générales:")
    stats_df.show()

    # Distribution par présence d'eau
    print("\nDistribution par présence d'eau:")
    water_dist = (
        df.groupBy("Water_Presence")
        .count()
        .withColumn(
            "presence_eau", when(col("Water_Presence") == 1, "Oui").otherwise("Non")
        )
        .select("presence_eau", "count")
        .orderBy(desc("count"))
    )
    water_dist.show()

    # Distribution par colonisabilité
    print("\nDistribution par colonisabilité:")
    colonisable_dist = (
        df.groupBy("Colonisable")
        .count()
        .withColumn(
            "colonisable_label", when(col("Colonisable") == 1, "Oui").otherwise("Non")
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
            (col("Temperature") >= -50)
            & (col("Temperature") <= 50)
            & (col("Gravity") >= 0.5)
            & (col("Gravity") <= 2.0)
            & (col("Water_Presence") == 1)
            & (col("Sunlight_Hours") >= 8)
            & (col("Sunlight_Hours") <= 16),
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
        "Name",
        "Gravity",
        "Temperature",
        "Sunlight_Hours",
        "Water_Presence",
        "Num_Moons",
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
            when((col("Temperature") >= 0) & (col("Temperature") <= 40), 20).otherwise(
                0
            )
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
        "Name",
        "score_colonisation",
        "potentiel_colonisation",
        "Temperature",
        "Gravity",
        "Water_Presence",
        "Minerals",
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
        "Gravity",
        "Temperature",
        "Sunlight_Hours",
        "Rotation_Time",
        "Minerals",
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
                outliers.select("Name", var).show(5)
            else:
                print("  Aucune anomalie détectée pour {}".format(var))

    return df


def save_to_hdfs(spark, raw_data, analysis_results):
    """
    Sauvegarde tous les résultats dans HDFS
    """
    print("\n💾 SAUVEGARDE DANS HDFS")
    print("=" * 50)

    from datetime import datetime

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        # 1. Sauvegarder les données brutes parsées
        raw_path = "hdfs://namenode:9000/planet_analytics/raw_data/planets_{}".format(
            timestamp
        )
        raw_data.write.mode("overwrite").parquet(raw_path)
        print("✅ Données brutes sauvegardées: {}".format(raw_path))

        # 2. Sauvegarder les statistiques de base
        stats_df = raw_data.agg(
            count("*").alias("total_planetes"),
            avg("Gravity").alias("gravite_moyenne"),
            avg("Temperature").alias("temperature_moyenne"),
            avg("Sunlight_Hours").alias("heures_soleil_moyenne"),
            avg("Rotation_Time").alias("rotation_moyenne"),
            avg("Num_Moons").alias("lunes_moyenne"),
            sum("Minerals").alias("mineraux_total"),
            min("Temperature").alias("temp_min"),
            max("Temperature").alias("temp_max"),
        )

        stats_path = (
            "hdfs://namenode:9000/planet_analytics/stats/basic_stats_{}".format(
                timestamp
            )
        )
        stats_df.write.mode("overwrite").parquet(stats_path)
        print("✅ Statistiques de base sauvegardées: {}".format(stats_path))

        # 3. Sauvegarder l'analyse d'habitabilité
        habitability_df = raw_data.withColumn(
            "conditions_habitables",
            when(
                (col("Temperature") >= -50)
                & (col("Temperature") <= 50)
                & (col("Gravity") >= 0.5)
                & (col("Gravity") <= 2.0)
                & (col("Water_Presence") == 1)
                & (col("Sunlight_Hours") >= 8)
                & (col("Sunlight_Hours") <= 16),
                "Potentiellement habitable",
            ).otherwise("Non habitable"),
        )

        habitability_path = (
            "hdfs://namenode:9000/planet_analytics/results/habitability_{}".format(
                timestamp
            )
        )
        habitability_df.write.mode("overwrite").parquet(habitability_path)
        print("✅ Analyse d'habitabilité sauvegardée: {}".format(habitability_path))

        # 4. Sauvegarder l'analyse de colonisation avec scores
        colonisation_df = analysis_results
        colonisation_path = (
            "hdfs://namenode:9000/planet_analytics/results/colonisation_{}".format(
                timestamp
            )
        )
        colonisation_df.write.mode("overwrite").parquet(colonisation_path)
        print("✅ Analyse de colonisation sauvegardée: {}".format(colonisation_path))

        # 5. Sauvegarder le top 10 des planètes
        top10_df = colonisation_df.orderBy(desc("score_colonisation")).limit(10)
        top10_path = "hdfs://namenode:9000/planet_analytics/results/top10_colonisation_{}".format(
            timestamp
        )
        top10_df.write.mode("overwrite").parquet(top10_path)
        print("✅ Top 10 de colonisation sauvegardé: {}".format(top10_path))

        # 6. Créer un fichier de métadonnées
        metadata = spark.createDataFrame(
            [
                (
                    timestamp,
                    raw_data.count(),
                    "planets_analysis",
                    "Analyse complète des découvertes de planètes",
                )
            ],
            ["timestamp", "nb_planetes", "type_analyse", "description"],
        )

        metadata_path = "hdfs://namenode:9000/planet_analytics/metadata_{}".format(
            timestamp
        )
        metadata.write.mode("overwrite").parquet(metadata_path)
        print("✅ Métadonnées sauvegardées: {}".format(metadata_path))

        print("\n🎉 TOUTES LES DONNÉES SAUVEGARDÉES AVEC SUCCÈS DANS HDFS!")
        return True

    except Exception as e:
        print("❌ Erreur lors de la sauvegarde HDFS: {}".format(e))
        import traceback

        traceback.print_exc()
        return False


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
                habitability_results = analyze_habitability_conditions(parsed_batch)
                colonisation_results = analyze_colonisation_potential(parsed_batch)
                detect_outliers(parsed_batch)

                # Sauvegarde complète dans HDFS
                save_to_hdfs(spark, parsed_batch, colonisation_results)

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
