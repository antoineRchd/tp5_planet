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
    Définit le schéma pour les données de découverte de planètes
    """
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("nom", StringType(), True),
            StructField("decouvreur", StringType(), True),
            StructField("date_decouverte", StringType(), True),
            StructField("masse", DoubleType(), True),
            StructField("rayon", DoubleType(), True),
            StructField("distance", DoubleType(), True),
            StructField("type", StringType(), True),
            StructField("statut", StringType(), True),
            StructField("atmosphere", StringType(), True),
            StructField("temperature_moyenne", DoubleType(), True),
            StructField("periode_orbitale", DoubleType(), True),
            StructField("nombre_satellites", IntegerType(), True),
            StructField("presence_eau", StringType(), True),
            StructField("timestamp_reception", StringType(), True),
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
        avg("masse").alias("masse_moyenne"),
        avg("rayon").alias("rayon_moyen"),
        avg("distance").alias("distance_moyenne"),
        avg("temperature_moyenne").alias("temperature_moyenne"),
        min("masse").alias("masse_min"),
        max("masse").alias("masse_max"),
    )

    print("Statistiques générales:")
    stats_df.show()

    # Distribution par type
    print("\nDistribution par type de planète:")
    type_dist = df.groupBy("type").count().orderBy(desc("count"))
    type_dist.show()

    # Distribution par statut
    print("\nDistribution par statut:")
    status_dist = df.groupBy("statut").count().orderBy(desc("count"))
    status_dist.show()

    return stats_df


def analyze_habitability_zone(df):
    """
    Analyse de la zone d'habitabilité
    """
    print("\n🌡️ ANALYSE DE LA ZONE D'HABITABILITÉ")
    print("=" * 50)

    # Définition de la zone habitable (température entre -50 et 50°C)
    habitable_zone = df.withColumn(
        "zone_habitable",
        when(
            (col("temperature_moyenne") >= -50) & (col("temperature_moyenne") <= 50),
            "habitable",
        ).otherwise("non_habitable"),
    )

    # Statistiques par zone
    zone_stats = habitable_zone.groupBy("zone_habitable").agg(
        count("*").alias("nombre_planetes"),
        avg("masse").alias("masse_moyenne"),
        avg("rayon").alias("rayon_moyen"),
        avg("distance").alias("distance_moyenne"),
    )

    print("Statistiques par zone d'habitabilité:")
    zone_stats.show()

    # Planètes potentiellement habitables
    potentially_habitable = habitable_zone.filter(
        (col("zone_habitable") == "habitable")
        & (col("rayon") >= 0.5)
        & (col("rayon") <= 2.0)
        & (col("masse") >= 0.1)
        & (col("masse") <= 10.0)
    )

    print(f"\nPlanètes potentiellement habitables:")
    potentially_habitable.select(
        "nom", "masse", "rayon", "temperature_moyenne", "distance"
    ).show()

    return habitable_zone


def detect_outliers(df):
    """
    Détection simple d'outliers avec PySpark
    """
    print("\n🚨 DÉTECTION D'ANOMALIES")
    print("=" * 50)

    # Calcul des quartiles pour la masse
    masse_quartiles = df.approxQuantile("masse", [0.25, 0.5, 0.75], 0.05)
    if len(masse_quartiles) == 3:
        q1, median, q3 = masse_quartiles
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        print(f"Analyse de la masse:")
        print(f"  Q1: {q1:.2f}, Médiane: {median:.2f}, Q3: {q3:.2f}")
        print(f"  Seuils d'anomalie: [{lower_bound:.2f}, {upper_bound:.2f}]")

        # Planètes avec masses anormales
        mass_outliers = df.filter(
            (col("masse") < lower_bound) | (col("masse") > upper_bound)
        )

        print(f"\nPlanètes avec masses anormales:")
        mass_outliers.select("nom", "masse", "type").show()

    # Planètes avec températures extrêmes
    temp_extremes = df.filter(
        (col("temperature_moyenne") < -200) | (col("temperature_moyenne") > 2000)
    )

    print(f"\nPlanètes avec températures extrêmes:")
    temp_extremes.select("nom", "temperature_moyenne", "type").show()

    return df


def main():
    """
    Fonction principale de traitement des données
    """
    print("🚀 PROCESSEUR SIMPLE DE DÉCOUVERTES DE PLANÈTES")
    print("=" * 60)

    # Configuration
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = "planet_discoveries"

    # Création de la session Spark
    spark = create_spark_session()

    try:
        print(f"📡 Connexion à Kafka: {kafka_servers}")
        print(f"📊 Topic: {topic}")

        # Lecture du stream Kafka
        df = read_kafka_stream(spark, kafka_servers, topic)

        # Affichage de la structure des données
        print("\n🔍 Schéma des données:")
        df.printSchema()

        # Pour le mode batch (traitement des données existantes)
        # On peut lire depuis Kafka en mode batch d'abord
        batch_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        if batch_df.count() > 0:
            print(f"\n📊 Messages Kafka trouvés: {batch_df.count()}")

            # Parse des données existantes
            planet_schema = define_planet_schema()
            parsed_batch = batch_df.select(
                from_json(col("value").cast("string"), planet_schema).alias("data")
            ).select("data.*")

            print(f"📋 Planètes parsées: {parsed_batch.count()}")

            if parsed_batch.count() > 0:
                # Analyses sur les données existantes
                calculate_basic_stats(parsed_batch)
                analyze_habitability_zone(parsed_batch)
                detect_outliers(parsed_batch)

                # Sauvegarde des résultats (si possible)
                try:
                    output_path = "/tmp/planet_analysis_results"
                    parsed_batch.write.mode("overwrite").json(output_path)
                    print(f"\n💾 Résultats sauvegardés: {output_path}")
                except Exception as e:
                    print(f"⚠️ Impossible de sauvegarder: {e}")

        else:
            print("\n⚠️ Aucune donnée trouvée dans Kafka")
            print("Envoyez des découvertes via l'API Flask d'abord!")

        print("\n✅ ANALYSE TERMINÉE")

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
