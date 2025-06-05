from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os


def create_spark_session():
    """
    Crée une session Spark configurée pour Kafka, HDFS et Hive
    """
    spark = (
        SparkSession.builder.appName("PlanetDiscoveryProcessor")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0",
        )
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
        .config("spark.sql.warehouse.dir", "/spark-warehouse")
        .enableHiveSupport()
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


def process_realtime_analytics(df):
    """
    Traitement en temps réel des données
    """
    # Agrégations en temps réel
    aggregated_df = (
        df.withWatermark("kafka_timestamp", "10 minutes")
        .groupBy(
            window(col("kafka_timestamp"), "5 minutes"), col("type"), col("statut")
        )
        .agg(
            count("*").alias("count"),
            avg("masse").alias("avg_masse"),
            avg("rayon").alias("avg_rayon"),
            avg("distance").alias("avg_distance"),
            avg("temperature_moyenne").alias("avg_temperature"),
        )
    )

    return aggregated_df


def write_to_hdfs(df, path, mode="append"):
    """
    Écrit les données vers HDFS
    """
    df.write.mode(mode).option("path", path).format("parquet").save()


def write_to_hive(df, table_name, mode="append"):
    """
    Écrit les données vers Hive
    """
    df.write.mode(mode).saveAsTable(table_name)


def main():
    """
    Fonction principale de traitement des données
    """
    print("🚀 Démarrage du processeur de découvertes de planètes")

    # Configuration
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = "planet_discoveries"
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")

    # Création de la session Spark
    spark = create_spark_session()

    try:
        # Lecture du stream Kafka
        print(f"📡 Connexion à Kafka: {kafka_servers}")
        print(f"📊 Topic: {topic}")

        df = read_kafka_stream(spark, kafka_servers, topic)

        # Affichage de la structure des données
        df.printSchema()

        # Traitement et affichage en temps réel
        query = (
            df.writeStream.outputMode("append")
            .format("console")
            .option("truncate", False)
            .trigger(processingTime="30 seconds")
            .start()
        )

        # Sauvegarde vers HDFS
        hdfs_query = (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", f"{hdfs_namenode}/planet_discoveries/raw")
            .option("checkpointLocation", "/tmp/checkpoints/hdfs")
            .trigger(processingTime="60 seconds")
            .start()
        )

        # Agrégations en temps réel
        aggregated_df = process_realtime_analytics(df)

        aggregated_query = (
            aggregated_df.writeStream.outputMode("update")
            .format("console")
            .option("truncate", False)
            .trigger(processingTime="60 seconds")
            .start()
        )

        print("✅ Streaming démarré. Appuyez sur Ctrl+C pour arrêter...")

        # Attente des queries
        query.awaitTermination()
        hdfs_query.awaitTermination()
        aggregated_query.awaitTermination()

    except Exception as e:
        print(f"❌ Erreur: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
