from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.stat import Correlation
from pyspark.ml.evaluation import ClusteringEvaluator
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json


def create_spark_session():
    """
    Crée une session Spark pour les analyses avancées
    """
    spark = (
        SparkSession.builder.appName("PlanetAdvancedAnalytics")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.warehouse.dir", "/spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_planet_data(spark, hdfs_path=None):
    """
    Charge les données de planètes depuis HDFS ou depuis un fichier local
    """
    if hdfs_path:
        try:
            df = spark.read.parquet(hdfs_path)
            print(f"✅ Données chargées depuis HDFS: {hdfs_path}")
            return df
        except Exception as e:
            print(f"⚠️ Erreur HDFS: {e}, utilisation des données de test")

    # Données de test si HDFS n'est pas disponible
    test_data = [
        (
            "planet-1",
            "Kepler-442b",
            "Equipe Kepler",
            "2015-01-06",
            2.34,
            1.34,
            1206.0,
            "super-terre",
            "confirmée",
            "inconnue",
            -40.0,
            112.3,
            0,
            "inconnue",
        ),
        (
            "planet-2",
            "Kepler-452b",
            "Mission Kepler",
            "2015-07-23",
            5.0,
            1.6,
            1400.0,
            "super-terre",
            "confirmée",
            "dense",
            5.0,
            385.0,
            1,
            "inconnue",
        ),
        (
            "planet-3",
            "HD 40307g",
            "Dr. Mikko Tuomi",
            "2012-11-07",
            7.1,
            1.8,
            42.0,
            "super-terre",
            "confirmée",
            "dense",
            15.0,
            197.8,
            2,
            "oui",
        ),
        (
            "planet-4",
            "Proxima Centauri b",
            "Guillem Anglada-Escudé",
            "2016-08-24",
            1.17,
            1.1,
            4.24,
            "terrestre",
            "confirmée",
            "mince",
            -39.0,
            11.2,
            0,
            "inconnue",
        ),
        (
            "planet-5",
            "TRAPPIST-1e",
            "Michaël Gillon",
            "2017-02-22",
            0.772,
            0.918,
            39.0,
            "terrestre",
            "confirmée",
            "mince",
            -22.0,
            6.1,
            0,
            "oui",
        ),
        (
            "planet-6",
            "Gliese 667Cc",
            "ESO",
            "2011-11-21",
            3.7,
            1.5,
            23.6,
            "super-terre",
            "confirmée",
            "dense",
            -3.0,
            28.1,
            0,
            "oui",
        ),
        (
            "planet-7",
            "K2-18b",
            "Ryan Cloutier",
            "2015-12-07",
            8.6,
            2.3,
            124.0,
            "super-terre",
            "confirmée",
            "hydrogène",
            -23.0,
            33.0,
            0,
            "oui",
        ),
        (
            "planet-8",
            "TOI-715b",
            "Georgina Dransfield",
            "2024-01-31",
            3.02,
            1.55,
            137.0,
            "super-terre",
            "confirmée",
            "mince",
            15.0,
            19.3,
            0,
            "inconnue",
        ),
        (
            "planet-9",
            "LP 890-9c",
            "Laetitia Delrez",
            "2022-09-05",
            2.6,
            1.4,
            105.0,
            "super-terre",
            "confirmée",
            "mince",
            -25.0,
            8.8,
            0,
            "inconnue",
        ),
        (
            "planet-10",
            "GJ 357d",
            "Rafael Luque",
            "2019-07-31",
            6.1,
            1.7,
            31.0,
            "super-terre",
            "confirmée",
            "dense",
            -53.0,
            55.7,
            0,
            "inconnue",
        ),
    ]

    schema = StructType(
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
        ]
    )

    df = spark.createDataFrame(test_data, schema)
    print("✅ Données de test créées")
    return df


def calculate_basic_statistics(df):
    """
    Calcule les statistiques de base
    """
    print("\n📊 STATISTIQUES DE BASE")
    print("=" * 50)

    # Statistiques numériques
    numeric_cols = [
        "masse",
        "rayon",
        "distance",
        "temperature_moyenne",
        "periode_orbitale",
        "nombre_satellites",
    ]
    stats_df = df.select(numeric_cols).describe()
    stats_df.show()

    # Distribution par type de planète
    print("\n🌍 Distribution par type de planète:")
    df.groupBy("type").count().orderBy(desc("count")).show()

    # Distribution par statut
    print("\n✅ Distribution par statut:")
    df.groupBy("statut").count().orderBy(desc("count")).show()

    # Distribution par présence d'eau
    print("\n💧 Distribution par présence d'eau:")
    df.groupBy("presence_eau").count().orderBy(desc("count")).show()

    return stats_df


def calculate_correlations(df):
    """
    Calcule les corrélations entre les variables numériques
    """
    print("\n🔗 ANALYSE DES CORRÉLATIONS")
    print("=" * 50)

    # Préparation des données numériques
    numeric_cols = [
        "masse",
        "rayon",
        "distance",
        "temperature_moyenne",
        "periode_orbitale",
        "nombre_satellites",
    ]

    # Création d'un vecteur de features
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    vector_df = assembler.transform(df).select("features")

    # Calcul de la matrice de corrélation
    correlation_matrix = Correlation.corr(vector_df, "features").head()
    correlation_array = correlation_matrix[0].toArray()

    # Affichage des corrélations importantes
    print("\n🔍 Corrélations significatives (|r| > 0.5):")
    for i, col1 in enumerate(numeric_cols):
        for j, col2 in enumerate(numeric_cols):
            if i < j:  # Éviter les doublons
                corr_value = correlation_array[i][j]
                if abs(corr_value) > 0.5:
                    print(f"  {col1} ↔ {col2}: {corr_value:.3f}")

    return correlation_array, numeric_cols


def analyze_water_relationships(df):
    """
    Analyse spécifique des relations avec la présence d'eau
    """
    print("\n💧 ANALYSE: PRÉSENCE D'EAU vs AUTRES CARACTÉRISTIQUES")
    print("=" * 60)

    # Statistiques par présence d'eau
    water_stats = df.groupBy("presence_eau").agg(
        avg("distance").alias("distance_moyenne"),
        avg("temperature_moyenne").alias("temp_moyenne"),
        avg("masse").alias("masse_moyenne"),
        avg("rayon").alias("rayon_moyen"),
        count("*").alias("nombre_planetes"),
    )

    print("📈 Statistiques moyennes par présence d'eau:")
    water_stats.show()

    # Zone habitable (température entre -50 et 50°C)
    habitable_zone = df.filter(
        (col("temperature_moyenne") >= -50) & (col("temperature_moyenne") <= 50)
    )

    print(f"\n🌡️ Planètes dans la zone de température habitable (-50°C à 50°C):")
    print(f"Total: {habitable_zone.count()}/{df.count()}")

    habitable_zone.select(
        "nom", "temperature_moyenne", "presence_eau", "distance", "type"
    ).show()

    return water_stats


def perform_clustering(df):
    """
    Effectue un clustering des planètes
    """
    print("\n🎯 CLUSTERING DES PLANÈTES")
    print("=" * 50)

    # Préparation des features pour le clustering
    feature_cols = [
        "masse",
        "rayon",
        "distance",
        "temperature_moyenne",
        "periode_orbitale",
    ]

    # Assemblage des features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    feature_df = assembler.transform(df)

    # Normalisation des features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    scaler_model = scaler.fit(feature_df)
    scaled_df = scaler_model.transform(feature_df)

    # Application du K-Means avec différents nombres de clusters
    best_k = 3
    best_silhouette = -1

    for k in range(2, 6):
        kmeans = KMeans(
            k=k, featuresCol="scaledFeatures", predictionCol="cluster", seed=42
        )
        model = kmeans.fit(scaled_df)
        predictions = model.transform(scaled_df)

        evaluator = ClusteringEvaluator(
            featuresCol="scaledFeatures", predictionCol="cluster"
        )
        silhouette = evaluator.evaluate(predictions)

        print(f"K={k}: Silhouette Score = {silhouette:.3f}")

        if silhouette > best_silhouette:
            best_silhouette = silhouette
            best_k = k

    # Modèle final avec le meilleur K
    print(
        f"\n🏆 Meilleur nombre de clusters: {best_k} (Silhouette: {best_silhouette:.3f})"
    )

    kmeans = KMeans(
        k=best_k, featuresCol="scaledFeatures", predictionCol="cluster", seed=42
    )
    model = kmeans.fit(scaled_df)
    clustered_df = model.transform(scaled_df)

    # Analyse des clusters
    print("\n📊 Analyse des clusters:")
    cluster_stats = (
        clustered_df.groupBy("cluster")
        .agg(
            count("*").alias("nombre_planetes"),
            avg("masse").alias("masse_moyenne"),
            avg("rayon").alias("rayon_moyen"),
            avg("distance").alias("distance_moyenne"),
            avg("temperature_moyenne").alias("temp_moyenne"),
        )
        .orderBy("cluster")
    )

    cluster_stats.show()

    # Affichage des planètes par cluster
    for cluster_id in range(best_k):
        print(f"\n🌌 Cluster {cluster_id}:")
        cluster_planets = clustered_df.filter(col("cluster") == cluster_id).select(
            "nom", "type", "masse", "rayon", "distance", "temperature_moyenne"
        )
        cluster_planets.show()

    return clustered_df, model


def identify_anomalies(df):
    """
    Identifie les planètes avec des caractéristiques atypiques
    """
    print("\n🚨 DÉTECTION D'ANOMALIES")
    print("=" * 50)

    # Calcul des quartiles et IQR pour chaque variable numérique
    numeric_cols = [
        "masse",
        "rayon",
        "distance",
        "temperature_moyenne",
        "periode_orbitale",
    ]

    anomalies = []

    for col_name in numeric_cols:
        # Calcul des quartiles
        quantiles = df.approxQuantile(col_name, [0.25, 0.5, 0.75], 0.05)
        q1, median, q3 = quantiles
        iqr = q3 - q1

        # Seuils d'anomalie
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        print(f"\n📏 {col_name}:")
        print(f"  Q1: {q1:.2f}, Médiane: {median:.2f}, Q3: {q3:.2f}")
        print(f"  Seuils d'anomalie: [{lower_bound:.2f}, {upper_bound:.2f}]")

        # Identification des anomalies
        col_anomalies = df.filter(
            (col(col_name) < lower_bound) | (col(col_name) > upper_bound)
        ).select("nom", col_name, "type")

        anomaly_count = col_anomalies.count()
        if anomaly_count > 0:
            print(f"  🔍 {anomaly_count} anomalie(s) détectée(s):")
            col_anomalies.show()
            anomalies.extend(col_anomalies.collect())

    return anomalies


def save_results_to_hdfs(df, analytics_results, hdfs_namenode):
    """
    Sauvegarde les résultats vers HDFS
    """
    print("\n💾 SAUVEGARDE VERS HDFS")
    print("=" * 50)

    try:
        # Sauvegarde des données enrichies
        enriched_path = f"{hdfs_namenode}/planet_analytics/enriched_data"
        df.write.mode("overwrite").parquet(enriched_path)
        print(f"✅ Données enrichies sauvegardées: {enriched_path}")

        # Sauvegarde des résultats d'analyse
        results_path = f"{hdfs_namenode}/planet_analytics/results"

        # Conversion des résultats en DataFrame et sauvegarde
        spark = SparkSession.getActiveSession()
        results_df = spark.createDataFrame([analytics_results], ["analysis_results"])
        results_df.write.mode("overwrite").json(results_path)
        print(f"✅ Résultats d'analyse sauvegardés: {results_path}")

    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde HDFS: {e}")


def save_to_hive(df, clustered_df):
    """
    Sauvegarde vers Hive
    """
    print("\n🗄️ SAUVEGARDE VERS HIVE")
    print("=" * 50)

    try:
        # Table des données brutes
        df.write.mode("overwrite").saveAsTable("planet_discoveries.raw_data")
        print("✅ Table 'planet_discoveries.raw_data' créée")

        # Table des données avec clusters
        clustered_df.select(
            "id",
            "nom",
            "decouvreur",
            "masse",
            "rayon",
            "distance",
            "temperature_moyenne",
            "periode_orbitale",
            "type",
            "statut",
            "presence_eau",
            "cluster",
        ).write.mode("overwrite").saveAsTable("planet_discoveries.clustered_data")
        print("✅ Table 'planet_discoveries.clustered_data' créée")

    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde Hive: {e}")


def main():
    """
    Fonction principale d'analyse avancée
    """
    print("🔬 ANALYSES AVANCÉES DES DÉCOUVERTES DE PLANÈTES")
    print("=" * 60)

    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
    hdfs_data_path = f"{hdfs_namenode}/planet_discoveries/raw"

    # Création de la session Spark
    spark = create_spark_session()

    try:
        # Chargement des données
        df = load_planet_data(spark, hdfs_data_path)

        print(f"\n📊 Nombre total de planètes: {df.count()}")
        print("\n🔍 Aperçu des données:")
        df.show(5)

        # 1. Statistiques de base
        stats_df = calculate_basic_statistics(df)

        # 2. Analyse des corrélations
        correlation_matrix, numeric_cols = calculate_correlations(df)

        # 3. Analyse spécifique de la présence d'eau
        water_stats = analyze_water_relationships(df)

        # 4. Clustering des planètes
        clustered_df, cluster_model = perform_clustering(df)

        # 5. Détection d'anomalies
        anomalies = identify_anomalies(df)

        # 6. Compilation des résultats
        analytics_results = {
            "total_planets": df.count(),
            "correlation_analysis": "completed",
            "clustering_completed": True,
            "anomalies_detected": len(anomalies),
            "water_bearing_planets": df.filter(col("presence_eau") == "oui").count(),
        }

        # 7. Sauvegarde des résultats
        save_results_to_hdfs(df, analytics_results, hdfs_namenode)
        save_to_hive(df, clustered_df)

        print("\n✅ ANALYSE COMPLÈTE TERMINÉE")
        print(f"📈 Résultats disponibles dans HDFS: {hdfs_namenode}/planet_analytics/")
        print(
            "🗄️ Tables Hive créées: planet_discoveries.raw_data, planet_discoveries.clustered_data"
        )

    except Exception as e:
        print(f"❌ Erreur lors de l'analyse: {e}")
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
