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
    Utilise la nouvelle structure avec Name, Num_Moons, Minerals, etc.
    """
    if hdfs_path:
        try:
            df = spark.read.parquet(hdfs_path)
            print(f"✅ Données chargées depuis HDFS: {hdfs_path}")
            return df
        except Exception as e:
            print(f"⚠️ Erreur HDFS: {e}, utilisation des données de test")

    # Données de test avec la nouvelle structure
    test_data = [
        (
            "Planet_18329",
            5,
            59,
            1.981602859469247,
            5.8168191458771705,
            28.381006239674264,
            56.76091939405808,
            0,
            0,
        ),
        (
            "Planet_28900",
            8,
            672,
            1.3881504830806715,
            14.715293728903166,
            27.48564614824687,
            51.0340563211323,
            0,
            0,
        ),
        (
            "Planet_56161",
            3,
            764,
            2.5308267251520093,
            22.902523479273974,
            63.39082702246432,
            42.99324764351807,
            1,
            0,
        ),
        (
            "Kepler-442b",
            0,
            45,
            0.89,
            12.5,
            -40.0,
            112.3,
            0,
            1,
        ),
        (
            "Kepler-452b",
            1,
            78,
            1.2,
            16.8,
            5.0,
            385.0,
            0,
            1,
        ),
        (
            "HD_40307g",
            2,
            89,
            1.8,
            18.2,
            15.0,
            197.8,
            1,
            1,
        ),
        (
            "Proxima_Centauri_b",
            0,
            23,
            1.1,
            11.0,
            -39.0,
            11.2,
            0,
            1,
        ),
        (
            "TRAPPIST-1e",
            0,
            34,
            0.92,
            8.5,
            -22.0,
            6.1,
            1,
            1,
        ),
        (
            "Gliese_667Cc",
            0,
            56,
            1.5,
            13.5,
            -3.0,
            28.1,
            1,
            1,
        ),
        (
            "K2-18b",
            0,
            67,
            2.3,
            20.1,
            -23.0,
            33.0,
            1,
            1,
        ),
        (
            "TOI-715b",
            0,
            45,
            1.55,
            15.2,
            15.0,
            19.3,
            0,
            1,
        ),
        (
            "LP_890-9c",
            0,
            38,
            1.4,
            12.8,
            -25.0,
            8.8,
            0,
            1,
        ),
        (
            "GJ_357d",
            0,
            52,
            1.7,
            17.3,
            -53.0,
            55.7,
            0,
            1,
        ),
        (
            "Hot_Jupiter_1",
            15,
            12,
            0.8,
            24.0,
            1200.0,
            3.2,
            0,
            0,
        ),
        (
            "Ice_Giant_1",
            25,
            234,
            3.8,
            2.1,
            -180.0,
            4500.0,
            1,
            0,
        ),
        (
            "Desert_Planet_1",
            2,
            890,
            2.1,
            18.5,
            85.0,
            45.6,
            0,
            0,
        ),
        (
            "Gas_Giant_1",
            42,
            156,
            0.6,
            22.3,
            -120.0,
            2890.0,
            0,
            0,
        ),
        (
            "Rocky_Planet_1",
            1,
            423,
            1.9,
            14.2,
            120.0,
            89.4,
            0,
            0,
        ),
        (
            "Ocean_World_1",
            3,
            67,
            1.3,
            16.8,
            8.0,
            78.2,
            1,
            1,
        ),
        (
            "Volcanic_Planet_1",
            0,
            789,
            2.8,
            19.6,
            450.0,
            156.7,
            0,
            0,
        ),
    ]

    schema = StructType(
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
        ]
    )

    df = spark.createDataFrame(test_data, schema)
    print("✅ Données de test créées avec la nouvelle structure")
    return df


def calculate_basic_statistics(df):
    """
    Calcule les statistiques de base
    """
    print("\n📊 STATISTIQUES DE BASE")
    print("=" * 50)

    # Statistiques numériques
    numeric_cols = [
        "Num_Moons",
        "Minerals",
        "Gravity",
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
    ]
    stats_df = df.select(numeric_cols).describe()
    stats_df.show()

    # Distribution par présence d'eau
    print("\n💧 Distribution par présence d'eau:")
    df.groupBy("Water_Presence").count().orderBy(desc("count")).show()

    # Distribution par colonisabilité
    print("\n🏠 Distribution par colonisabilité:")
    df.groupBy("Colonisable").count().orderBy(desc("count")).show()

    # Analyse des zones de température
    print("\n🌡️ Zones de température:")
    temp_zones = df.withColumn(
        "Temperature_Zone",
        when(col("Temperature") < -50, "Très froid")
        .when((col("Temperature") >= -50) & (col("Temperature") < 0), "Froid")
        .when((col("Temperature") >= 0) & (col("Temperature") < 50), "Tempéré")
        .when((col("Temperature") >= 50) & (col("Temperature") < 100), "Chaud")
        .otherwise("Très chaud"),
    )
    temp_zones.groupBy("Temperature_Zone").count().orderBy(desc("count")).show()

    return stats_df


def calculate_correlations(df):
    """
    Calcule les corrélations entre les variables numériques
    """
    print("\n🔗 ANALYSE DES CORRÉLATIONS")
    print("=" * 50)

    # Préparation des données numériques
    numeric_cols = [
        "Num_Moons",
        "Minerals",
        "Gravity",
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
        "Water_Presence",
        "Colonisable",
    ]

    # Création d'un vecteur de features
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    vector_df = assembler.transform(df).select("features")

    # Calcul de la matrice de corrélation
    correlation_matrix = Correlation.corr(vector_df, "features").head()
    correlation_array = correlation_matrix[0].toArray()

    # Affichage des corrélations importantes
    print("\n🔍 Corrélations significatives (|r| > 0.3):")
    for i, col1 in enumerate(numeric_cols):
        for j, col2 in enumerate(numeric_cols):
            if i < j:  # Éviter les doublons
                corr_value = correlation_array[i][j]
                if abs(corr_value) > 0.3:
                    print(f"  {col1} ↔ {col2}: {corr_value:.3f}")

    return correlation_array, numeric_cols


def analyze_water_relationships(df):
    """
    Analyse spécifique des relations avec la présence d'eau
    """
    print("\n💧 ANALYSE: PRÉSENCE D'EAU vs AUTRES CARACTÉRISTIQUES")
    print("=" * 60)

    # Statistiques par présence d'eau
    water_stats = df.groupBy("Water_Presence").agg(
        avg("Temperature").alias("temperature_moyenne"),
        avg("Gravity").alias("gravite_moyenne"),
        avg("Minerals").alias("mineraux_moyenne"),
        avg("Sunlight_Hours").alias("soleil_moyen"),
        avg("Num_Moons").alias("lunes_moyenne"),
        count("*").alias("nombre_planetes"),
    )

    print("📈 Statistiques moyennes par présence d'eau:")
    water_stats.show()

    # Relation eau-température
    print("\n🌡️ Relation Eau-Température:")
    water_temp_relation = df.groupBy("Water_Presence").agg(
        avg("Temperature").alias("temp_moyenne"),
        min("Temperature").alias("temp_min"),
        max("Temperature").alias("temp_max"),
        stddev("Temperature").alias("temp_stddev"),
    )
    water_temp_relation.show()

    # Zone habitable (température entre -50 et 50°C)
    habitable_zone = df.filter((col("Temperature") >= -50) & (col("Temperature") <= 50))

    print(f"\n🌡️ Planètes dans la zone de température habitable (-50°C à 50°C):")
    print(f"Total: {habitable_zone.count()}/{df.count()}")

    habitable_zone.select(
        "Name", "Temperature", "Water_Presence", "Gravity", "Minerals"
    ).show()

    # Analyse de la relation eau-minéraux
    print("\n⛏️ Relation Eau-Minéraux:")
    water_mineral_relation = df.groupBy("Water_Presence").agg(
        avg("Minerals").alias("mineraux_moyenne"),
        min("Minerals").alias("mineraux_min"),
        max("Minerals").alias("mineraux_max"),
    )
    water_mineral_relation.show()

    return water_stats


def perform_clustering(df):
    """
    Effectue un clustering des planètes
    """
    print("\n🎯 CLUSTERING DES PLANÈTES")
    print("=" * 50)

    # Préparation des features pour le clustering
    feature_cols = [
        "Minerals",
        "Gravity",
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
        "Num_Moons",
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
            avg("Minerals").alias("mineraux_moyenne"),
            avg("Gravity").alias("gravite_moyenne"),
            avg("Temperature").alias("temperature_moyenne"),
            avg("Sunlight_Hours").alias("soleil_moyen"),
            avg("Water_Presence").alias("eau_moyenne"),
            avg("Colonisable").alias("colonisable_moyenne"),
        )
        .orderBy("cluster")
    )

    cluster_stats.show()

    # Affichage des planètes par cluster
    for cluster_id in range(best_k):
        print(f"\n🌌 Cluster {cluster_id}:")
        cluster_planets = clustered_df.filter(col("cluster") == cluster_id).select(
            "Name",
            "Minerals",
            "Gravity",
            "Temperature",
            "Water_Presence",
            "Colonisable",
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
        "Minerals",
        "Gravity",
        "Temperature",
        "Sunlight_Hours",
        "Rotation_Time",
    ]

    anomalies = []

    for col_name in numeric_cols:
        # Calcul des quartiles
        quantiles = df.approxQuantile(col_name, [0.25, 0.5, 0.75], 0.05)
        if len(quantiles) == 3:
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
            ).select("Name", col_name, "Water_Presence", "Colonisable")

            anomaly_count = col_anomalies.count()
            if anomaly_count > 0:
                print(f"  🔍 {anomaly_count} anomalie(s) détectée(s):")
                col_anomalies.show()
                anomalies.extend(col_anomalies.collect())

    return anomalies


def analyze_habitability_factors(df):
    """
    Analyse les facteurs d'habitabilité
    """
    print("\n🏠 ANALYSE DES FACTEURS D'HABITABILITÉ")
    print("=" * 50)

    # Analyse des planètes colonisables
    colonizable_planets = df.filter(col("Colonisable") == 1)
    non_colonizable_planets = df.filter(col("Colonisable") == 0)

    print(f"🌍 Planètes colonisables: {colonizable_planets.count()}")
    print(f"🚫 Planètes non colonisables: {non_colonizable_planets.count()}")

    # Caractéristiques moyennes des planètes colonisables
    print("\n📊 Caractéristiques moyennes des planètes colonisables:")
    colonizable_stats = colonizable_planets.agg(
        avg("Temperature").alias("temp_moyenne"),
        avg("Gravity").alias("gravite_moyenne"),
        avg("Minerals").alias("mineraux_moyenne"),
        avg("Sunlight_Hours").alias("soleil_moyen"),
        avg("Water_Presence").alias("eau_moyenne"),
        avg("Num_Moons").alias("lunes_moyenne"),
    )
    colonizable_stats.show()

    # Caractéristiques moyennes des planètes non colonisables
    print("\n📊 Caractéristiques moyennes des planètes NON colonisables:")
    non_colonizable_stats = non_colonizable_planets.agg(
        avg("Temperature").alias("temp_moyenne"),
        avg("Gravity").alias("gravite_moyenne"),
        avg("Minerals").alias("mineraux_moyenne"),
        avg("Sunlight_Hours").alias("soleil_moyen"),
        avg("Water_Presence").alias("eau_moyenne"),
        avg("Num_Moons").alias("lunes_moyenne"),
    )
    non_colonizable_stats.show()

    # Analyse croisée eau-colonisabilité
    print("\n💧🏠 Relation Eau-Colonisabilité:")
    water_colonizable_crosstab = df.crosstab("Water_Presence", "Colonisable")
    water_colonizable_crosstab.show()

    return colonizable_stats, non_colonizable_stats


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
        results_data = [
            (
                analytics_results["total_planets"],
                analytics_results["water_bearing_planets"],
                analytics_results["colonizable_planets"],
                analytics_results["correlation_analysis"],
                analytics_results["clustering_completed"],
                analytics_results["anomalies_detected"],
            )
        ]

        results_schema = StructType(
            [
                StructField("total_planets", IntegerType(), True),
                StructField("water_bearing_planets", IntegerType(), True),
                StructField("colonizable_planets", IntegerType(), True),
                StructField("correlation_analysis", StringType(), True),
                StructField("clustering_completed", BooleanType(), True),
                StructField("anomalies_detected", IntegerType(), True),
            ]
        )

        results_df = spark.createDataFrame(results_data, results_schema)
        results_df.write.mode("overwrite").parquet(results_path)
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
            "Name",
            "Num_Moons",
            "Minerals",
            "Gravity",
            "Sunlight_Hours",
            "Temperature",
            "Rotation_Time",
            "Water_Presence",
            "Colonisable",
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

        # 4. Analyse des facteurs d'habitabilité
        colonizable_stats, non_colonizable_stats = analyze_habitability_factors(df)

        # 5. Clustering des planètes
        clustered_df, cluster_model = perform_clustering(df)

        # 6. Détection d'anomalies
        anomalies = identify_anomalies(df)

        # 7. Compilation des résultats
        analytics_results = {
            "total_planets": df.count(),
            "correlation_analysis": "completed",
            "clustering_completed": True,
            "anomalies_detected": len(anomalies),
            "water_bearing_planets": df.filter(col("Water_Presence") == 1).count(),
            "colonizable_planets": df.filter(col("Colonisable") == 1).count(),
        }

        # 8. Sauvegarde des résultats
        save_results_to_hdfs(df, analytics_results, hdfs_namenode)
        save_to_hive(df, clustered_df)

        print("\n✅ ANALYSE COMPLÈTE TERMINÉE")
        print(f"📈 Résultats disponibles dans HDFS: {hdfs_namenode}/planet_analytics/")
        print(
            "🗄️ Tables Hive créées: planet_discoveries.raw_data, planet_discoveries.clustered_data"
        )

        # Résumé des découvertes
        print(f"\n🌍 RÉSUMÉ DES DÉCOUVERTES:")
        print(f"  • Total de planètes analysées: {analytics_results['total_planets']}")
        print(f"  • Planètes avec eau: {analytics_results['water_bearing_planets']}")
        print(f"  • Planètes colonisables: {analytics_results['colonizable_planets']}")
        print(f"  • Anomalies détectées: {analytics_results['anomalies_detected']}")

    except Exception as e:
        print(f"❌ Erreur lors de l'analyse: {e}")
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
