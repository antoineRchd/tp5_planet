from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.stat import Correlation
from pyspark.ml.evaluation import ClusteringEvaluator
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


def load_planet_data(spark, hdfs_path=None, csv_path=None):
    """
    Charge les données de planètes depuis HDFS, CSV ou données de test
    """
    if hdfs_path:
        try:
            df = spark.read.parquet(hdfs_path)
            print(f"✅ Données chargées depuis HDFS: {hdfs_path}")
            return df
        except Exception as e:
            print(f"⚠️ Erreur HDFS: {e}")

    if csv_path:
        try:
            df = (
                spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_path)
            )
            print(f"✅ Données chargées depuis CSV: {csv_path}")
            return df
        except Exception as e:
            print(f"⚠️ Erreur CSV: {e}")

    # Données de test si aucune source n'est disponible
    test_data = [
        ("Kepler-442b", 2, 45, 0.85, 8.2, -15.5, 112.3, 0, 1),
        ("Kepler-452b", 1, 78, 1.2, 10.5, 5.0, 385.0, 1, 1),
        ("HD-40307g", 3, 62, 1.8, 6.8, 15.0, 197.8, 1, 1),
        ("Proxima-Centauri-b", 0, 34, 1.1, 4.2, -39.0, 11.2, 0, 0),
        ("TRAPPIST-1e", 0, 58, 0.92, 7.1, -22.0, 6.1, 1, 1),
        ("Gliese-667Cc", 1, 71, 1.5, 9.3, -3.0, 28.1, 1, 1),
        ("K2-18b", 2, 83, 2.3, 5.9, -23.0, 33.0, 1, 1),
        ("TOI-715b", 0, 49, 1.55, 8.7, 15.0, 19.3, 0, 0),
        ("LP-890-9c", 1, 38, 1.4, 6.2, -25.0, 8.8, 0, 0),
        ("GJ-357d", 0, 67, 1.7, 4.5, -53.0, 55.7, 0, 0),
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
    print("\n🏗️ Distribution par colonisabilité:")
    df.groupBy("Colonisable").count().orderBy(desc("count")).show()

    # Statistiques par nombre de lunes
    print("\n🌙 Distribution par nombre de lunes:")
    df.groupBy("Num_Moons").count().orderBy("Num_Moons").show()

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


def analyze_habitability_factors(df):
    """
    Analyse spécifique des facteurs d'habitabilité
    """
    print("\n🌍 ANALYSE: FACTEURS D'HABITABILITÉ")
    print("=" * 60)

    # Statistiques par colonisabilité
    habitability_stats = df.groupBy("Colonisable").agg(
        avg("Temperature").alias("temperature_moyenne"),
        avg("Gravity").alias("gravite_moyenne"),
        avg("Sunlight_Hours").alias("ensoleillement_moyen"),
        avg("Minerals").alias("mineraux_moyens"),
        avg("Water_Presence").alias("pourcentage_eau"),
        count("*").alias("nombre_planetes"),
    )

    print("📈 Statistiques moyennes par colonisabilité:")
    habitability_stats.show()

    # Zone de température habitable (-50 à 50°C)
    habitable_temp = df.filter((col("Temperature") >= -50) & (col("Temperature") <= 50))

    print(f"\n🌡️ Planètes dans la zone de température habitable (-50°C à 50°C):")
    print(f"Total: {habitable_temp.count()}/{df.count()}")

    habitable_temp.select(
        "Name", "Temperature", "Water_Presence", "Colonisable", "Gravity", "Minerals"
    ).show()

    # Planètes avec eau ET colonisables
    water_and_colonizable = df.filter(
        (col("Water_Presence") == 1) & (col("Colonisable") == 1)
    )

    print(f"\n💧🏗️ Planètes avec eau ET colonisables:")
    print(f"Total: {water_and_colonizable.count()}/{df.count()}")
    water_and_colonizable.show()

    return habitability_stats


def analyze_resource_potential(df):
    """
    Analyse du potentiel en ressources
    """
    print("\n⛏️ ANALYSE: POTENTIEL EN RESSOURCES")
    print("=" * 60)

    # Classification par richesse minérale
    df_with_mineral_class = df.withColumn(
        "mineral_class",
        when(col("Minerals") >= 75, "Très riche")
        .when(col("Minerals") >= 50, "Riche")
        .when(col("Minerals") >= 25, "Modéré")
        .otherwise("Pauvre"),
    )

    print("🏔️ Distribution par richesse minérale:")
    df_with_mineral_class.groupBy("mineral_class").count().orderBy(desc("count")).show()

    # Corrélation richesse minérale vs colonisabilité
    mineral_colonization = df_with_mineral_class.groupBy(
        "mineral_class", "Colonisable"
    ).count()
    print("⛏️🏗️ Richesse minérale vs Colonisabilité:")
    mineral_colonization.show()

    # Top planètes par ressources
    print("\n🏆 Top 10 planètes par richesse minérale:")
    df.select(
        "Name", "Minerals", "Water_Presence", "Colonisable", "Temperature"
    ).orderBy(desc("Minerals")).limit(10).show()

    return df_with_mineral_class


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
            avg("Minerals").alias("mineraux_moyens"),
            avg("Gravity").alias("gravite_moyenne"),
            avg("Temperature").alias("temperature_moyenne"),
            avg("Sunlight_Hours").alias("ensoleillement_moyen"),
            avg("Water_Presence").alias("pourcentage_eau"),
            avg("Colonisable").alias("pourcentage_colonisable"),
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
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
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
        ).select("Name", col_name, "Water_Presence", "Colonisable")

        anomaly_count = col_anomalies.count()
        if anomaly_count > 0:
            print(f"  🔍 {anomaly_count} anomalie(s) détectée(s):")
            col_anomalies.show()
            anomalies.extend(col_anomalies.collect())

    return anomalies


def calculate_colonization_score(df):
    """
    Calcule un score de colonisation basé sur multiple facteurs
    """
    print("\n🚀 CALCUL DU SCORE DE COLONISATION")
    print("=" * 50)

    # Score basé sur plusieurs facteurs
    df_with_score = df.withColumn(
        "colonization_score",
        (
            # Température idéale (entre -20 et 30°C)
            when((col("Temperature") >= -20) & (col("Temperature") <= 30), 25)
            .when((col("Temperature") >= -50) & (col("Temperature") <= 50), 15)
            .otherwise(0)
            +
            # Gravité proche de la Terre (0.8 à 1.2)
            when((col("Gravity") >= 0.8) & (col("Gravity") <= 1.2), 20)
            .when((col("Gravity") >= 0.5) & (col("Gravity") <= 2.0), 10)
            .otherwise(0)
            +
            # Présence d'eau
            when(col("Water_Presence") == 1, 30).otherwise(0)
            +
            # Ressources minérales (normalisé sur 25 points)
            (col("Minerals") * 25 / 100)
        ),
    )

    print("🏆 Top 10 planètes par score de colonisation:")
    df_with_score.select(
        "Name",
        "colonization_score",
        "Temperature",
        "Gravity",
        "Water_Presence",
        "Minerals",
        "Colonisable",
    ).orderBy(desc("colonization_score")).limit(10).show()

    # Comparaison avec la classification actuelle
    print("\n📊 Score moyen par classification actuelle:")
    df_with_score.groupBy("Colonisable").agg(
        avg("colonization_score").alias("score_moyen")
    ).show()

    return df_with_score


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
    csv_path = "/app/planets_dataset.csv"

    # Création de la session Spark
    spark = create_spark_session()

    try:
        # Chargement des données
        df = load_planet_data(spark, hdfs_data_path, csv_path)

        print(f"\n📊 Nombre total de planètes: {df.count()}")
        print("\n🔍 Aperçu des données:")
        df.show(5)

        # 1. Statistiques de base
        stats_df = calculate_basic_statistics(df)

        # 2. Analyse des corrélations
        correlation_matrix, numeric_cols = calculate_correlations(df)

        # 3. Analyse des facteurs d'habitabilité
        habitability_stats = analyze_habitability_factors(df)

        # 4. Analyse du potentiel en ressources
        resource_df = analyze_resource_potential(df)

        # 5. Clustering des planètes
        clustered_df, cluster_model = perform_clustering(df)

        # 6. Détection d'anomalies
        anomalies = identify_anomalies(df)

        # 7. Calcul du score de colonisation
        scored_df = calculate_colonization_score(df)

        # 8. Compilation des résultats
        analytics_results = {
            "total_planets": df.count(),
            "correlation_analysis": "completed",
            "clustering_completed": True,
            "anomalies_detected": len(anomalies),
            "water_bearing_planets": df.filter(col("Water_Presence") == 1).count(),
            "colonizable_planets": df.filter(col("Colonisable") == 1).count(),
        }

        # 9. Sauvegarde des résultats
        save_results_to_hdfs(scored_df, analytics_results, hdfs_namenode)
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
