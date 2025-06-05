from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    StringIndexer,
    OneHotEncoder,
)
from pyspark.ml.classification import (
    RandomForestClassifier,
    GBTClassifier,
    LogisticRegression,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
import os
import numpy as np


def create_spark_session():
    """
    Crée une session Spark pour le machine learning
    """
    spark = (
        SparkSession.builder.appName("PlanetHabitabilityPredictor")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.warehouse.dir", "/spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_enhanced_dataset(spark):
    """
    Crée un dataset enrichi avec plus d'exemples pour l'entraînement
    """
    # Dataset étendu avec des planètes connues et leurs caractéristiques d'habitabilité
    enhanced_data = [
        # Planètes habitables confirmées/potentielles
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
            1,
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
            1,
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
            1,
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
            1,
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
            1,
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
            1,
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
            1,
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
            1,
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
            1,
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
            1,
        ),
        # Planètes potentiellement habitables
        (
            "planet-11",
            "Kepler-1649c",
            "Equipe Kepler",
            "2020-04-15",
            1.06,
            1.06,
            300.0,
            "terrestre",
            "confirmée",
            "mince",
            -39.0,
            19.5,
            0,
            "inconnue",
            1,
        ),
        (
            "planet-12",
            "TOI-175b",
            "NASA TESS",
            "2023-01-11",
            4.25,
            1.73,
            120.0,
            "super-terre",
            "confirmée",
            "dense",
            8.0,
            25.6,
            0,
            "inconnue",
            1,
        ),
        (
            "planet-13",
            "LHS 1140b",
            "MEarth Project",
            "2017-04-19",
            6.6,
            1.4,
            40.0,
            "super-terre",
            "confirmée",
            "mince",
            -53.0,
            24.7,
            0,
            "inconnue",
            1,
        ),
        # Planètes non habitables (trop chaudes)
        (
            "planet-14",
            "HD 149026b",
            "NASA",
            "2005-07-01",
            114.0,
            0.725,
            256.0,
            "géante gazeuse",
            "confirmée",
            "hydrogène",
            2000.0,
            2.9,
            0,
            "non",
            0,
        ),
        (
            "planet-15",
            "WASP-12b",
            "SuperWASP",
            "2008-04-01",
            445.0,
            1.79,
            871.0,
            "géante gazeuse",
            "confirmée",
            "hydrogène",
            2516.0,
            1.1,
            0,
            "non",
            0,
        ),
        (
            "planet-16",
            "Kepler-7b",
            "Equipe Kepler",
            "2010-01-04",
            150.0,
            1.48,
            3000.0,
            "géante gazeuse",
            "confirmée",
            "hydrogène",
            1540.0,
            4.9,
            0,
            "non",
            0,
        ),
        (
            "planet-17",
            "CoRoT-7b",
            "CoRoT",
            "2009-02-03",
            4.8,
            1.58,
            489.0,
            "super-terre",
            "confirmée",
            "aucune",
            1800.0,
            0.85,
            0,
            "non",
            0,
        ),
        # Planètes non habitables (trop froides)
        (
            "planet-18",
            "OGLE-2005-BLG-390Lb",
            "OGLE",
            "2006-01-25",
            5.5,
            1.5,
            21500.0,
            "super-terre",
            "confirmée",
            "glacée",
            -223.0,
            3500.0,
            0,
            "glacée",
            0,
        ),
        (
            "planet-19",
            "Kepler-1708b",
            "Equipe Kepler",
            "2022-01-13",
            4.6,
            2.6,
            5500.0,
            "géante gazeuse",
            "confirmée",
            "hydrogène",
            -183.0,
            737.0,
            1,
            "non",
            0,
        ),
        (
            "planet-20",
            "PSR B1257+12 A",
            "Aleksander Wolszczan",
            "1992-01-09",
            0.02,
            0.5,
            2300.0,
            "terrestre",
            "confirmée",
            "aucune",
            -213.0,
            25.3,
            0,
            "non",
            0,
        ),
        # Planètes dans des systèmes binaires (compliqué pour l'habitabilité)
        (
            "planet-21",
            "Kepler-16b",
            "Equipe Kepler",
            "2011-09-15",
            105.0,
            0.75,
            245.0,
            "géante gazeuse",
            "confirmée",
            "hydrogène",
            -101.0,
            229.0,
            0,
            "non",
            0,
        ),
        (
            "planet-22",
            "Alpha Centauri Bb",
            "ESO",
            "2012-10-17",
            1.13,
            1.04,
            4.37,
            "terrestre",
            "non confirmée",
            "inconnue",
            1200.0,
            3.2,
            0,
            "non",
            0,
        ),
        # Naines brunes et objets exotiques
        (
            "planet-23",
            "2M1207b",
            "ESO",
            "2004-05-01",
            25.0,
            1.5,
            230.0,
            "naine",
            "confirmée",
            "méthane",
            -173.0,
            2900.0,
            0,
            "non",
            0,
        ),
        (
            "planet-24",
            "PSR B1620-26 b",
            "Steinn Sigurdsson",
            "2003-07-10",
            2.5,
            2.3,
            12400.0,
            "géante gazeuse",
            "confirmée",
            "hydrogène",
            -220.0,
            36500.0,
            0,
            "non",
            0,
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
            StructField(
                "habitable", IntegerType(), True
            ),  # 1 = habitable, 0 = non habitable
        ]
    )

    df = spark.createDataFrame(enhanced_data, schema)
    return df


def engineer_features(df):
    """
    Ingénierie des features pour améliorer la prédiction
    """
    # Calcul de nouvelles features
    enhanced_df = (
        df.withColumn(
            "zone_habitable",
            when(
                (col("temperature_moyenne") >= -50)
                & (col("temperature_moyenne") <= 50),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "taille_terrestre",
            when((col("rayon") >= 0.5) & (col("rayon") <= 2.0), 1).otherwise(0),
        )
        .withColumn(
            "masse_terrestre",
            when((col("masse") >= 0.1) & (col("masse") <= 10.0), 1).otherwise(0),
        )
        .withColumn(
            "distance_log",
            log10(col("distance") + 1),  # Log de la distance pour réduire l'asymétrie
        )
        .withColumn("periode_log", log10(col("periode_orbitale") + 1))
        .withColumn(
            "densite_approx",
            col("masse") / pow(col("rayon"), 3),  # Densité approximative
        )
        .withColumn("eau_binaire", when(col("presence_eau") == "oui", 1).otherwise(0))
    )

    return enhanced_df


def prepare_ml_pipeline(df):
    """
    Prépare le pipeline de machine learning
    """
    # Encodage des variables catégorielles
    type_indexer = StringIndexer(inputCol="type", outputCol="type_indexed")
    atmosphere_indexer = StringIndexer(
        inputCol="atmosphere", outputCol="atmosphere_indexed"
    )
    statut_indexer = StringIndexer(inputCol="statut", outputCol="statut_indexed")

    # Features numériques
    numeric_features = [
        "masse",
        "rayon",
        "distance_log",
        "temperature_moyenne",
        "periode_log",
        "nombre_satellites",
        "densite_approx",
        "zone_habitable",
        "taille_terrestre",
        "masse_terrestre",
        "eau_binaire",
    ]

    # Features catégorielles encodées
    categorical_features = ["type_indexed", "atmosphere_indexed", "statut_indexed"]

    # Assemblage de toutes les features
    all_features = numeric_features + categorical_features
    assembler = VectorAssembler(inputCols=all_features, outputCol="features")

    # Normalisation
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

    return type_indexer, atmosphere_indexer, statut_indexer, assembler, scaler


def train_models(df):
    """
    Entraîne plusieurs modèles de classification
    """
    print("\n🤖 ENTRAÎNEMENT DES MODÈLES D'IA")
    print("=" * 50)

    # Ingénierie des features
    df_features = engineer_features(df)

    # Affichage des statistiques de classe
    print("📊 Distribution des classes:")
    df_features.groupBy("habitable").count().show()

    # Préparation du pipeline
    type_indexer, atmosphere_indexer, statut_indexer, assembler, scaler = (
        prepare_ml_pipeline(df_features)
    )

    # Division train/test
    train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

    print(f"🎯 Données d'entraînement: {train_df.count()}")
    print(f"🧪 Données de test: {test_df.count()}")

    # Modèles à tester
    models = {
        "RandomForest": RandomForestClassifier(
            featuresCol="scaledFeatures", labelCol="habitable", numTrees=50, seed=42
        ),
        "GradientBoosting": GBTClassifier(
            featuresCol="scaledFeatures", labelCol="habitable", maxIter=20, seed=42
        ),
        "LogisticRegression": LogisticRegression(
            featuresCol="scaledFeatures", labelCol="habitable", maxIter=100
        ),
    }

    best_model = None
    best_score = 0
    best_name = ""
    results = {}

    # Évaluateurs
    binary_evaluator = BinaryClassificationEvaluator(
        rawPredictionCol="rawPrediction",
        labelCol="habitable",
        metricName="areaUnderROC",
    )

    multi_evaluator = MulticlassClassificationEvaluator(
        predictionCol="prediction", labelCol="habitable", metricName="accuracy"
    )

    for model_name, model in models.items():
        print(f"\n🔧 Entraînement du modèle: {model_name}")

        # Pipeline complet
        pipeline = Pipeline(
            stages=[
                type_indexer,
                atmosphere_indexer,
                statut_indexer,
                assembler,
                scaler,
                model,
            ]
        )

        # Entraînement
        model_fitted = pipeline.fit(train_df)

        # Prédictions sur le test
        predictions = model_fitted.transform(test_df)

        # Évaluation
        auc_score = binary_evaluator.evaluate(predictions)
        accuracy_score = multi_evaluator.evaluate(predictions)

        results[model_name] = {
            "model": model_fitted,
            "auc": auc_score,
            "accuracy": accuracy_score,
        }

        print(f"  📈 AUC: {auc_score:.3f}")
        print(f"  🎯 Accuracy: {accuracy_score:.3f}")

        if auc_score > best_score:
            best_score = auc_score
            best_model = model_fitted
            best_name = model_name

    print(f"\n🏆 Meilleur modèle: {best_name} (AUC: {best_score:.3f})")

    return best_model, results, test_df


def analyze_feature_importance(model, feature_names):
    """
    Analyse l'importance des features
    """
    print("\n📊 IMPORTANCE DES FEATURES")
    print("=" * 50)

    try:
        # Extraction du modèle final (RandomForest ou GBT)
        stages = model.stages
        final_model = stages[-1]

        if hasattr(final_model, "featureImportances"):
            importances = final_model.featureImportances.toArray()

            # Création d'un DataFrame avec les importances
            feature_importance = list(zip(feature_names, importances))
            feature_importance.sort(key=lambda x: x[1], reverse=True)

            print("🔍 Top 10 des features les plus importantes:")
            for i, (feature, importance) in enumerate(feature_importance[:10], 1):
                print(f"  {i}. {feature}: {importance:.3f}")

            return feature_importance
        else:
            print("⚠️ Modèle ne supporte pas l'analyse d'importance des features")
            return None

    except Exception as e:
        print(f"❌ Erreur lors de l'analyse des features: {e}")
        return None


def predict_habitability(model, new_planets_data, spark):
    """
    Prédit l'habitabilité de nouvelles planètes
    """
    print("\n🔮 PRÉDICTION D'HABITABILITÉ")
    print("=" * 50)

    # Création du DataFrame pour les nouvelles planètes
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("nom", StringType(), True),
            StructField("masse", DoubleType(), True),
            StructField("rayon", DoubleType(), True),
            StructField("distance", DoubleType(), True),
            StructField("type", StringType(), True),
            StructField("atmosphere", StringType(), True),
            StructField("statut", StringType(), True),
            StructField("temperature_moyenne", DoubleType(), True),
            StructField("periode_orbitale", DoubleType(), True),
            StructField("nombre_satellites", IntegerType(), True),
            StructField("presence_eau", StringType(), True),
        ]
    )

    new_df = spark.createDataFrame(new_planets_data, schema)

    # Application de l'ingénierie des features
    new_df_features = engineer_features(new_df)

    # Prédictions
    predictions = model.transform(new_df_features)

    # Affichage des résultats
    print("🌍 Prédictions d'habitabilité:")
    predictions.select(
        "nom", "masse", "rayon", "temperature_moyenne", "prediction", "probability"
    ).show(truncate=False)

    return predictions


def save_model_and_results(model, results, hdfs_namenode):
    """
    Sauvegarde le modèle et les résultats
    """
    print("\n💾 SAUVEGARDE DU MODÈLE")
    print("=" * 50)

    try:
        # Sauvegarde du modèle
        model_path = f"{hdfs_namenode}/planet_ml_models/habitability_model"
        model.write().overwrite().save(model_path)
        print(f"✅ Modèle sauvegardé: {model_path}")

        # Sauvegarde des métriques (dans un fichier local pour cet exemple)
        with open("/tmp/model_results.json", "w") as f:
            import json

            metrics = {
                name: {"auc": result["auc"], "accuracy": result["accuracy"]}
                for name, result in results.items()
            }
            json.dump(metrics, f, indent=2)
        print("✅ Métriques sauvegardées: /tmp/model_results.json")

    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde: {e}")


def main():
    """
    Fonction principale du modèle d'habitabilité
    """
    print("🤖 MODÈLE D'IA: PRÉDICTION D'HABITABILITÉ DES PLANÈTES")
    print("=" * 60)

    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")

    # Création de la session Spark
    spark = create_spark_session()

    try:
        # 1. Création du dataset d'entraînement
        df = create_enhanced_dataset(spark)
        print(f"\n📊 Dataset créé avec {df.count()} planètes")

        # 2. Entraînement des modèles
        best_model, results, test_df = train_models(df)

        # 3. Analyse de l'importance des features
        feature_names = [
            "masse",
            "rayon",
            "distance_log",
            "temperature_moyenne",
            "periode_log",
            "nombre_satellites",
            "densite_approx",
            "zone_habitable",
            "taille_terrestre",
            "masse_terrestre",
            "eau_binaire",
            "type_indexed",
            "atmosphere_indexed",
            "statut_indexed",
        ]
        feature_importance = analyze_feature_importance(best_model, feature_names)

        # 4. Exemples de prédiction sur de nouvelles planètes
        new_planets = [
            (
                "new-1",
                "Planète X1",
                1.5,
                1.2,
                50.0,
                "terrestre",
                "mince",
                "confirmée",
                10.0,
                30.0,
                1,
                "inconnue",
            ),
            (
                "new-2",
                "Planète X2",
                0.8,
                0.9,
                25.0,
                "terrestre",
                "mince",
                "confirmée",
                -10.0,
                45.0,
                0,
                "oui",
            ),
            (
                "new-3",
                "Planète X3",
                10.0,
                3.0,
                200.0,
                "géante gazeuse",
                "hydrogène",
                "confirmée",
                500.0,
                100.0,
                5,
                "non",
            ),
            (
                "new-4",
                "Planète X4",
                2.1,
                1.4,
                75.0,
                "super-terre",
                "dense",
                "confirmée",
                25.0,
                60.0,
                2,
                "inconnue",
            ),
        ]

        predictions = predict_habitability(best_model, new_planets, spark)

        # 5. Sauvegarde
        save_model_and_results(best_model, results, hdfs_namenode)

        print("\n✅ MODÈLE D'HABITABILITÉ COMPLÉTÉ")
        print(
            "🎯 Le modèle peut maintenant prédire l'habitabilité de nouvelles planètes!"
        )

    except Exception as e:
        print(f"❌ Erreur lors de l'entraînement du modèle: {e}")
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
