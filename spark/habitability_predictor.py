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


def load_planet_data(spark, csv_path=None):
    """
    Charge les données de planètes depuis CSV ou utilise des données de test
    """
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

    # Dataset de test étendu avec des planètes connues et leurs caractéristiques
    enhanced_data = [
        # Planètes habitables/colonisables
        ("Kepler-442b", 2, 65, 0.85, 8.2, -15.5, 112.3, 0, 1),
        ("Kepler-452b", 1, 78, 1.2, 10.5, 5.0, 385.0, 1, 1),
        ("HD-40307g", 3, 82, 1.8, 6.8, 15.0, 197.8, 1, 1),
        ("TRAPPIST-1e", 0, 58, 0.92, 7.1, -22.0, 6.1, 1, 1),
        ("Gliese-667Cc", 1, 71, 1.5, 9.3, -3.0, 28.1, 1, 1),
        ("K2-18b", 2, 83, 2.3, 5.9, -23.0, 33.0, 1, 1),
        ("Kepler-186f", 0, 56, 1.1, 8.7, -47.0, 129.9, 1, 1),
        ("Kepler-62e", 1, 74, 1.6, 7.4, -15.0, 122.4, 1, 1),
        ("Kepler-62f", 0, 69, 1.4, 6.9, -65.0, 267.3, 1, 1),
        ("Kepler-438b", 2, 52, 1.12, 8.8, 2.0, 35.2, 0, 1),
        # Planètes non colonisables
        ("Proxima-Centauri-b", 0, 34, 1.1, 4.2, -39.0, 11.2, 0, 0),
        ("TOI-715b", 0, 49, 1.55, 8.7, 15.0, 19.3, 0, 0),
        ("LP-890-9c", 1, 38, 1.4, 6.2, -25.0, 8.8, 0, 0),
        ("GJ-357d", 0, 67, 1.7, 4.5, -53.0, 55.7, 0, 0),
        ("Venus-like-1", 0, 23, 0.9, 12.1, 462.0, 243.0, 0, 0),
        ("Mercury-like-1", 0, 45, 0.38, 14.2, 167.0, 58.6, 0, 0),
        ("Mars-like-1", 2, 31, 0.38, 9.8, -80.0, 24.6, 0, 0),
        ("Jupiter-like-1", 79, 12, 2.36, 2.1, -108.0, 9.9, 0, 0),
        ("Saturn-like-1", 146, 8, 0.916, 1.8, -139.0, 10.8, 0, 0),
        ("Neptune-like-1", 16, 5, 1.14, 0.6, -201.0, 16.1, 0, 0),
        # Cas limites intéressants
        ("Hot-Jupiter-1", 12, 15, 3.2, 16.5, 1200.0, 2.1, 0, 0),
        ("Cold-Giant-1", 23, 9, 4.1, 0.3, -230.0, 45.2, 0, 0),
        ("Desert-World-1", 0, 89, 0.7, 13.8, 78.0, 28.7, 0, 0),
        ("Ocean-World-1", 1, 42, 1.05, 9.1, 12.0, 31.4, 1, 1),
        ("Volcanic-World-1", 3, 91, 1.8, 8.2, 85.0, 18.6, 0, 0),
        ("Frozen-World-1", 0, 67, 0.9, 5.4, -156.0, 67.8, 1, 0),
        ("Tidally-Locked-1", 0, 78, 1.2, 11.2, 25.0, 365.0, 1, 1),
        ("High-Gravity-1", 4, 88, 3.8, 7.9, 18.0, 14.2, 1, 0),
        ("Low-Gravity-1", 0, 55, 0.3, 8.9, 8.0, 19.7, 1, 0),
        ("Rich-Minerals-1", 2, 95, 1.1, 8.5, 22.0, 26.3, 1, 1),
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

    df = spark.createDataFrame(enhanced_data, schema)
    print("✅ Dataset de test étendu créé avec 30 planètes")
    return df


def engineer_features(df):
    """
    Ingénierie des features pour améliorer la prédiction
    """
    print("\n🔧 INGÉNIERIE DES FEATURES")
    print("=" * 50)

    # Features dérivées
    enhanced_df = (
        df.withColumn(
            # Zone de température habitable
            "temp_habitable",
            when((col("Temperature") >= -50) & (col("Temperature") <= 50), 1).otherwise(
                0
            ),
        )
        .withColumn(
            # Gravité proche de la Terre
            "gravity_earth_like",
            when((col("Gravity") >= 0.8) & (col("Gravity") <= 1.2), 1).otherwise(0),
        )
        .withColumn(
            # Ensoleillement optimal
            "optimal_sunlight",
            when(
                (col("Sunlight_Hours") >= 8) & (col("Sunlight_Hours") <= 12), 1
            ).otherwise(0),
        )
        .withColumn(
            # Rotation synchrone (potentiellement problématique)
            "tidally_locked",
            when(col("Rotation_Time") > 300, 1).otherwise(0),
        )
        .withColumn(
            # Richesse minérale
            "mineral_rich",
            when(col("Minerals") >= 70, 1).otherwise(0),
        )
        .withColumn(
            # Score composite d'habitabilité
            "habitability_score",
            (
                col("temp_habitable") * 25
                + col("gravity_earth_like") * 20
                + col("Water_Presence") * 30
                + col("optimal_sunlight") * 15
                + (col("Minerals") / 100.0) * 10
            ),
        )
        .withColumn(
            # Interaction température-gravité
            "temp_gravity_interaction",
            col("Temperature") * col("Gravity"),
        )
        .withColumn(
            # Ratio ensoleillement/rotation
            "sunlight_rotation_ratio",
            col("Sunlight_Hours")
            / (col("Rotation_Time") + 1),  # +1 pour éviter division par 0
        )
    )

    print("Features engineered ajoutées:")
    print("- Zone de température habitable")
    print("- Gravité proche de la Terre")
    print("- Ensoleillement optimal")
    print("- Verrouillage de marée")
    print("- Richesse minérale")
    print("- Score d'habitabilité composite")
    print("- Interactions entre variables")

    return enhanced_df


def prepare_ml_pipeline(df):
    """
    Prépare le pipeline de machine learning
    """
    print("\n🤖 PRÉPARATION DU PIPELINE ML")
    print("=" * 50)

    # Features pour l'entraînement
    feature_cols = [
        "Num_Moons",
        "Minerals",
        "Gravity",
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
        "Water_Presence",
        "temp_habitable",
        "gravity_earth_like",
        "optimal_sunlight",
        "tidally_locked",
        "mineral_rich",
        "habitability_score",
        "temp_gravity_interaction",
        "sunlight_rotation_ratio",
    ]

    # Assemblage des features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Normalisation
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

    print(f"Features utilisées: {len(feature_cols)}")
    print("Pipeline: VectorAssembler -> StandardScaler")

    return assembler, scaler, feature_cols


def train_models(df):
    """
    Entraîne plusieurs modèles de classification
    """
    print("\n🏋️ ENTRAÎNEMENT DES MODÈLES")
    print("=" * 50)

    # Préparation des données
    enhanced_df = engineer_features(df)
    assembler, scaler, feature_cols = prepare_ml_pipeline(enhanced_df)

    # Séparation train/test
    train_df, test_df = enhanced_df.randomSplit([0.8, 0.2], seed=42)

    print(f"Dataset d'entraînement: {train_df.count()} planètes")
    print(f"Dataset de test: {test_df.count()} planètes")

    # Configuration des modèles
    models = {
        "RandomForest": RandomForestClassifier(
            featuresCol="scaledFeatures",
            labelCol="Colonisable",
            numTrees=100,
            maxDepth=10,
            seed=42,
        ),
        "GradientBoosting": GBTClassifier(
            featuresCol="scaledFeatures",
            labelCol="Colonisable",
            maxIter=100,
            maxDepth=6,
            seed=42,
        ),
        "LogisticRegression": LogisticRegression(
            featuresCol="scaledFeatures", labelCol="Colonisable", maxIter=100
        ),
    }

    results = {}
    trained_models = {}

    for model_name, model in models.items():
        print(f"\n🔄 Entraînement: {model_name}")

        # Pipeline complet
        pipeline = Pipeline(stages=[assembler, scaler, model])

        # Entraînement
        trained_pipeline = pipeline.fit(train_df)
        trained_models[model_name] = trained_pipeline

        # Prédictions
        predictions = trained_pipeline.transform(test_df)

        # Évaluation
        binary_evaluator = BinaryClassificationEvaluator(
            labelCol="Colonisable", rawPredictionCol="rawPrediction"
        )
        multiclass_evaluator = MulticlassClassificationEvaluator(
            labelCol="Colonisable", predictionCol="prediction"
        )

        auc = binary_evaluator.evaluate(predictions)
        accuracy = multiclass_evaluator.evaluate(
            predictions, {multiclass_evaluator.metricName: "accuracy"}
        )
        precision = multiclass_evaluator.evaluate(
            predictions, {multiclass_evaluator.metricName: "weightedPrecision"}
        )
        recall = multiclass_evaluator.evaluate(
            predictions, {multiclass_evaluator.metricName: "weightedRecall"}
        )
        f1 = multiclass_evaluator.evaluate(
            predictions, {multiclass_evaluator.metricName: "f1"}
        )

        results[model_name] = {
            "AUC": auc,
            "Accuracy": accuracy,
            "Precision": precision,
            "Recall": recall,
            "F1-Score": f1,
        }

        print(f"  AUC: {auc:.3f}")
        print(f"  Accuracy: {accuracy:.3f}")
        print(f"  Precision: {precision:.3f}")
        print(f"  Recall: {recall:.3f}")
        print(f"  F1-Score: {f1:.3f}")

    # Meilleur modèle
    best_model_name = max(results.keys(), key=lambda k: results[k]["F1-Score"])
    best_model = trained_models[best_model_name]

    print(f"\n🏆 Meilleur modèle: {best_model_name}")
    print(f"F1-Score: {results[best_model_name]['F1-Score']:.3f}")

    return best_model, results, feature_cols


def analyze_feature_importance(model, feature_names):
    """
    Analyse l'importance des features (pour Random Forest)
    """
    print("\n📊 IMPORTANCE DES FEATURES")
    print("=" * 50)

    try:
        # Extraction du modèle Random Forest du pipeline
        rf_model = None
        for stage in model.stages:
            if hasattr(stage, "featureImportances"):
                rf_model = stage
                break

        if rf_model and hasattr(rf_model, "featureImportances"):
            importances = rf_model.featureImportances.toArray()

            # Création du DataFrame d'importance
            importance_data = list(zip(feature_names, importances))
            importance_data.sort(key=lambda x: x[1], reverse=True)

            print("Top 10 features les plus importantes:")
            for i, (feature, importance) in enumerate(importance_data[:10]):
                print(f"  {i+1:2d}. {feature:25s}: {importance:.3f}")

            return importance_data
        else:
            print("⚠️ Importance des features non disponible pour ce modèle")
            return None

    except Exception as e:
        print(f"❌ Erreur lors de l'analyse d'importance: {e}")
        return None


def predict_new_planets(model, new_data, spark, feature_cols):
    """
    Prédit l'habitabilité de nouvelles planètes
    """
    print("\n🔮 PRÉDICTIONS SUR NOUVELLES PLANÈTES")
    print("=" * 50)

    # Nouvelles planètes à tester
    test_planets = [
        ("New-World-1", 1, 75, 1.05, 9.2, 18.5, 24.1, 1, None),
        ("New-World-2", 0, 45, 0.95, 8.8, -12.3, 28.7, 0, None),
        ("New-World-3", 3, 88, 1.8, 7.1, 35.2, 15.9, 1, None),
        ("New-World-4", 0, 32, 0.7, 11.2, -78.4, 67.3, 1, None),
        ("New-World-5", 2, 91, 1.15, 8.9, 8.7, 22.4, 1, None),
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

    new_df = spark.createDataFrame(test_planets, schema)

    # Ajout des features engineered
    enhanced_new_df = engineer_features(new_df)

    # Prédictions
    predictions = model.transform(enhanced_new_df)

    print("Prédictions de colonisabilité:")
    predictions.select(
        "Name",
        "Temperature",
        "Gravity",
        "Water_Presence",
        "Minerals",
        "prediction",
        "probability",
    ).show(truncate=False)

    return predictions


def save_model_and_results(model, results, hdfs_namenode):
    """
    Sauvegarde le modèle et les résultats
    """
    print("\n💾 SAUVEGARDE DU MODÈLE")
    print("=" * 50)

    try:
        model_path = f"{hdfs_namenode}/planet_ml_models/habitability_predictor"
        model.write().overwrite().save(model_path)
        print(f"✅ Modèle sauvegardé: {model_path}")

        # Sauvegarde des métriques
        results_path = f"{hdfs_namenode}/planet_ml_models/evaluation_results"
        spark = SparkSession.getActiveSession()

        results_data = []
        for model_name, metrics in results.items():
            results_data.append(
                (
                    model_name,
                    metrics["AUC"],
                    metrics["Accuracy"],
                    metrics["Precision"],
                    metrics["Recall"],
                    metrics["F1-Score"],
                )
            )

        results_schema = StructType(
            [
                StructField("model_name", StringType(), True),
                StructField("auc", DoubleType(), True),
                StructField("accuracy", DoubleType(), True),
                StructField("precision", DoubleType(), True),
                StructField("recall", DoubleType(), True),
                StructField("f1_score", DoubleType(), True),
            ]
        )

        results_df = spark.createDataFrame(results_data, results_schema)
        results_df.write.mode("overwrite").parquet(results_path)
        print(f"✅ Résultats sauvegardés: {results_path}")

    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde: {e}")


def main():
    """
    Fonction principale du prédicteur d'habitabilité
    """
    print("🔮 PRÉDICTEUR D'HABITABILITÉ PLANÉTAIRE")
    print("=" * 60)

    # Configuration
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
    csv_path = "/app/planets_dataset.csv"

    # Création de la session Spark
    spark = create_spark_session()

    try:
        # 1. Chargement des données
        df = load_planet_data(spark, csv_path)

        print(f"\n📊 Dataset: {df.count()} planètes")
        print("\n🔍 Distribution des classes:")
        df.groupBy("Colonisable").count().show()

        # 2. Entraînement des modèles
        best_model, results, feature_cols = train_models(df)

        # 3. Analyse de l'importance des features
        feature_importance = analyze_feature_importance(best_model, feature_cols)

        # 4. Prédictions sur nouvelles planètes
        predictions = predict_new_planets(best_model, None, spark, feature_cols)

        # 5. Sauvegarde
        save_model_and_results(best_model, results, hdfs_namenode)

        print("\n✅ ENTRAÎNEMENT ET ÉVALUATION TERMINÉS")
        print(f"🎯 Meilleur modèle disponible pour prédictions")
        print(f"📊 Métriques et modèle sauvegardés dans HDFS")

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
