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
    Utilise la nouvelle structure: Name, Num_Moons, Minerals, Gravity, Sunlight_Hours, Temperature, Rotation_Time, Water_Presence, Colonisable
    """
    # Dataset étendu avec des planètes et leurs caractéristiques d'habitabilité
    enhanced_data = [
        # Planètes habitables/colonisables
        ("Kepler-442b", 0, 45, 0.89, 12.5, -40.0, 112.3, 0, 1),
        ("Kepler-452b", 1, 78, 1.2, 16.8, 5.0, 385.0, 0, 1),
        ("HD_40307g", 2, 89, 1.8, 18.2, 15.0, 197.8, 1, 1),
        ("Proxima_Centauri_b", 0, 23, 1.1, 11.0, -39.0, 11.2, 0, 1),
        ("TRAPPIST-1e", 0, 34, 0.92, 8.5, -22.0, 6.1, 1, 1),
        ("Gliese_667Cc", 0, 56, 1.5, 13.5, -3.0, 28.1, 1, 1),
        ("K2-18b", 0, 67, 2.3, 20.1, -23.0, 33.0, 1, 1),
        ("TOI-715b", 0, 45, 1.55, 15.2, 15.0, 19.3, 0, 1),
        ("LP_890-9c", 0, 38, 1.4, 12.8, -25.0, 8.8, 0, 1),
        ("GJ_357d", 0, 52, 1.7, 17.3, -53.0, 55.7, 0, 1),
        ("Ocean_World_1", 3, 67, 1.3, 16.8, 8.0, 78.2, 1, 1),
        ("Kepler-1649c", 0, 41, 1.06, 10.5, -39.0, 19.5, 0, 1),
        ("TOI-175b", 0, 73, 1.73, 18.6, 8.0, 25.6, 0, 1),
        ("LHS_1140b", 0, 58, 1.4, 14.2, -53.0, 24.7, 0, 1),
        # Planètes tempérées mais non habitables (facteurs défavorables)
        ("Planet_18329", 5, 59, 1.98, 5.8, 28.4, 56.8, 0, 0),
        ("Planet_28900", 8, 672, 1.39, 14.7, 27.5, 51.0, 0, 0),
        ("Desert_Planet_1", 2, 890, 2.1, 18.5, 85.0, 45.6, 0, 0),
        ("Rocky_Planet_1", 1, 423, 1.9, 14.2, 120.0, 89.4, 0, 0),
        # Planètes trop chaudes
        ("Hot_Jupiter_1", 15, 12, 0.8, 24.0, 1200.0, 3.2, 0, 0),
        ("Venus_Like_1", 0, 234, 0.9, 22.3, 462.0, 243.0, 0, 0),
        ("Volcanic_Planet_1", 0, 789, 2.8, 19.6, 450.0, 156.7, 0, 0),
        ("Mercury_Like_1", 0, 156, 0.38, 23.8, 427.0, 58.6, 0, 0),
        ("Lava_World_1", 2, 445, 1.2, 21.4, 800.0, 12.3, 0, 0),
        # Planètes trop froides
        ("Ice_Giant_1", 25, 234, 3.8, 2.1, -180.0, 4500.0, 1, 0),
        ("Frozen_Planet_1", 4, 123, 1.1, 3.2, -200.0, 1200.0, 1, 0),
        ("Pluto_Like_1", 5, 67, 0.66, 1.8, -229.0, 6.4, 1, 0),
        ("Europa_Like_1", 0, 89, 1.3, 0.5, -160.0, 3.6, 1, 0),
        ("Titan_Like_1", 0, 78, 1.4, 0.8, -179.0, 15.9, 1, 0),
        # Géantes gazeuses (non habitables)
        ("Gas_Giant_1", 42, 156, 0.6, 22.3, -120.0, 2890.0, 0, 0),
        ("Jupiter_Like_1", 79, 89, 2.36, 20.1, -110.0, 4333.0, 0, 0),
        ("Saturn_Like_1", 83, 123, 0.69, 18.7, -140.0, 10747.0, 0, 0),
        ("Neptune_Like_1", 14, 234, 1.14, 5.4, -200.0, 60182.0, 0, 0),
        ("Uranus_Like_1", 27, 167, 0.89, 4.2, -195.0, 30589.0, 0, 0),
        # Planètes avec conditions extrêmes
        ("High_Radiation_1", 2, 345, 1.5, 25.6, 45.0, 89.2, 0, 0),
        ("No_Atmosphere_1", 0, 234, 2.1, 14.8, 200.0, 176.4, 0, 0),
        ("Tidally_Locked_1", 0, 123, 1.2, 24.0, -150.0, 12.4, 0, 0),
        ("Super_Earth_Heavy_1", 8, 567, 4.2, 16.3, 35.0, 234.7, 1, 0),
        ("Mini_Neptune_1", 12, 78, 0.8, 12.8, -45.0, 456.8, 0, 0),
        # Planètes potentiellement habitables mais avec défauts
        (
            "Planet_56161",
            3,
            764,
            2.53,
            22.9,
            63.4,
            43.0,
            1,
            0,
        ),  # Trop de minéraux, trop chaud
        ("Heavy_World_1", 6, 234, 3.5, 15.6, 12.0, 67.8, 1, 0),  # Gravité trop forte
        ("Windy_Planet_1", 1, 123, 1.1, 28.9, 18.0, 8.9, 1, 0),  # Trop de soleil
        (
            "Resource_Poor_1",
            0,
            12,
            0.95,
            14.2,
            22.0,
            45.6,
            1,
            0,
        ),  # Très peu de minéraux
        ("Fast_Rotation_1", 2, 89, 1.3, 16.4, 15.0, 2.3, 1, 0),  # Rotation trop rapide
        # Planètes de référence (Terre-like habitables)
        ("Earth_Like_1", 1, 234, 1.0, 12.0, 15.0, 24.0, 1, 1),
        ("Earth_Like_2", 2, 198, 0.98, 11.8, 18.0, 23.9, 1, 1),
        ("Earth_Like_3", 1, 267, 1.02, 12.3, 12.0, 24.1, 1, 1),
        ("Perfect_World_1", 2, 156, 0.95, 13.2, 22.0, 26.8, 1, 1),
        ("Garden_World_1", 3, 189, 1.05, 11.5, 19.0, 22.4, 1, 1),
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
            StructField(
                "Colonisable", IntegerType(), True
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
            "temperature_zone",
            when((col("Temperature") >= -50) & (col("Temperature") <= 50), 1).otherwise(
                0
            ),
        )
        .withColumn(
            "earth_like_gravity",
            when((col("Gravity") >= 0.5) & (col("Gravity") <= 2.0), 1).otherwise(0),
        )
        .withColumn(
            "optimal_sunlight",
            when(
                (col("Sunlight_Hours") >= 8) & (col("Sunlight_Hours") <= 16), 1
            ).otherwise(0),
        )
        .withColumn(
            "moderate_rotation",
            when(
                (col("Rotation_Time") >= 12) & (col("Rotation_Time") <= 48), 1
            ).otherwise(0),
        )
        .withColumn(
            "mineral_richness",
            when(col("Minerals") > 200, 1).otherwise(0),
        )
        .withColumn(
            "moon_stability",
            when((col("Num_Moons") >= 1) & (col("Num_Moons") <= 5), 1).otherwise(0),
        )
        .withColumn(
            "temperature_squared",
            col("Temperature") * col("Temperature"),  # Feature non-linéaire
        )
        .withColumn(
            "gravity_sunlight_interaction",
            col("Gravity") * col("Sunlight_Hours"),  # Interaction entre features
        )
        .withColumn(
            "habitability_score",
            (
                col("temperature_zone")
                + col("earth_like_gravity")
                + col("optimal_sunlight")
                + col("Water_Presence")
                + col("moderate_rotation")
            )
            / 5.0,  # Score d'habitabilité composite
        )
    )

    return enhanced_df


def prepare_ml_pipeline(df):
    """
    Prépare le pipeline de machine learning
    """
    # Features numériques originales
    original_features = [
        "Num_Moons",
        "Minerals",
        "Gravity",
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
        "Water_Presence",
    ]

    # Features engineered
    engineered_features = [
        "temperature_zone",
        "earth_like_gravity",
        "optimal_sunlight",
        "moderate_rotation",
        "mineral_richness",
        "moon_stability",
        "temperature_squared",
        "gravity_sunlight_interaction",
        "habitability_score",
    ]

    # Assemblage de toutes les features
    all_features = original_features + engineered_features
    assembler = VectorAssembler(inputCols=all_features, outputCol="features")

    # Normalisation
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

    return assembler, scaler, all_features


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
    df_features.groupBy("Colonisable").count().show()

    print("📈 Statistiques des features engineered:")
    df_features.select(
        "temperature_zone",
        "earth_like_gravity",
        "optimal_sunlight",
        "moderate_rotation",
        "habitability_score",
    ).describe().show()

    # Préparation du pipeline
    assembler, scaler, feature_names = prepare_ml_pipeline(df_features)

    # Division train/test
    train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

    print(f"🎯 Données d'entraînement: {train_df.count()}")
    print(f"🧪 Données de test: {test_df.count()}")

    # Modèles à tester
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
            maxIter=50,
            maxDepth=8,
            seed=42,
        ),
        "LogisticRegression": LogisticRegression(
            featuresCol="scaledFeatures",
            labelCol="Colonisable",
            maxIter=200,
            regParam=0.01,
        ),
    }

    best_model = None
    best_score = 0
    best_name = ""
    results = {}

    # Évaluateurs
    binary_evaluator = BinaryClassificationEvaluator(
        rawPredictionCol="rawPrediction",
        labelCol="Colonisable",
        metricName="areaUnderROC",
    )

    multi_evaluator = MulticlassClassificationEvaluator(
        predictionCol="prediction", labelCol="Colonisable", metricName="accuracy"
    )

    for model_name, model in models.items():
        print(f"\n🔧 Entraînement du modèle: {model_name}")

        # Pipeline complet
        pipeline = Pipeline(stages=[assembler, scaler, model])

        # Entraînement
        model_fitted = pipeline.fit(train_df)

        # Prédictions sur le test
        predictions = model_fitted.transform(test_df)

        # Évaluation
        auc_score = binary_evaluator.evaluate(predictions)
        accuracy_score = multi_evaluator.evaluate(predictions)

        # Calcul de métriques supplémentaires
        f1_evaluator = MulticlassClassificationEvaluator(
            predictionCol="prediction", labelCol="Colonisable", metricName="f1"
        )
        f1_score = f1_evaluator.evaluate(predictions)

        precision_evaluator = MulticlassClassificationEvaluator(
            predictionCol="prediction",
            labelCol="Colonisable",
            metricName="weightedPrecision",
        )
        precision_score = precision_evaluator.evaluate(predictions)

        results[model_name] = {
            "model": model_fitted,
            "auc": auc_score,
            "accuracy": accuracy_score,
            "f1": f1_score,
            "precision": precision_score,
        }

        print(f"  📈 AUC: {auc_score:.3f}")
        print(f"  🎯 Accuracy: {accuracy_score:.3f}")
        print(f"  📊 F1-Score: {f1_score:.3f}")
        print(f"  🔍 Precision: {precision_score:.3f}")

        if auc_score > best_score:
            best_score = auc_score
            best_model = model_fitted
            best_name = model_name

    print(f"\n🏆 Meilleur modèle: {best_name} (AUC: {best_score:.3f})")

    return best_model, results, test_df, feature_names


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
            StructField("Name", StringType(), True),
            StructField("Num_Moons", IntegerType(), True),
            StructField("Minerals", IntegerType(), True),
            StructField("Gravity", DoubleType(), True),
            StructField("Sunlight_Hours", DoubleType(), True),
            StructField("Temperature", DoubleType(), True),
            StructField("Rotation_Time", DoubleType(), True),
            StructField("Water_Presence", IntegerType(), True),
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
        "Name",
        "Temperature",
        "Gravity",
        "Water_Presence",
        "habitability_score",
        "prediction",
        "probability",
    ).show(truncate=False)

    # Analyse des prédictions
    habitable_count = predictions.filter(col("prediction") == 1).count()
    total_count = predictions.count()

    print(f"\n📊 Résumé des prédictions:")
    print(f"  🌍 Planètes prédites habitables: {habitable_count}/{total_count}")
    print(f"  📈 Pourcentage d'habitabilité: {(habitable_count/total_count)*100:.1f}%")

    return predictions


def evaluate_model_robustness(model, df, spark):
    """
    Évalue la robustesse du modèle avec validation croisée
    """
    print("\n🔄 ÉVALUATION DE LA ROBUSTESSE DU MODÈLE")
    print("=" * 50)

    try:
        # Préparation des données
        df_features = engineer_features(df)
        assembler, scaler, _ = prepare_ml_pipeline(df_features)

        # Création d'un modèle RandomForest pour la validation croisée
        rf = RandomForestClassifier(
            featuresCol="scaledFeatures", labelCol="Colonisable", numTrees=50, seed=42
        )

        pipeline = Pipeline(stages=[assembler, scaler, rf])

        # Grille de paramètres
        paramGrid = (
            ParamGridBuilder()
            .addGrid(rf.numTrees, [30, 50, 100])
            .addGrid(rf.maxDepth, [5, 8, 10])
            .build()
        )

        # Validation croisée
        evaluator = BinaryClassificationEvaluator(
            labelCol="Colonisable", metricName="areaUnderROC"
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=evaluator,
            numFolds=3,
            seed=42,
        )

        # Entraînement avec validation croisée
        cv_model = cv.fit(df_features)

        # Meilleurs paramètres
        best_params = cv_model.bestModel.stages[-1].extractParamMap()
        print("🏆 Meilleurs paramètres trouvés:")
        for param, value in best_params.items():
            print(f"  {param.name}: {value}")

        # Score de validation croisée
        avg_score = max(cv_model.avgMetrics)
        print(f"\n📊 Score moyen de validation croisée: {avg_score:.3f}")

        return cv_model

    except Exception as e:
        print(f"❌ Erreur lors de la validation croisée: {e}")
        return None


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
                name: {
                    "auc": result["auc"],
                    "accuracy": result["accuracy"],
                    "f1": result["f1"],
                    "precision": result["precision"],
                }
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

        # Aperçu du dataset
        print("\n🔍 Aperçu du dataset:")
        df.show(5)

        # 2. Entraînement des modèles
        best_model, results, test_df, feature_names = train_models(df)

        # 3. Analyse de l'importance des features
        feature_importance = analyze_feature_importance(best_model, feature_names)

        # 4. Évaluation de la robustesse
        cv_model = evaluate_model_robustness(best_model, df, spark)

        # 5. Exemples de prédiction sur de nouvelles planètes
        new_planets = [
            ("Candidate_A", 1, 150, 1.2, 12.0, 18.0, 24.5, 1),  # Très prometteur
            ("Candidate_B", 0, 80, 0.9, 10.5, -15.0, 28.3, 0),  # Moyennement prometteur
            ("Candidate_C", 15, 30, 0.6, 22.0, -120.0, 2500.0, 0),  # Géante gazeuse
            ("Candidate_D", 2, 250, 1.8, 14.8, 65.0, 45.2, 0),  # Trop chaud
            ("Candidate_E", 3, 189, 1.05, 11.5, 19.0, 22.4, 1),  # Très prometteur
            ("Hot_Desert", 0, 890, 2.1, 18.5, 150.0, 45.6, 0),  # Désert chaud
        ]

        predictions = predict_habitability(best_model, new_planets, spark)

        # 6. Sauvegarde
        save_model_and_results(best_model, results, hdfs_namenode)

        print("\n✅ MODÈLE D'HABITABILITÉ COMPLÉTÉ")
        print(
            "🎯 Le modèle peut maintenant prédire l'habitabilité de nouvelles planètes!"
        )
        print("\n📋 Facteurs clés identifiés pour l'habitabilité:")
        print("  • Température dans la zone habitable (-50°C à 50°C)")
        print("  • Gravité similaire à la Terre (0.5 à 2.0)")
        print("  • Présence d'eau")
        print("  • Heures de soleil modérées (8 à 16h)")
        print("  • Rotation modérée (12 à 48h)")

    except Exception as e:
        print(f"❌ Erreur lors de l'entraînement du modèle: {e}")
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
