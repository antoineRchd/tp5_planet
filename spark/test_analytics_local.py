#!/usr/bin/env python3
"""
Script de démonstration des analyses avancées - Version locale
Peut être exécuté sans Spark pour démontrer les concepts
"""

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import json
from datetime import datetime


def create_sample_dataset():
    """
    Crée un dataset d'exemple de planètes pour les analyses
    """
    data = [
        # Planètes habitables
        {
            "nom": "Kepler-442b",
            "masse": 2.34,
            "rayon": 1.34,
            "distance": 1206.0,
            "temperature": -40.0,
            "type": "super-terre",
            "eau": 1,
            "habitable": 1,
        },
        {
            "nom": "TRAPPIST-1e",
            "masse": 0.772,
            "rayon": 0.918,
            "distance": 39.0,
            "temperature": -22.0,
            "type": "terrestre",
            "eau": 1,
            "habitable": 1,
        },
        {
            "nom": "Proxima Centauri b",
            "masse": 1.17,
            "rayon": 1.1,
            "distance": 4.24,
            "temperature": -39.0,
            "type": "terrestre",
            "eau": 0,
            "habitable": 1,
        },
        {
            "nom": "Gliese 667Cc",
            "masse": 3.7,
            "rayon": 1.5,
            "distance": 23.6,
            "temperature": -3.0,
            "type": "super-terre",
            "eau": 1,
            "habitable": 1,
        },
        {
            "nom": "K2-18b",
            "masse": 8.6,
            "rayon": 2.3,
            "distance": 124.0,
            "temperature": -23.0,
            "type": "super-terre",
            "eau": 1,
            "habitable": 1,
        },
        # Planètes non habitables
        {
            "nom": "WASP-12b",
            "masse": 445.0,
            "rayon": 1.79,
            "distance": 871.0,
            "temperature": 2516.0,
            "type": "géante gazeuse",
            "eau": 0,
            "habitable": 0,
        },
        {
            "nom": "CoRoT-7b",
            "masse": 4.8,
            "rayon": 1.58,
            "distance": 489.0,
            "temperature": 1800.0,
            "type": "super-terre",
            "eau": 0,
            "habitable": 0,
        },
        {
            "nom": "Kepler-7b",
            "masse": 150.0,
            "rayon": 1.48,
            "distance": 3000.0,
            "temperature": 1540.0,
            "type": "géante gazeuse",
            "eau": 0,
            "habitable": 0,
        },
        {
            "nom": "OGLE-390Lb",
            "masse": 5.5,
            "rayon": 1.5,
            "distance": 21500.0,
            "temperature": -223.0,
            "type": "super-terre",
            "eau": 0,
            "habitable": 0,
        },
        {
            "nom": "PSR B1257+12",
            "masse": 0.02,
            "rayon": 0.5,
            "distance": 2300.0,
            "temperature": -213.0,
            "type": "terrestre",
            "eau": 0,
            "habitable": 0,
        },
    ]

    return pd.DataFrame(data)


def calculate_statistics(df):
    """
    Calcule les statistiques de base
    """
    print("📊 STATISTIQUES DE BASE")
    print("=" * 50)

    print(f"Nombre total de planètes: {len(df)}")
    print(f"Planètes habitables: {df['habitable'].sum()}")
    print(f"Planètes non habitables: {len(df) - df['habitable'].sum()}")

    print("\n🌍 Distribution par type:")
    print(df["type"].value_counts())

    print("\n📈 Statistiques numériques:")
    stats = df[["masse", "rayon", "distance", "temperature"]].describe()
    print(stats.round(2))

    return stats


def analyze_correlations(df):
    """
    Analyse les corrélations
    """
    print("\n🔗 ANALYSE DES CORRÉLATIONS")
    print("=" * 50)

    numeric_cols = ["masse", "rayon", "distance", "temperature", "eau", "habitable"]
    correlation_matrix = df[numeric_cols].corr()

    print("Matrice de corrélation:")
    print(correlation_matrix.round(3))

    print("\n🔍 Corrélations importantes avec l'habitabilité:")
    habitability_corr = correlation_matrix["habitable"].sort_values(
        key=abs, ascending=False
    )
    for feature, corr in habitability_corr.items():
        if feature != "habitable" and abs(corr) > 0.3:
            print(f"  {feature}: {corr:.3f}")

    return correlation_matrix


def perform_clustering(df):
    """
    Effectue un clustering des planètes
    """
    print("\n🎯 CLUSTERING DES PLANÈTES")
    print("=" * 50)

    # Préparation des données
    features = ["masse", "rayon", "distance", "temperature"]
    X = df[features].values

    # Normalisation
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # K-Means
    n_clusters = 3
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    clusters = kmeans.fit_predict(X_scaled)

    df["cluster"] = clusters

    print(f"Clustering en {n_clusters} groupes:")
    for i in range(n_clusters):
        cluster_planets = df[df["cluster"] == i]
        print(f"\n🌌 Cluster {i} ({len(cluster_planets)} planètes):")
        print(f"  Planètes: {', '.join(cluster_planets['nom'].tolist())}")
        print(f"  Moyenne masse: {cluster_planets['masse'].mean():.2f}")
        print(f"  Moyenne température: {cluster_planets['temperature'].mean():.2f}")
        print(f"  % habitables: {cluster_planets['habitable'].mean()*100:.1f}%")

    return df


def train_habitability_model(df):
    """
    Entraîne un modèle de prédiction d'habitabilité
    """
    print("\n🤖 MODÈLE D'IA: PRÉDICTION D'HABITABILITÉ")
    print("=" * 50)

    # Préparation des features
    features = ["masse", "rayon", "distance", "temperature", "eau"]
    X = df[features].values
    y = df["habitable"].values

    # Division train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )

    # Entraînement Random Forest
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Prédictions
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    print(f"🎯 Précision du modèle: {accuracy:.2%}")

    # Importance des features
    feature_importance = list(zip(features, model.feature_importances_))
    feature_importance.sort(key=lambda x: x[1], reverse=True)

    print("\n📊 Importance des caractéristiques:")
    for feature, importance in feature_importance:
        print(f"  {feature}: {importance:.3f}")

    # Test sur de nouvelles planètes
    print("\n🔮 Prédictions sur nouvelles planètes:")
    new_planets = [
        ["Planète X1", 1.5, 1.2, 50.0, 10.0, 1],  # Potentiellement habitable
        ["Planète X2", 10.0, 3.0, 200.0, 500.0, 0],  # Non habitable
        ["Planète X3", 0.8, 0.9, 25.0, -10.0, 1],  # Potentiellement habitable
    ]

    for planet_data in new_planets:
        name = planet_data[0]
        features_values = planet_data[1:]
        prediction = model.predict([features_values])[0]
        probability = model.predict_proba([features_values])[0][1]

        habitability = "HABITABLE" if prediction == 1 else "NON HABITABLE"
        print(f"  {name}: {habitability} (probabilité: {probability:.2%})")

    return model


def detect_anomalies(df):
    """
    Détecte les anomalies dans les données
    """
    print("\n🚨 DÉTECTION D'ANOMALIES")
    print("=" * 50)

    anomalies = []
    numeric_cols = ["masse", "rayon", "distance", "temperature"]

    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        col_anomalies = df[(df[col] < lower_bound) | (df[col] > upper_bound)]

        if not col_anomalies.empty:
            print(f"\n📏 Anomalies pour {col}:")
            print(f"  Seuils: [{lower_bound:.2f}, {upper_bound:.2f}]")
            for _, planet in col_anomalies.iterrows():
                print(f"    {planet['nom']}: {planet[col]:.2f}")
                anomalies.append(planet["nom"])

    unique_anomalies = list(set(anomalies))
    print(f"\n🔍 Planètes avec anomalies: {len(unique_anomalies)}")
    for anomaly in unique_anomalies:
        print(f"  - {anomaly}")

    return unique_anomalies


def generate_report(df, stats, correlations, model, anomalies):
    """
    Génère un rapport complet
    """
    print("\n📋 RAPPORT FINAL")
    print("=" * 50)

    report = {
        "timestamp": datetime.now().isoformat(),
        "dataset": {
            "total_planets": len(df),
            "habitable_planets": int(df["habitable"].sum()),
            "planet_types": df["type"].value_counts().to_dict(),
        },
        "statistics": {
            "average_mass": float(df["masse"].mean()),
            "average_radius": float(df["rayon"].mean()),
            "average_temperature": float(df["temperature"].mean()),
            "habitable_percentage": float(df["habitable"].mean() * 100),
        },
        "correlations": {
            "water_temperature": float(correlations.loc["eau", "temperature"]),
            "mass_radius": float(correlations.loc["masse", "rayon"]),
            "habitability_temperature": float(
                correlations.loc["habitable", "temperature"]
            ),
        },
        "clustering": {
            "clusters_found": int(df["cluster"].nunique()),
            "cluster_distribution": df["cluster"].value_counts().to_dict(),
        },
        "anomalies": {"count": len(anomalies), "planets": anomalies},
        "model_performance": {
            "accuracy": "Évalué sur échantillon test",
            "key_features": ["température", "présence d'eau", "masse"],
        },
    }

    # Sauvegarde du rapport
    with open("/tmp/planet_analysis_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("✅ Analyse complète terminée!")
    print(f"📄 Rapport sauvegardé: /tmp/planet_analysis_report.json")
    print(f"🌍 {report['dataset']['total_planets']} planètes analysées")
    print(
        f"🏡 {report['dataset']['habitable_planets']} planètes potentiellement habitables"
    )
    print(f"🚨 {report['anomalies']['count']} anomalies détectées")


def main():
    """
    Fonction principale de démonstration
    """
    print("🔬 ANALYSE AVANCÉE DES DÉCOUVERTES DE PLANÈTES")
    print("Version de démonstration locale")
    print("=" * 60)

    # 1. Création du dataset
    df = create_sample_dataset()
    print(f"📊 Dataset créé avec {len(df)} planètes")

    # 2. Statistiques de base
    stats = calculate_statistics(df)

    # 3. Analyse des corrélations
    correlations = analyze_correlations(df)

    # 4. Clustering
    df = perform_clustering(df)

    # 5. Modèle d'IA
    model = train_habitability_model(df)

    # 6. Détection d'anomalies
    anomalies = detect_anomalies(df)

    # 7. Rapport final
    generate_report(df, stats, correlations, model, anomalies)

    print("\n🌐 INTERFACES DISPONIBLES:")
    print("  - API Flask: http://localhost:5001")
    print("  - Kafka Topics: http://localhost:9092")

    print("\n🚀 PROCHAINES ÉTAPES:")
    print("  - Démarrer les services Spark complets")
    print("  - Traitement streaming en temps réel")
    print("  - Stockage HDFS et Hive")


if __name__ == "__main__":
    main()
