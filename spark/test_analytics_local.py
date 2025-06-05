#!/usr/bin/env python3
"""
Script de démonstration des analyses avancées - Version locale
Peut être exécuté sans Spark pour démontrer les concepts avec la nouvelle structure CSV
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
    Crée un dataset d'exemple de planètes avec la nouvelle structure CSV
    """
    data = [
        # Planètes colonisables/habitables
        {
            "Name": "Kepler-442b",
            "Num_Moons": 2,
            "Minerals": 65,
            "Gravity": 0.85,
            "Sunlight_Hours": 8.2,
            "Temperature": -15.5,
            "Rotation_Time": 112.3,
            "Water_Presence": 0,
            "Colonisable": 1,
        },
        {
            "Name": "TRAPPIST-1e",
            "Num_Moons": 0,
            "Minerals": 58,
            "Gravity": 0.92,
            "Sunlight_Hours": 7.1,
            "Temperature": -22.0,
            "Rotation_Time": 6.1,
            "Water_Presence": 1,
            "Colonisable": 1,
        },
        {
            "Name": "Gliese-667Cc",
            "Num_Moons": 1,
            "Minerals": 71,
            "Gravity": 1.5,
            "Sunlight_Hours": 9.3,
            "Temperature": -3.0,
            "Rotation_Time": 28.1,
            "Water_Presence": 1,
            "Colonisable": 1,
        },
        {
            "Name": "K2-18b",
            "Num_Moons": 2,
            "Minerals": 83,
            "Gravity": 2.3,
            "Sunlight_Hours": 5.9,
            "Temperature": -23.0,
            "Rotation_Time": 33.0,
            "Water_Presence": 1,
            "Colonisable": 1,
        },
        {
            "Name": "Kepler-452b",
            "Num_Moons": 1,
            "Minerals": 78,
            "Gravity": 1.2,
            "Sunlight_Hours": 10.5,
            "Temperature": 5.0,
            "Rotation_Time": 385.0,
            "Water_Presence": 1,
            "Colonisable": 1,
        },
        {
            "Name": "HD-40307g",
            "Num_Moons": 3,
            "Minerals": 82,
            "Gravity": 1.8,
            "Sunlight_Hours": 6.8,
            "Temperature": 15.0,
            "Rotation_Time": 197.8,
            "Water_Presence": 1,
            "Colonisable": 1,
        },
        {
            "Name": "Ocean-World-1",
            "Num_Moons": 1,
            "Minerals": 42,
            "Gravity": 1.05,
            "Sunlight_Hours": 9.1,
            "Temperature": 12.0,
            "Rotation_Time": 31.4,
            "Water_Presence": 1,
            "Colonisable": 1,
        },
        # Planètes non colonisables
        {
            "Name": "Venus-like-1",
            "Num_Moons": 0,
            "Minerals": 23,
            "Gravity": 0.9,
            "Sunlight_Hours": 12.1,
            "Temperature": 462.0,
            "Rotation_Time": 243.0,
            "Water_Presence": 0,
            "Colonisable": 0,
        },
        {
            "Name": "Mercury-like-1",
            "Num_Moons": 0,
            "Minerals": 45,
            "Gravity": 0.38,
            "Sunlight_Hours": 14.2,
            "Temperature": 167.0,
            "Rotation_Time": 58.6,
            "Water_Presence": 0,
            "Colonisable": 0,
        },
        {
            "Name": "Mars-like-1",
            "Num_Moons": 2,
            "Minerals": 31,
            "Gravity": 0.38,
            "Sunlight_Hours": 9.8,
            "Temperature": -80.0,
            "Rotation_Time": 24.6,
            "Water_Presence": 0,
            "Colonisable": 0,
        },
        {
            "Name": "Frozen-World-1",
            "Num_Moons": 0,
            "Minerals": 67,
            "Gravity": 0.9,
            "Sunlight_Hours": 5.4,
            "Temperature": -156.0,
            "Rotation_Time": 67.8,
            "Water_Presence": 1,
            "Colonisable": 0,
        },
        {
            "Name": "High-Gravity-1",
            "Num_Moons": 4,
            "Minerals": 88,
            "Gravity": 3.8,
            "Sunlight_Hours": 7.9,
            "Temperature": 18.0,
            "Rotation_Time": 14.2,
            "Water_Presence": 1,
            "Colonisable": 0,
        },
        {
            "Name": "Low-Gravity-1",
            "Num_Moons": 0,
            "Minerals": 55,
            "Gravity": 0.3,
            "Sunlight_Hours": 8.9,
            "Temperature": 8.0,
            "Rotation_Time": 19.7,
            "Water_Presence": 1,
            "Colonisable": 0,
        },
        {
            "Name": "Hot-Jupiter-1",
            "Num_Moons": 12,
            "Minerals": 15,
            "Gravity": 3.2,
            "Sunlight_Hours": 16.5,
            "Temperature": 1200.0,
            "Rotation_Time": 2.1,
            "Water_Presence": 0,
            "Colonisable": 0,
        },
        {
            "Name": "Desert-World-1",
            "Num_Moons": 0,
            "Minerals": 89,
            "Gravity": 0.7,
            "Sunlight_Hours": 13.8,
            "Temperature": 78.0,
            "Rotation_Time": 28.7,
            "Water_Presence": 0,
            "Colonisable": 0,
        },
    ]

    return pd.DataFrame(data)


def calculate_statistics(df):
    """
    Calcule les statistiques de base avec la nouvelle structure
    """
    print("📊 STATISTIQUES DE BASE")
    print("=" * 50)

    print(f"Nombre total de planètes: {len(df)}")
    print(f"Planètes colonisables: {df['Colonisable'].sum()}")
    print(f"Planètes non colonisables: {len(df) - df['Colonisable'].sum()}")

    print("\n💧 Distribution par présence d'eau:")
    water_dist = df["Water_Presence"].value_counts()
    print(f"  Avec eau: {water_dist.get(1, 0)}")
    print(f"  Sans eau: {water_dist.get(0, 0)}")

    print("\n🌙 Distribution par nombre de lunes:")
    moon_dist = df["Num_Moons"].value_counts().sort_index()
    for moons, count in moon_dist.items():
        print(f"  {moons} lune(s): {count}")

    print("\n📈 Statistiques numériques:")
    numeric_cols = [
        "Num_Moons",
        "Minerals",
        "Gravity",
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
    ]
    stats = df[numeric_cols].describe()
    print(stats.round(2))

    # Zones d'habitabilité
    habitable_temp = df[(df["Temperature"] >= -50) & (df["Temperature"] <= 50)]
    print(
        f"\n🌡️ Planètes dans zone de température habitable (-50°C à 50°C): {len(habitable_temp)}"
    )

    earth_like_gravity = df[(df["Gravity"] >= 0.8) & (df["Gravity"] <= 1.2)]
    print(
        f"🌍 Planètes avec gravité proche de la Terre (0.8-1.2g): {len(earth_like_gravity)}"
    )

    return stats


def analyze_correlations(df):
    """
    Analyse les corrélations avec la nouvelle structure
    """
    print("\n🔗 ANALYSE DES CORRÉLATIONS")
    print("=" * 50)

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
    correlation_matrix = df[numeric_cols].corr()

    print("Matrice de corrélation:")
    print(correlation_matrix.round(3))

    print("\n🔍 Corrélations importantes avec la colonisabilité:")
    colonisable_corr = correlation_matrix["Colonisable"].sort_values(
        key=abs, ascending=False
    )
    for feature, corr in colonisable_corr.items():
        if feature != "Colonisable" and abs(corr) > 0.2:
            print(f"  {feature}: {corr:.3f}")

    return correlation_matrix


def perform_clustering(df):
    """
    Effectue un clustering des planètes avec la nouvelle structure
    """
    print("\n🎯 CLUSTERING DES PLANÈTES")
    print("=" * 50)

    # Préparation des données
    features = ["Minerals", "Gravity", "Sunlight_Hours", "Temperature", "Rotation_Time"]
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
        print(f"  Planètes: {', '.join(cluster_planets['Name'].tolist())}")

        # Caractéristiques moyennes du cluster
        avg_stats = cluster_planets[features].mean()
        print(f"  Caractéristiques moyennes:")
        for feature in features:
            print(f"    {feature}: {avg_stats[feature]:.2f}")

        # Colonisabilité dans le cluster
        colonisable_count = cluster_planets["Colonisable"].sum()
        print(f"  Planètes colonisables: {colonisable_count}/{len(cluster_planets)}")

    return clusters


def engineer_features(df):
    """
    Crée des features dérivées pour améliorer l'analyse
    """
    print("\n🔧 INGÉNIERIE DES FEATURES")
    print("=" * 50)

    df_enhanced = df.copy()

    # Zone de température habitable
    df_enhanced["temp_habitable"] = (
        (df_enhanced["Temperature"] >= -50) & (df_enhanced["Temperature"] <= 50)
    ).astype(int)

    # Gravité proche de la Terre
    df_enhanced["gravity_earth_like"] = (
        (df_enhanced["Gravity"] >= 0.8) & (df_enhanced["Gravity"] <= 1.2)
    ).astype(int)

    # Ensoleillement optimal
    df_enhanced["optimal_sunlight"] = (
        (df_enhanced["Sunlight_Hours"] >= 8) & (df_enhanced["Sunlight_Hours"] <= 12)
    ).astype(int)

    # Richesse minérale
    df_enhanced["mineral_rich"] = (df_enhanced["Minerals"] >= 70).astype(int)

    # Score d'habitabilité composite
    df_enhanced["habitability_score"] = (
        df_enhanced["temp_habitable"] * 25
        + df_enhanced["gravity_earth_like"] * 20
        + df_enhanced["Water_Presence"] * 30
        + df_enhanced["optimal_sunlight"] * 15
        + (df_enhanced["Minerals"] / 100.0) * 10
    )

    print("Features créées:")
    print("- temp_habitable: Zone de température habitable")
    print("- gravity_earth_like: Gravité proche de la Terre")
    print("- optimal_sunlight: Ensoleillement optimal")
    print("- mineral_rich: Richesse minérale")
    print("- habitability_score: Score composite d'habitabilité")

    return df_enhanced


def train_colonisability_model(df):
    """
    Entraîne un modèle de prédiction de colonisabilité
    """
    print("\n🤖 ENTRAÎNEMENT DU MODÈLE DE COLONISABILITÉ")
    print("=" * 50)

    # Ingénierie des features
    df_enhanced = engineer_features(df)

    # Préparation des données
    feature_columns = [
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
        "mineral_rich",
        "habitability_score",
    ]

    X = df_enhanced[feature_columns]
    y = df_enhanced["Colonisable"]

    # Division train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )

    # Entraînement du modèle
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Prédictions et évaluation
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    print(f"Précision du modèle: {accuracy:.3f}")
    print("\nRapport de classification:")
    print(
        classification_report(
            y_test, y_pred, target_names=["Non colonisable", "Colonisable"]
        )
    )

    # Importance des features
    feature_importance = pd.DataFrame(
        {"feature": feature_columns, "importance": model.feature_importances_}
    ).sort_values("importance", ascending=False)

    print("\n📊 Importance des features:")
    for _, row in feature_importance.head(8).iterrows():
        print(f"  {row['feature']}: {row['importance']:.3f}")

    return model, feature_importance


def detect_anomalies(df):
    """
    Détecte les planètes avec des caractéristiques atypiques
    """
    print("\n🚨 DÉTECTION D'ANOMALIES")
    print("=" * 50)

    anomalies = []
    numeric_cols = [
        "Minerals",
        "Gravity",
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
    ]

    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        col_anomalies = df[(df[col] < lower_bound) | (df[col] > upper_bound)]

        if len(col_anomalies) > 0:
            print(f"\n📏 Anomalies pour {col}:")
            print(f"  Seuils: [{lower_bound:.2f}, {upper_bound:.2f}]")
            for _, planet in col_anomalies.iterrows():
                print(f"  {planet['Name']}: {planet[col]:.2f}")
                anomalies.append(
                    {
                        "planet": planet["Name"],
                        "feature": col,
                        "value": planet[col],
                        "expected_range": f"[{lower_bound:.2f}, {upper_bound:.2f}]",
                    }
                )

    return anomalies


def calculate_colonization_scores(df):
    """
    Calcule des scores de colonisation personnalisés
    """
    print("\n🚀 CALCUL DES SCORES DE COLONISATION")
    print("=" * 50)

    df_scores = df.copy()

    # Score basé sur multiple critères
    df_scores["colonization_score"] = (
        # Température idéale (entre -20 et 30°C)
        np.where(
            (df_scores["Temperature"] >= -20) & (df_scores["Temperature"] <= 30),
            25,
            np.where(
                (df_scores["Temperature"] >= -50) & (df_scores["Temperature"] <= 50),
                15,
                0,
            ),
        )
        +
        # Gravité proche de la Terre (0.8 à 1.2)
        np.where(
            (df_scores["Gravity"] >= 0.8) & (df_scores["Gravity"] <= 1.2),
            20,
            np.where(
                (df_scores["Gravity"] >= 0.5) & (df_scores["Gravity"] <= 2.0), 10, 0
            ),
        )
        +
        # Présence d'eau
        df_scores["Water_Presence"] * 30
        +
        # Ressources minérales (normalisé sur 25 points)
        (df_scores["Minerals"] * 25 / 100)
    )

    # Top planètes pour colonisation
    top_planets = df_scores.nlargest(10, "colonization_score")

    print("🏆 Top 10 des planètes pour la colonisation:")
    for i, (_, planet) in enumerate(top_planets.iterrows(), 1):
        print(
            f"  {i:2d}. {planet['Name']:15s}: {planet['colonization_score']:.1f} points"
        )
        print(
            f"      Temp: {planet['Temperature']:6.1f}°C, Gravité: {planet['Gravity']:.2f}g, "
            f"Eau: {'Oui' if planet['Water_Presence'] else 'Non'}, Minéraux: {planet['Minerals']}"
        )

    return df_scores


def generate_report(df, stats, correlations, model, anomalies):
    """
    Génère un rapport complet d'analyse
    """
    print("\n📋 GÉNÉRATION DU RAPPORT FINAL")
    print("=" * 50)

    report = {
        "timestamp": datetime.now().isoformat(),
        "dataset_summary": {
            "total_planets": len(df),
            "colonizable_planets": int(df["Colonisable"].sum()),
            "water_bearing_planets": int(df["Water_Presence"].sum()),
            "average_temperature": float(df["Temperature"].mean()),
            "average_gravity": float(df["Gravity"].mean()),
            "average_minerals": float(df["Minerals"].mean()),
        },
        "model_performance": {
            "algorithm": "RandomForest",
            "features_used": [
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
                "mineral_rich",
                "habitability_score",
            ],
        },
        "anomalies_detected": len(anomalies),
        "key_insights": [
            f"Temperature shows correlation of {correlations.loc['Temperature', 'Colonisable']:.3f} with colonizability",
            f"Water presence shows correlation of {correlations.loc['Water_Presence', 'Colonisable']:.3f} with colonizability",
            f"Gravity shows correlation of {correlations.loc['Gravity', 'Colonisable']:.3f} with colonizability",
            f"{len(df[(df['Temperature'] >= -50) & (df['Temperature'] <= 50)])} planets in habitable temperature zone",
        ],
    }

    # Sauvegarde du rapport
    with open(
        f"/tmp/planet_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        "w",
    ) as f:
        json.dump(report, f, indent=2)

    print("✅ Rapport généré et sauvegardé")
    print(f"📊 Résumé: {report['dataset_summary']['total_planets']} planètes analysées")
    print(
        f"🎯 {report['dataset_summary']['colonizable_planets']} planètes colonisables identifiées"
    )
    print(
        f"💧 {report['dataset_summary']['water_bearing_planets']} planètes avec eau détectées"
    )
    print(f"🚨 {report['anomalies_detected']} anomalies trouvées")

    return report


def main():
    """
    Fonction principale de démonstration des analyses
    """
    print("🔬 DÉMONSTRATION DES ANALYSES PLANÉTAIRES")
    print(
        "Structure CSV: Name, Num_Moons, Minerals, Gravity, Sunlight_Hours, Temperature, Rotation_Time, Water_Presence, Colonisable"
    )
    print("=" * 80)

    try:
        # 1. Création/chargement du dataset
        df = create_sample_dataset()
        print(f"📊 Dataset créé avec {len(df)} planètes d'exemple")

        # 2. Statistiques de base
        stats = calculate_statistics(df)

        # 3. Analyse des corrélations
        correlations = analyze_correlations(df)

        # 4. Clustering
        clusters = perform_clustering(df)

        # 5. Calcul des scores de colonisation
        df_with_scores = calculate_colonization_scores(df)

        # 6. Entraînement du modèle ML
        model, feature_importance = train_colonisability_model(df)

        # 7. Détection d'anomalies
        anomalies = detect_anomalies(df)

        # 8. Génération du rapport
        report = generate_report(df_with_scores, stats, correlations, model, anomalies)

        print("\n✅ ANALYSE COMPLÈTE TERMINÉE")
        print("🎯 Toutes les méthodes d'analyse ont été démontrées avec succès")
        print("📁 Rapport détaillé sauvegardé dans /tmp/")

    except Exception as e:
        print(f"❌ Erreur lors de l'analyse: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
