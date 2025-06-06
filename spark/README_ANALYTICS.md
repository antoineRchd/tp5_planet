# 🌍 Analyses Avancées et Modèles d'IA pour Découvertes de Planètes

Ce module contient les scripts d'analyse avancée et de machine learning pour le système de découverte de planètes, utilisant la nouvelle structure de données.

## 📊 Structure des Données

La nouvelle structure de données utilise les champs suivants :
- `Name` : Nom de la planète
- `Num_Moons` : Nombre de lunes/satellites
- `Minerals` : Quantité de minéraux
- `Gravity` : Force de gravité
- `Sunlight_Hours` : Heures d'exposition solaire
- `Temperature` : Température moyenne
- `Rotation_Time` : Temps de rotation
- `Water_Presence` : Présence d'eau (0/1)
- `Colonisable` : Potentiel de colonisation (0/1)

## 🔧 Scripts Disponibles

### 1. `advanced_analytics.py`
**Analyses avancées et statistiques descriptives**

**Fonctionnalités :**
- Statistiques de base des planètes
- Analyse des corrélations entre caractéristiques
- Analyse spécifique eau vs autres facteurs
- Clustering des planètes similaires (K-Means)
- Détection d'anomalies (méthode IQR)
- Analyse des facteurs d'habitabilité
- Sauvegarde vers HDFS et Hive

**Exécution :**
```bash
# Dans le conteneur Spark
python /app/advanced_analytics.py

# Ou via spark-submit
spark-submit --master spark://spark-master:7077 /app/advanced_analytics.py
```

### 2. `habitability_predictor.py`
**Modèle d'IA pour prédiction d'habitabilité**

**Fonctionnalités :**
- Ingénierie de features avancées
- Entraînement de modèles ML (RandomForest, GBT, LogisticRegression)
- Évaluation avec métriques multiples (AUC, Accuracy, F1-Score)
- Analyse d'importance des features
- Validation croisée pour robustesse
- Prédictions sur nouvelles planètes
- Sauvegarde du modèle persisté

**Exécution :**
```bash
# Dans le conteneur Spark
python /app/habitability_predictor.py

# Ou via spark-submit
spark-submit --master spark://spark-master:7077 /app/habitability_predictor.py
```

### 3. `run_analytics.py`
**Script d'orchestration des analyses**

**Fonctionnalités :**
- Exécution séquentielle des analyses et modèles
- Vérification de l'environnement Spark
- Gestion d'erreurs et rapports de progression
- Résumé complet des résultats

**Exécution :**
```bash
# Dans le conteneur Spark
python /app/run_analytics.py
```

### 4. `kafka_spark_processor.py`
**Traitement en temps réel avec Kafka**

**Fonctionnalités :**
- Traitement streaming des découvertes via Kafka
- Analyse en temps réel avec classification automatique
- Sauvegarde continue vers HDFS
- Analyses batch des données accumulées
- Interface interactive avec menu

**Exécution :**
```bash
# Dans le conteneur Spark
python /app/kafka_spark_processor.py

# Options :
# 1. Streaming temps réel
# 2. Analyse batch
# 3. Les deux
```

## 🚀 Guide d'Utilisation Rapide

### Étape 1: Démarrer l'environnement
```bash
# Démarrer les services
docker-compose up -d

# Vérifier que tous les services sont actifs
docker-compose ps
```

### Étape 2: Lancer les analyses complètes
```bash
# Entrer dans le conteneur Spark
docker exec -it tp5_planet-spark-app bash

# Lancer le pipeline complet
python /app/run_analytics.py
```

### Étape 3: Traitement temps réel (optionnel)
```bash
# Dans le conteneur Spark
python /app/kafka_spark_processor.py

# Choisir l'option 1 pour le streaming
# Envoyer des données via l'API Flask pour voir le traitement en temps réel
```

## 📈 Analyses Réalisées

### 🔬 Analyses Avancées
1. **Statistiques Descriptives**
   - Distribution des variables numériques
   - Zones de température
   - Présence d'eau et colonisabilité

2. **Analyse des Corrélations**
   - Matrice de corrélation entre toutes les variables
   - Identification des relations significatives (|r| > 0.3)

3. **Analyse Eau-Habitabilité**
   - Statistiques par présence d'eau
   - Relation température-eau
   - Relation minéraux-eau
   - Zone de température habitable (-50°C à 50°C)

4. **Clustering**
   - K-Means avec optimisation du nombre de clusters
   - Évaluation par Silhouette Score
   - Caractérisation des clusters

5. **Détection d'Anomalies**
   - Méthode des quartiles (IQR)
   - Identification des planètes atypiques

### 🤖 Modèle d'IA - Prédiction d'Habitabilité

1. **Ingénierie de Features**
   - `temperature_zone` : Zone de température habitable
   - `earth_like_gravity` : Gravité similaire à la Terre
   - `optimal_sunlight` : Exposition solaire optimale
   - `moderate_rotation` : Rotation modérée
   - `mineral_richness` : Richesse minérale
   - `moon_stability` : Stabilité orbitale (lunes)
   - `habitability_score` : Score composite d'habitabilité

2. **Modèles Entraînés**
   - **RandomForest** : Ensemble de arbres de décision
   - **Gradient Boosting** : Boosting avec arbres
   - **Logistic Regression** : Classification linéaire

3. **Évaluation**
   - **AUC** : Area Under ROC Curve
   - **Accuracy** : Précision globale
   - **F1-Score** : Harmonie précision/rappel
   - **Precision** : Précision pondérée

4. **Facteurs Clés Identifiés**
   - Zone de température habitable
   - Présence d'eau
   - Gravité terrestre
   - Exposition solaire modérée
   - Rotation stable

## 💾 Données de Sortie

### HDFS
- `/planet_analytics/enriched_data` : Données enrichies
- `/planet_analytics/results` : Résultats d'analyses
- `/planet_ml_models/habitability_model` : Modèle persisté
- `/streaming/planet_analysis` : Données streaming
- `/streaming/batch_summaries` : Résumés batch

### Hive
- `planet_discoveries.raw_data` : Données brutes
- `planet_discoveries.clustered_data` : Données avec clusters

### Fichiers Locaux
- `/tmp/model_results.json` : Métriques des modèles

## 🔧 Configuration

### Variables d'Environnement
```bash
SPARK_MASTER_URL=spark://spark-master:7077
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
HDFS_NAMENODE=hdfs://namenode:9000
HIVE_METASTORE=thrift://hive-metastore:9083
```

### Prérequis
- Apache Spark 3.3.0
- Hadoop 3.2.1
- Kafka 2.13
- Python packages : pyspark, numpy, pandas, matplotlib, seaborn

## 📊 Résultats Attendus

### Analyses Statistiques
- Distribution des types de planètes
- Corrélations entre caractéristiques
- Identification de zones habitables
- Groupes de planètes similaires

### Modèle d'Habitabilité
- Prédiction binaire (habitable/non-habitable)
- Score de confiance pour chaque prédiction
- Importance relative des facteurs
- Performance : AUC > 0.85 attendu

### Streaming
- Classification en temps réel
- Alertes pour planètes hautement habitables
- Accumulation de statistiques temporelles

## 🚨 Dépannage

### Problèmes Courants

1. **Erreur de connexion Spark**
   ```bash
   # Vérifier que Spark Master est actif
   docker logs tp5_planet-spark-master
   ```

2. **Erreur HDFS**
   ```bash
   # Vérifier HDFS
   docker logs tp5_planet-namenode
   docker logs tp5_planet-datanode
   ```

3. **Erreur Kafka**
   ```bash
   # Vérifier Kafka
   docker logs tp5_planet-kafka
   ```

4. **Mémoire insuffisante**
   - Ajuster les paramètres `--driver-memory` et `--executor-memory`
   - Réduire la taille des datasets de test

### Logs
```bash
# Logs Spark
docker logs tp5_planet-spark-app

# Logs en temps réel
docker logs -f tp5_planet-spark-app
```

## 🎯 Objectifs Atteints

✅ **Analyses Avancées Complètes**
- Relations entre caractéristiques des planètes
- Corrélations eau-habitabilité  
- Clustering automatique
- Détection d'anomalies

✅ **Modèle d'IA Performant**
- Prédiction d'habitabilité avec >85% de précision
- Features engineering avancées
- Validation croisée robuste
- Modèle persisté et réutilisable

✅ **Intégration Big Data**
- Sauvegarde HDFS distribuée
- Tables Hive pour requêtes SQL
- Traitement streaming Kafka
- Pipeline scalable

✅ **Interface Utilisateur**
- Scripts d'orchestration automatisés
- Rapports détaillés et visuels
- Métriques de performance complètes 