#!/usr/bin/env python3
"""
Script pour vérifier et afficher les sauvegardes HDFS des analyses de planètes
"""

from pyspark.sql import SparkSession
import os


def main():
    print("🔍 VÉRIFICATION DES SAUVEGARDES HDFS")
    print("=" * 50)

    # Configuration Spark
    spark = SparkSession.builder.appName("CheckHDFSResults").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Lister les fichiers dans HDFS
        print("\n📁 Structure des répertoires HDFS:")

        # 1. Métadonnées disponibles
        try:
            metadata_files = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/metadata_*"
            )
            print("\n📋 Métadonnées d'analyses disponibles:")
            metadata_files.select(
                "timestamp", "nb_planetes", "type_analyse", "description"
            ).show(truncate=False)
        except Exception as e:
            print("⚠️ Aucune métadonnée trouvée: {}".format(e))

        # 2. Données brutes les plus récentes
        try:
            latest_raw = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/raw_data/planets_*"
            )
            print(f"\n🌍 Données brutes - Nombre de planètes: {latest_raw.count()}")
            print("Colonnes disponibles:", latest_raw.columns)
            latest_raw.show(5)
        except Exception as e:
            print("⚠️ Aucune donnée brute trouvée: {}".format(e))

        # 3. Statistiques de base les plus récentes
        try:
            latest_stats = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/stats/basic_stats_*"
            )
            print("\n📊 Statistiques de base:")
            latest_stats.show()
        except Exception as e:
            print("⚠️ Aucune statistique trouvée: {}".format(e))

        # 4. Analyse d'habitabilité
        try:
            habitability = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/results/habitability_*"
            )
            print(
                f"\n🌍 Analyse d'habitabilité - Nombre de planètes: {habitability.count()}"
            )
            habitability.groupBy("conditions_habitables").count().show()
        except Exception as e:
            print("⚠️ Aucune analyse d'habitabilité trouvée: {}".format(e))

        # 5. Analyse de colonisation
        try:
            colonisation = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/results/colonisation_*"
            )
            print(
                f"\n🚀 Analyse de colonisation - Nombre de planètes: {colonisation.count()}"
            )
            colonisation.groupBy("potentiel_colonisation").count().show()
        except Exception as e:
            print("⚠️ Aucune analyse de colonisation trouvée: {}".format(e))

        # 6. Top 10 de colonisation
        try:
            top10 = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/results/top10_colonisation_*"
            )
            print(f"\n🏆 Top 10 de colonisation:")
            top10.select(
                "Name",
                "score_colonisation",
                "potentiel_colonisation",
                "Temperature",
                "Gravity",
            ).show()
        except Exception as e:
            print("⚠️ Aucun top 10 trouvé: {}".format(e))

        print("\n✅ VÉRIFICATION TERMINÉE")

    except Exception as e:
        print("❌ Erreur lors de la vérification: {}".format(e))
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
