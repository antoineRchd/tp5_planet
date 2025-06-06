#!/usr/bin/env python3
"""
Script pour afficher l'analyse complète d'une planète stockée dans HDFS
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys
import os


def show_planet_analysis(planet_name):
    print("🔍 ANALYSE DE LA PLANÈTE : {}".format(planet_name))
    print("=" * 60)

    # Configuration Spark
    spark = SparkSession.builder.appName("ShowPlanetAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Données brutes de la planète
        print("\n🌍 DONNÉES BRUTES")
        print("-" * 30)
        try:
            raw_data = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/raw_data/planets_*"
            )
            planet_raw = raw_data.filter(col("Name") == planet_name)

            if planet_raw.count() > 0:
                planet_raw.show(truncate=False)

                # Détails de la planète
                planet_info = planet_raw.first()
                print("📋 Caractéristiques détaillées :")
                print("   • Nom : {}".format(planet_info["Name"]))
                print("   • Lunes : {}".format(planet_info["Num_Moons"]))
                print("   • Minéraux : {} unités".format(planet_info["Minerals"]))
                print("   • Gravité : {} G".format(planet_info["Gravity"]))
                print(
                    "   • Heures de soleil : {} h/jour".format(
                        planet_info["Sunlight_Hours"]
                    )
                )
                print("   • Température : {} °C".format(planet_info["Temperature"]))
                print(
                    "   • Temps de rotation : {} heures".format(
                        planet_info["Rotation_Time"]
                    )
                )
                print(
                    "   • Présence d'eau : {}".format(
                        "Oui" if planet_info["Water_Presence"] == 1 else "Non"
                    )
                )
                print(
                    "   • Colonisable : {}".format(
                        "Oui" if planet_info["Colonisable"] == 1 else "Non"
                    )
                )
            else:
                print(
                    "❌ Planète '{}' non trouvée dans les données brutes".format(
                        planet_name
                    )
                )
                return

        except Exception as e:
            print("❌ Erreur lecture données brutes : {}".format(e))
            return

        # 2. Analyse d'habitabilité
        print("\n🌱 ANALYSE D'HABITABILITÉ")
        print("-" * 30)
        try:
            habitability = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/results/habitability_*"
            )
            planet_habitability = habitability.filter(col("Name") == planet_name)

            if planet_habitability.count() > 0:
                hab_info = planet_habitability.first()
                status = hab_info["conditions_habitables"]
                print("🔬 Statut : {}".format(status))

                if status == "Potentiellement habitable":
                    print("✅ Cette planète remplit les conditions d'habitabilité :")
                    print(
                        "   • Température : {} °C (plage viable : -50 à 50°C)".format(
                            hab_info["Temperature"]
                        )
                    )
                    print(
                        "   • Gravité : {} G (plage viable : 0.5 à 2.0G)".format(
                            hab_info["Gravity"]
                        )
                    )
                    print(
                        "   • Eau présente : {}".format(
                            "Oui" if hab_info["Water_Presence"] == 1 else "Non"
                        )
                    )
                    print(
                        "   • Heures de soleil : {} h (plage viable : 8 à 16h)".format(
                            hab_info["Sunlight_Hours"]
                        )
                    )
                else:
                    print(
                        "❌ Cette planète ne remplit pas toutes les conditions d'habitabilité"
                    )
        except Exception as e:
            print("❌ Erreur lecture habitabilité : {}".format(e))

        # 3. Analyse de colonisation
        print("\n🚀 ANALYSE DE COLONISATION")
        print("-" * 30)
        try:
            colonisation = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/results/colonisation_*"
            )
            planet_colonisation = colonisation.filter(col("Name") == planet_name)

            if planet_colonisation.count() > 0:
                col_info = planet_colonisation.first()
                score = col_info["score_colonisation"]
                potential = col_info["potentiel_colonisation"]

                print("🎯 Score de colonisation : {}/100".format(score))
                print("📊 Potentiel : {}".format(potential))

                # Détail du scoring
                print("\n📈 Détail du score (sur 100) :")
                temp_score = 20 if 0 <= col_info["Temperature"] <= 40 else 0
                gravity_score = 25 if 0.8 <= col_info["Gravity"] <= 1.2 else 0
                water_score = 30 if col_info["Water_Presence"] == 1 else 0
                sun_score = 15 if 10 <= col_info["Sunlight_Hours"] <= 14 else 0
                mineral_score = 10 if col_info["Minerals"] > 500 else 5

                print(
                    "   • Température favorable (0-40°C) : {} points".format(temp_score)
                )
                print(
                    "   • Gravité optimale (0.8-1.2G) : {} points".format(gravity_score)
                )
                print("   • Présence d'eau : {} points".format(water_score))
                print("   • Heures de soleil (10-14h) : {} points".format(sun_score))
                print("   • Ressources minérales : {} points".format(mineral_score))
                print("   • TOTAL : {} points".format(score))

                # Recommandation
                if score >= 80:
                    print(
                        "\n🌟 RECOMMANDATION : Planète EXCELLENTE pour la colonisation !"
                    )
                elif score >= 60:
                    print("\n👍 RECOMMANDATION : BONNE candidate pour la colonisation")
                elif score >= 40:
                    print("\n⚖️ RECOMMANDATION : Colonisation POSSIBLE avec adaptations")
                else:
                    print(
                        "\n⚠️ RECOMMANDATION : Colonisation DIFFICILE, non prioritaire"
                    )

        except Exception as e:
            print("❌ Erreur lecture colonisation : {}".format(e))

        # 4. Position dans le classement
        print("\n🏆 CLASSEMENT")
        print("-" * 30)
        try:
            top10 = spark.read.parquet(
                "hdfs://namenode:9000/planet_analytics/results/top10_colonisation_*"
            )
            # Ajouter un numéro de rang
            top10_ranked = top10.orderBy(desc("score_colonisation")).withColumn(
                "rang", row_number().over(Window.orderBy(desc("score_colonisation")))
            )

            planet_rank = top10_ranked.filter(col("Name") == planet_name)
            if planet_rank.count() > 0:
                rank_info = planet_rank.first()
                rank = rank_info["rang"]
                print(
                    "🥇 Position : {}e place sur {} planètes analysées".format(
                        rank, top10.count()
                    )
                )

                # Afficher le top 5 pour contexte
                print("\n📋 Top 5 des planètes pour colonisation :")
                top5 = top10_ranked.limit(5)
                for row in top5.collect():
                    marker = "👉 " if row["Name"] == planet_name else "   "
                    print(
                        "{}{}. {} - Score: {}/100 ({})".format(
                            marker,
                            row["rang"],
                            row["Name"],
                            row["score_colonisation"],
                            row["potentiel_colonisation"],
                        )
                    )
            else:
                print("❌ Planète non classée dans le top 10")

        except Exception as e:
            print("❌ Erreur lecture classement : {}".format(e))

        print("\n✅ ANALYSE TERMINÉE")

    except Exception as e:
        print("❌ Erreur générale: {}".format(e))
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


def main():
    if len(sys.argv) != 2:
        print("Usage: python show_planet_analysis.py <nom_planete>")
        sys.exit(1)

    planet_name = sys.argv[1]
    show_planet_analysis(planet_name)


if __name__ == "__main__":
    main()
