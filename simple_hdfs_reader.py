#!/usr/bin/env python3
"""
Script simple pour lire les données depuis HDFS
"""

import subprocess
import sys


def read_planet_simple(planet_name):
    """Lire une planète avec un script Spark simplifié"""
    print(f"🔍 LECTURE SIMPLE DE '{planet_name}' DEPUIS HDFS")
    print("=" * 60)

    # Script Spark ultra-simplifié
    spark_script = f"""
from pyspark.sql import SparkSession
import sys

# Configuration Spark minimale
spark = SparkSession.builder \\
    .appName("SimpleRead") \\
    .config("spark.sql.adaptive.enabled", "false") \\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    print("🌍 DONNÉES BRUTES:")
    raw_data = spark.read.parquet("hdfs://namenode:9000/planet_analytics/raw_data/planets_*")
    planet_data = raw_data.filter(raw_data.Name == "{planet_name}")
    
    count = planet_data.count()
    print(f"Nombre d'enregistrements trouvés: {{count}}")
    
    if count > 0:
        # Afficher un échantillon
        print("\\nÉchantillon des données:")
        planet_data.select("Name", "Temperature", "Gravity", "Water_Presence", "Colonisable").limit(3).show()
        
        # Statistiques de base
        planet_sample = planet_data.first()
        print(f"\\n📋 Caractéristiques de {planet_name}:")
        print(f"   • Température: {{planet_sample['Temperature']}}°C")
        print(f"   • Gravité: {{planet_sample['Gravity']}}G")
        print(f"   • Eau présente: {{'Oui' if planet_sample['Water_Presence'] == 1 else 'Non'}}")
        print(f"   • Colonisable: {{'Oui' if planet_sample['Colonisable'] == 1 else 'Non'}}")
        print(f"   • Minéraux: {{planet_sample['Minerals']}} unités")
        print(f"   • Lunes: {{planet_sample['Num_Moons']}}")
        
        # Vérifier les analyses
        try:
            habitability = spark.read.parquet("hdfs://namenode:9000/planet_analytics/results/habitability_*")
            hab_data = habitability.filter(habitability.Name == "{planet_name}")
            if hab_data.count() > 0:
                hab_info = hab_data.first()
                print(f"\\n🌱 HABITABILITÉ: {{hab_info['conditions_habitables']}}")
        except:
            print("\\n⚠️  Pas d'analyse d'habitabilité trouvée")
            
        try:
            colonisation = spark.read.parquet("hdfs://namenode:9000/planet_analytics/results/colonisation_*")
            col_data = colonisation.filter(colonisation.Name == "{planet_name}")
            if col_data.count() > 0:
                col_info = col_data.first()
                print(f"🚀 COLONISATION: Score {{col_info['score_colonisation']}}/100 ({{col_info['potentiel_colonisation']}})")
        except:
            print("⚠️  Pas d'analyse de colonisation trouvée")
    else:
        print(f"❌ Planète '{planet_name}' non trouvée dans HDFS")
        
except Exception as e:
    print(f"❌ Erreur: {{e}}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
"""

    try:
        # Écrire le script
        with open("/tmp/simple_read.py", "w") as f:
            f.write(spark_script)

        # Copier dans le container
        subprocess.run(
            [
                "docker",
                "cp",
                "/tmp/simple_read.py",
                "tp5_planet-spark-app:/tmp/simple_read.py",
            ],
            timeout=10,
        )

        print("⏳ Exécution de la lecture HDFS...")

        # Exécuter avec un timeout plus court et en mode local
        result = subprocess.run(
            [
                "docker",
                "exec",
                "tp5_planet-spark-app",
                "/spark/bin/spark-submit",
                "--master",
                "local[1]",
                "--conf",
                "spark.sql.adaptive.enabled=false",
                "--conf",
                "spark.ui.enabled=false",
                "/tmp/simple_read.py",
            ],
            capture_output=True,
            text=True,
            timeout=90,
        )

        if result.returncode == 0:
            # Filtrer les logs pour ne garder que les infos importantes
            output_lines = result.stdout.split("\n")
            important_lines = []
            capture = False

            for line in output_lines:
                if "🌍 DONNÉES BRUTES:" in line:
                    capture = True
                elif "INFO" in line and capture:
                    continue
                elif "WARN" in line and capture:
                    continue

                if capture:
                    important_lines.append(line)

            if important_lines:
                print("\n".join(important_lines))
            else:
                print("✅ Lecture terminée - voir les logs complets si nécessaire")
        else:
            print(f"❌ Erreur d'exécution: {result.stderr}")

        # Nettoyage
        subprocess.run(["rm", "-f", "/tmp/simple_read.py"])

    except subprocess.TimeoutExpired:
        print("⏰ Timeout - Le processus prend trop de temps")
        print("💡 Les données sont présentes dans HDFS (voir vérification précédente)")
    except Exception as e:
        print(f"❌ Erreur: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        planet_name = sys.argv[1]
        read_planet_simple(planet_name)
    else:
        read_planet_simple("NouvelleTerre")
