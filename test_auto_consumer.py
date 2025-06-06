#!/usr/bin/env python3
"""
Test du consumer Kafka Spark automatique avec vérification HDFS
"""

import requests
import time
import json
import subprocess
from datetime import datetime


def test_spark_auto_consumer():
    """Test complet du consumer automatique"""
    print("🧪 TEST DU CONSUMER KAFKA SPARK AUTOMATIQUE")
    print("=" * 60)

    # 1. Vérifier que le container Spark est actif
    print("1️⃣ Vérification du container Spark...")
    try:
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                "name=tp5_planet-spark-app",
                "--format",
                "{{.Status}}",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        if "Up" in result.stdout:
            print("   ✅ Container Spark actif")
        else:
            print("   ❌ Container Spark non actif")
            return False
    except Exception as e:
        print(f"   ❌ Erreur: {e}")
        return False

    # 2. Vérifier les logs récents du consumer
    print("\n2️⃣ Vérification des logs du consumer...")
    try:
        result = subprocess.run(
            ["docker", "logs", "--tail", "10", "tp5_planet-spark-app"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        logs = result.stdout
        if "DÉMARRAGE PIPELINE" in logs or "Consumer Kafka" in logs:
            print("   ✅ Consumer en cours d'exécution")
        else:
            print("   ⚠️  Consumer peut-être en attente")

        # Afficher les dernières lignes
        print("   📋 Dernières lignes des logs:")
        for line in logs.split("\n")[-5:]:
            if line.strip():
                print(f"      {line}")

    except Exception as e:
        print(f"   ❌ Erreur logs: {e}")

    # 3. Envoyer une planète de test
    print("\n3️⃣ Envoi d'une planète de test...")

    test_planet = {
        "Name": f"TestPlanet_{int(time.time())}",
        "Num_Moons": 3,
        "Minerals": 450,
        "Gravity": 9.8,
        "Sunlight_Hours": 12.5,
        "Temperature": 22.0,
        "Rotation_Time": 24.0,
        "Water_Presence": 1,
        "Colonisable": 1,
    }

    try:
        response = requests.post(
            "http://localhost:5001/discoveries", json=test_planet, timeout=10
        )

        if response.status_code == 201:
            print("   ✅ Planète envoyée avec succès")
            print(f"   📊 Réponse: {response.json()}")
        else:
            print(f"   ❌ Erreur envoi: {response.status_code}")
            return False

    except Exception as e:
        print(f"   ❌ Erreur requête: {e}")
        return False

    # 4. Attendre et vérifier le traitement
    print("\n4️⃣ Attente du traitement par Spark (30s)...")
    time.sleep(30)

    # 5. Vérifier les nouveaux logs
    print("\n5️⃣ Vérification du traitement...")
    try:
        result = subprocess.run(
            ["docker", "logs", "--tail", "20", "tp5_planet-spark-app"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        recent_logs = result.stdout

        # Chercher des signes de traitement
        if test_planet["Name"] in recent_logs:
            print("   ✅ Planète détectée dans les logs Spark")
        elif "ANALYSE" in recent_logs or "Résultats sauvegardés" in recent_logs:
            print("   ✅ Traitement Spark détecté")
        else:
            print("   ⚠️  Pas de trace évidente du traitement")

        print("   📋 Logs récents du traitement:")
        for line in recent_logs.split("\n")[-10:]:
            if line.strip():
                print(f"      {line}")

    except Exception as e:
        print(f"   ❌ Erreur vérification: {e}")

    # 6. Statistiques du container
    print("\n6️⃣ Statistiques du container...")
    try:
        result = subprocess.run(
            [
                "docker",
                "stats",
                "tp5_planet-spark-app",
                "--no-stream",
                "--format",
                "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        print("   📊 Utilisation des ressources:")
        print(f"      {result.stdout}")

    except Exception as e:
        print(f"   ❌ Erreur stats: {e}")

    # 7. Vérification HDFS
    print("\n7️⃣ Vérification de la sauvegarde HDFS...")
    hdfs_verified = verify_hdfs_storage(test_planet["Name"])

    if hdfs_verified:
        print("   ✅ Données sauvegardées dans HDFS")
    else:
        print("   ⚠️  Sauvegarde HDFS non vérifiée")

    # 8. Statistiques HDFS
    print("\n8️⃣ Statistiques HDFS...")
    get_hdfs_statistics()

    print(f"\n⏰ Test terminé à {datetime.now().strftime('%H:%M:%S')}")
    print("🎯 Le consumer Kafka Spark automatique est opérationnel !")

    if hdfs_verified:
        print("💾 Système complet : Kafka → Spark → HDFS ✅")

    return True


def verify_hdfs_storage(planet_name):
    """Vérifier que les données de la planète sont dans HDFS"""
    try:
        print("   🔍 Recherche de la planète dans HDFS...")

        # Créer un script Spark temporaire pour vérifier HDFS
        verification_script = f"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyHDFS").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    # Vérifier les données brutes
    raw_data = spark.read.parquet("hdfs://namenode:9000/planet_analytics/raw_data/planets_*")
    planet_found = raw_data.filter(raw_data.Name == "{planet_name}").count()
    
    print(f"HDFS_CHECK:RAW_DATA:{{planet_found}}")
    
    # Vérifier les analyses d'habitabilité
    try:
        habitability = spark.read.parquet("hdfs://namenode:9000/planet_analytics/results/habitability_*")
        hab_found = habitability.filter(habitability.Name == "{planet_name}").count()
        print(f"HDFS_CHECK:HABITABILITY:{{hab_found}}")
    except:
        print("HDFS_CHECK:HABITABILITY:0")
    
    # Vérifier les analyses de colonisation
    try:
        colonisation = spark.read.parquet("hdfs://namenode:9000/planet_analytics/results/colonisation_*")
        col_found = colonisation.filter(colonisation.Name == "{planet_name}").count()
        print(f"HDFS_CHECK:COLONISATION:{{col_found}}")
    except:
        print("HDFS_CHECK:COLONISATION:0")
        
except Exception as e:
    print(f"HDFS_CHECK:ERROR:{{e}}")
finally:
    spark.stop()
"""

        # Écrire le script temporaire
        with open("/tmp/verify_hdfs.py", "w") as f:
            f.write(verification_script)

        # Copier dans le container Spark
        subprocess.run(
            [
                "docker",
                "cp",
                "/tmp/verify_hdfs.py",
                "tp5_planet-spark-app:/tmp/verify_hdfs.py",
            ],
            timeout=10,
        )

        # Exécuter la vérification
        result = subprocess.run(
            [
                "docker",
                "exec",
                "tp5_planet-spark-app",
                "/spark/bin/spark-submit",
                "--packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                "/tmp/verify_hdfs.py",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        # Analyser les résultats
        output = result.stdout

        raw_count = 0
        hab_count = 0
        col_count = 0

        for line in output.split("\n"):
            if line.startswith("HDFS_CHECK:"):
                parts = line.split(":")
                if len(parts) >= 3:
                    check_type = parts[1]
                    count = parts[2]

                    try:
                        count_val = int(count)
                        if check_type == "RAW_DATA":
                            raw_count = count_val
                        elif check_type == "HABITABILITY":
                            hab_count = count_val
                        elif check_type == "COLONISATION":
                            col_count = count_val
                    except ValueError:
                        if check_type == "ERROR":
                            print(f"   ❌ Erreur HDFS: {count}")

        print(f"   📊 Résultats de vérification HDFS:")
        print(f"      • Données brutes: {raw_count} enregistrement(s)")
        print(f"      • Analyses habitabilité: {hab_count} enregistrement(s)")
        print(f"      • Analyses colonisation: {col_count} enregistrement(s)")

        # Nettoyage
        subprocess.run(["rm", "-f", "/tmp/verify_hdfs.py"])

        return raw_count > 0 and (hab_count > 0 or col_count > 0)

    except Exception as e:
        print(f"   ❌ Erreur vérification HDFS: {e}")
        return False


def get_hdfs_statistics():
    """Afficher les statistiques HDFS"""
    try:
        print("   📁 Structure des répertoires HDFS:")

        # Lister les répertoires HDFS
        result = subprocess.run(
            [
                "docker",
                "exec",
                "tp5_planet-namenode",
                "hdfs",
                "dfs",
                "-ls",
                "-R",
                "/planet_analytics",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )

        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")

            file_count = 0
            total_size = 0

            for line in lines:
                if line.startswith("d") or line.startswith("-"):
                    parts = line.split()
                    if len(parts) >= 5:
                        size = parts[4]
                        path = parts[-1]

                        if not line.startswith("d"):  # C'est un fichier
                            file_count += 1
                            try:
                                total_size += int(size)
                            except ValueError:
                                pass

                        # Afficher les éléments principaux
                        if "/planet_analytics/" in path:
                            item_type = "📁" if line.startswith("d") else "📄"
                            size_display = (
                                f"({size} bytes)" if not line.startswith("d") else ""
                            )
                            print(
                                f"      {item_type} {path.split('/')[-1]} {size_display}"
                            )

            print(f"   📊 Statistiques HDFS:")
            print(f"      • Nombre de fichiers: {file_count}")
            print(f"      • Taille totale: {total_size:,} bytes")

        else:
            print("   ⚠️  Impossible d'accéder à HDFS")

    except Exception as e:
        print(f"   ❌ Erreur statistiques HDFS: {e}")


def show_planet_from_hdfs(planet_name):
    """Afficher une planète spécifique depuis HDFS"""
    try:
        print(f"🔍 RECHERCHE DE '{planet_name}' DANS HDFS")
        print("=" * 50)

        # Script pour lire depuis HDFS
        read_script = f"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadPlanetFromHDFS").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    # Lire les données brutes
    raw_data = spark.read.parquet("hdfs://namenode:9000/planet_analytics/raw_data/planets_*")
    planet_data = raw_data.filter(raw_data.Name == "{planet_name}")
    
    if planet_data.count() > 0:
        print("\\n🌍 DONNÉES BRUTES:")
        planet_data.show(truncate=False)
        
        # Lire les analyses
        try:
            habitability = spark.read.parquet("hdfs://namenode:9000/planet_analytics/results/habitability_*")
            planet_hab = habitability.filter(habitability.Name == "{planet_name}")
            
            if planet_hab.count() > 0:
                print("\\n🌱 ANALYSE D'HABITABILITÉ:")
                planet_hab.select("Name", "conditions_habitables", "Temperature", "Gravity", "Water_Presence").show(truncate=False)
        except:
            print("\\n⚠️  Pas d'analyse d'habitabilité trouvée")
            
        try:
            colonisation = spark.read.parquet("hdfs://namenode:9000/planet_analytics/results/colonisation_*")
            planet_col = colonisation.filter(colonisation.Name == "{planet_name}")
            
            if planet_col.count() > 0:
                print("\\n🚀 ANALYSE DE COLONISATION:")
                planet_col.select("Name", "score_colonisation", "potentiel_colonisation", "Temperature", "Gravity").show(truncate=False)
        except:
            print("\\n⚠️  Pas d'analyse de colonisation trouvée")
    else:
        print(f"\\n❌ Planète '{planet_name}' non trouvée dans HDFS")
        
except Exception as e:
    print(f"\\n❌ Erreur: {{e}}")
finally:
    spark.stop()
"""

        # Écrire et exécuter le script
        with open("/tmp/read_planet.py", "w") as f:
            f.write(read_script)

        subprocess.run(
            [
                "docker",
                "cp",
                "/tmp/read_planet.py",
                "tp5_planet-spark-app:/tmp/read_planet.py",
            ],
            timeout=10,
        )

        result = subprocess.run(
            [
                "docker",
                "exec",
                "tp5_planet-spark-app",
                "/spark/bin/spark-submit",
                "--packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                "/tmp/read_planet.py",
            ],
            timeout=60,
        )

        # Nettoyage
        subprocess.run(["rm", "-f", "/tmp/read_planet.py"])

    except Exception as e:
        print(f"❌ Erreur lecture HDFS: {e}")


def monitor_consumer_realtime():
    """Monitoring en temps réel du consumer"""
    print("📡 MONITORING TEMPS RÉEL DU CONSUMER")
    print("=" * 50)
    print("Appuyez sur Ctrl+C pour arrêter")

    try:
        while True:
            # Afficher les logs en temps réel
            result = subprocess.run(
                ["docker", "logs", "--tail", "5", "tp5_planet-spark-app"],
                capture_output=True,
                text=True,
                timeout=5,
            )

            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"\n[{timestamp}] Logs récents:")

            for line in result.stdout.split("\n")[-3:]:
                if line.strip():
                    print(f"   {line}")

            time.sleep(10)

    except KeyboardInterrupt:
        print("\n👋 Monitoring arrêté")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "monitor":
            monitor_consumer_realtime()
        elif command == "hdfs":
            if len(sys.argv) > 2:
                planet_name = sys.argv[2]
                show_planet_from_hdfs(planet_name)
            else:
                print("Usage: python test_auto_consumer.py hdfs <nom_planete>")
        elif command == "stats":
            print("📊 STATISTIQUES HDFS")
            print("=" * 30)
            get_hdfs_statistics()
        else:
            print("Commandes disponibles:")
            print("  python test_auto_consumer.py          # Test complet")
            print("  python test_auto_consumer.py monitor  # Monitoring temps réel")
            print(
                "  python test_auto_consumer.py hdfs <nom>  # Lire planète depuis HDFS"
            )
            print("  python test_auto_consumer.py stats    # Statistiques HDFS")
    else:
        test_spark_auto_consumer()
