#!/usr/bin/env python3
"""
Monitoring spécialisé pour le traitement du dataset de planètes
"""

import subprocess
import json
import time
from datetime import datetime
from collections import defaultdict


class DatasetProcessingMonitor:
    def __init__(self):
        self.planet_stats = defaultdict(int)
        self.temperature_ranges = {
            "Très froid": (-100, 0),
            "Froid": (0, 20),
            "Tempéré": (20, 40),
            "Chaud": (40, 80),
            "Très chaud": (80, 200),
        }

    def analyze_kafka_messages(self):
        """Analyser les messages Kafka pour extraire des statistiques"""
        try:
            # Lire les messages récents
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "kafka",
                    "kafka-console-consumer.sh",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--topic",
                    "planet_discoveries",
                    "--from-beginning",
                    "--timeout-ms",
                    "5000",
                    "--max-messages",
                    "50",
                ],
                capture_output=True,
                text=True,
                timeout=15,
            )

            if not result.stdout:
                return {"error": "Aucun message trouvé"}

            messages = [line for line in result.stdout.strip().split("\n") if line]
            planet_data = []

            for msg in messages:
                try:
                    data = json.loads(msg)
                    if "Name" in data:  # Vérifier que c'est notre format
                        planet_data.append(data)
                except:
                    continue

            return self.analyze_planet_data(planet_data)

        except Exception as e:
            return {"error": f"Erreur analyse Kafka: {e}"}

    def analyze_planet_data(self, planet_data):
        """Analyser les données des planètes"""
        if not planet_data:
            return {"error": "Aucune donnée planète trouvée"}

        stats = {
            "total_planets": len(planet_data),
            "water_planets": 0,
            "colonizable_planets": 0,
            "habitable_planets": 0,
            "temperature_distribution": defaultdict(int),
            "gravity_stats": {"min": float("inf"), "max": 0, "avg": 0},
            "moons_stats": {"min": float("inf"), "max": 0, "avg": 0},
            "interesting_planets": [],
        }

        temperatures = []
        gravities = []
        moons = []

        for planet in planet_data:
            try:
                # Présence d'eau
                if planet.get("Water_Presence") == 1:
                    stats["water_planets"] += 1

                # Colonisable
                if planet.get("Colonisable") == 1:
                    stats["colonizable_planets"] += 1

                # Habitable (eau + colonisable)
                if planet.get("Water_Presence") == 1 and planet.get("Colonisable") == 1:
                    stats["habitable_planets"] += 1
                    stats["interesting_planets"].append(
                        {
                            "name": planet.get("Name", "Unknown"),
                            "temperature": planet.get("Temperature", "N/A"),
                            "gravity": planet.get("Gravity", "N/A"),
                            "moons": planet.get("Num_Moons", "N/A"),
                        }
                    )

                # Distribution de température
                temp = planet.get("Temperature")
                if temp is not None:
                    temperatures.append(temp)
                    for range_name, (
                        min_temp,
                        max_temp,
                    ) in self.temperature_ranges.items():
                        if min_temp <= temp < max_temp:
                            stats["temperature_distribution"][range_name] += 1
                            break

                # Statistiques gravité
                gravity = planet.get("Gravity")
                if gravity is not None:
                    gravities.append(gravity)

                # Statistiques satellites
                num_moons = planet.get("Num_Moons")
                if num_moons is not None:
                    moons.append(num_moons)

            except Exception as e:
                continue

        # Calculer les statistiques finales
        if temperatures:
            stats["temperature_stats"] = {
                "min": min(temperatures),
                "max": max(temperatures),
                "avg": sum(temperatures) / len(temperatures),
            }

        if gravities:
            stats["gravity_stats"] = {
                "min": min(gravities),
                "max": max(gravities),
                "avg": sum(gravities) / len(gravities),
            }

        if moons:
            stats["moons_stats"] = {
                "min": min(moons),
                "max": max(moons),
                "avg": sum(moons) / len(moons),
            }

        return stats

    def check_spark_processing(self):
        """Vérifier le traitement Spark"""
        try:
            # Vérifier les applications Spark
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "spark-master",
                    "curl",
                    "-s",
                    "http://localhost:8080/api/v1/applications",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if result.returncode == 0:
                try:
                    apps = json.loads(result.stdout)
                    return {
                        "total_apps": len(apps),
                        "active_apps": len(
                            [
                                app
                                for app in apps
                                if not app.get("attempts", [{}])[-1].get(
                                    "completed", True
                                )
                            ]
                        ),
                        "apps": [
                            {"name": app.get("name"), "id": app.get("id")[:8]}
                            for app in apps[-3:]
                        ],
                    }
                except:
                    return {"error": "Format réponse Spark invalide"}
            else:
                return {"error": "Spark Master inaccessible"}

        except Exception as e:
            return {"error": f"Erreur Spark: {e}"}

    def check_hdfs_data(self):
        """Vérifier les données dans HDFS"""
        try:
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "hdfs-namenode",
                    "hdfs",
                    "dfs",
                    "-ls",
                    "/planet_discoveries/",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")
                files = []
                for line in lines[1:]:  # Skip header
                    parts = line.split()
                    if len(parts) >= 8:
                        files.append({"name": parts[-1], "size": parts[4]})

                return {"files": files, "total_files": len(files)}
            else:
                return {"error": "HDFS inaccessible ou répertoire inexistant"}

        except Exception as e:
            return {"error": f"Erreur HDFS: {e}"}

    def print_detailed_report(self):
        """Afficher un rapport détaillé"""
        timestamp = datetime.now().strftime("%H:%M:%S")

        print(f"\n🌍 RAPPORT DÉTAILLÉ DATASET PLANÈTES - {timestamp}")
        print("=" * 60)

        # 1. Analyse des messages Kafka
        print("📡 ANALYSE KAFKA:")
        kafka_stats = self.analyze_kafka_messages()

        if "error" in kafka_stats:
            print(f"   ❌ {kafka_stats['error']}")
        else:
            print(f"   • Total planètes: {kafka_stats['total_planets']}")
            print(
                f"   • Avec eau: {kafka_stats['water_planets']} ({kafka_stats['water_planets']/kafka_stats['total_planets']*100:.1f}%)"
            )
            print(
                f"   • Colonisables: {kafka_stats['colonizable_planets']} ({kafka_stats['colonizable_planets']/kafka_stats['total_planets']*100:.1f}%)"
            )
            print(
                f"   • Habitables: {kafka_stats['habitable_planets']} ({kafka_stats['habitable_planets']/kafka_stats['total_planets']*100:.1f}%)"
            )

            # Distribution des températures
            if kafka_stats.get("temperature_distribution"):
                print(f"\n   🌡️  Distribution températures:")
                for range_name, count in kafka_stats[
                    "temperature_distribution"
                ].items():
                    percentage = count / kafka_stats["total_planets"] * 100
                    print(f"      • {range_name}: {count} ({percentage:.1f}%)")

            # Statistiques détaillées
            if "temperature_stats" in kafka_stats:
                temp_stats = kafka_stats["temperature_stats"]
                print(f"\n   📊 Statistiques:")
                print(
                    f"      • Température: {temp_stats['min']:.1f}°C à {temp_stats['max']:.1f}°C (moy: {temp_stats['avg']:.1f}°C)"
                )

            if "gravity_stats" in kafka_stats:
                grav_stats = kafka_stats["gravity_stats"]
                print(
                    f"      • Gravité: {grav_stats['min']:.2f}g à {grav_stats['max']:.2f}g (moy: {grav_stats['avg']:.2f}g)"
                )

            # Planètes intéressantes
            if kafka_stats.get("interesting_planets"):
                print(f"\n   ⭐ Planètes habitables détectées:")
                for planet in kafka_stats["interesting_planets"][:5]:  # Limiter à 5
                    print(
                        f"      • {planet['name']}: {planet['temperature']:.1f}°C, {planet['gravity']:.2f}g"
                    )

        # 2. Traitement Spark
        print(f"\n⚡ TRAITEMENT SPARK:")
        spark_stats = self.check_spark_processing()

        if "error" in spark_stats:
            print(f"   ❌ {spark_stats['error']}")
        else:
            print(f"   • Applications totales: {spark_stats['total_apps']}")
            print(f"   • Applications actives: {spark_stats['active_apps']}")
            if spark_stats.get("apps"):
                print(f"   • Dernières apps:")
                for app in spark_stats["apps"]:
                    print(f"      • {app['name']} (ID: {app['id']})")

        # 3. Stockage HDFS
        print(f"\n💾 STOCKAGE HDFS:")
        hdfs_stats = self.check_hdfs_data()

        if "error" in hdfs_stats:
            print(f"   ❌ {hdfs_stats['error']}")
        else:
            print(f"   • Fichiers stockés: {hdfs_stats['total_files']}")
            if hdfs_stats.get("files"):
                print(f"   • Derniers fichiers:")
                for file_info in hdfs_stats["files"][-3:]:  # 3 derniers
                    print(f"      • {file_info['name']} ({file_info['size']} bytes)")

    def monitor_dataset_processing(self, interval=15):
        """Monitoring continu du traitement du dataset"""
        print("🚀 MONITORING TRAITEMENT DATASET PLANÈTES")
        print(f"   Intervalle: {interval} secondes")
        print("   Ctrl+C pour arrêter")
        print("=" * 60)

        try:
            while True:
                self.print_detailed_report()
                print(f"\n⏰ Prochaine mise à jour dans {interval}s...")
                print("=" * 60)
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\n\n🛑 Monitoring arrêté.")


def main():
    monitor = DatasetProcessingMonitor()

    print("🌍 MONITORING DATASET PLANÈTES")
    print("Choisissez le mode:")
    print("1. Rapport unique")
    print("2. Monitoring continu (15s)")
    print("3. Monitoring rapide (10s)")

    try:
        choice = input("\nVotre choix (1-3): ").strip()
    except KeyboardInterrupt:
        print("\nAnnulé.")
        return

    if choice == "1":
        monitor.print_detailed_report()
    elif choice == "2":
        monitor.monitor_dataset_processing(15)
    elif choice == "3":
        monitor.monitor_dataset_processing(10)
    else:
        print("Choix invalide. Rapport unique par défaut.")
        monitor.print_detailed_report()


if __name__ == "__main__":
    main()
