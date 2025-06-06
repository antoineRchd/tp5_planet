#!/usr/bin/env python3
"""
Monitoring en temps réel du pipeline Kafka Spark
"""

import subprocess
import time
import json
from datetime import datetime
import signal
import sys


class KafkaSparkMonitor:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        print("\n\n🛑 Arrêt du monitoring...")
        self.running = False
        sys.exit(0)

    def check_kafka_topics(self):
        """Vérifier les topics Kafka"""
        try:
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "kafka",
                    "kafka-topics.sh",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--list",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if result.returncode == 0:
                topics = result.stdout.strip().split("\n")
                return topics
            return []
        except:
            return []

    def count_kafka_messages(self, topic):
        """Compter les messages dans un topic"""
        try:
            # Obtenir les offsets
            result = subprocess.run(
                [
                    "docker",
                    "exec",
                    "kafka",
                    "kafka-run-class.sh",
                    "kafka.tools.GetOffsetShell",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--topic",
                    topic,
                    "--time",
                    "-1",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")
                total = 0
                for line in lines:
                    if ":" in line:
                        offset = int(line.split(":")[-1])
                        total += offset
                return total
            return 0
        except:
            return 0

    def check_spark_jobs(self):
        """Vérifier les jobs Spark actifs"""
        try:
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
                    active_apps = [
                        app
                        for app in apps
                        if app.get("attempts", [{}])[-1].get("completed") == False
                    ]
                    return len(active_apps), len(apps)
                except:
                    return 0, 0
            return 0, 0
        except:
            return 0, 0

    def get_hdfs_usage(self):
        """Vérifier l'utilisation HDFS"""
        try:
            result = subprocess.run(
                ["docker", "exec", "hdfs-namenode", "hdfs", "dfsadmin", "-report"],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if result.returncode == 0:
                lines = result.stdout.split("\n")
                for line in lines:
                    if "DFS Used%" in line:
                        usage = line.split(":")[-1].strip()
                        return usage
            return "N/A"
        except:
            return "N/A"

    def print_status(self):
        """Afficher le statut complet"""
        timestamp = datetime.now().strftime("%H:%M:%S")

        print(f"\n📊 MONITORING KAFKA SPARK - {timestamp}")
        print("=" * 60)

        # Topics Kafka
        topics = self.check_kafka_topics()
        print(f"📡 KAFKA:")
        if topics:
            for topic in topics:
                if topic and not topic.startswith("__"):
                    count = self.count_kafka_messages(topic)
                    print(f"   • {topic}: {count} messages")
        else:
            print("   ❌ Aucun topic trouvé")

        # Spark Jobs
        active_apps, total_apps = self.check_spark_jobs()
        print(f"\n⚡ SPARK:")
        print(f"   • Applications actives: {active_apps}")
        print(f"   • Total applications: {total_apps}")

        # HDFS
        hdfs_usage = self.get_hdfs_usage()
        print(f"\n💾 HDFS:")
        print(f"   • Utilisation: {hdfs_usage}")

        # Services Docker
        print(f"\n🐳 SERVICES:")
        services = ["kafka", "spark-master", "spark-worker-1", "flask", "hdfs-namenode"]
        for service in services:
            try:
                result = subprocess.run(
                    [
                        "docker",
                        "ps",
                        "--filter",
                        f"name={service}",
                        "--format",
                        "{{.Status}}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=2,
                )

                if result.stdout.strip():
                    status = result.stdout.strip().split()[0]
                    if status == "Up":
                        print(f"   ✅ {service}: Actif")
                    else:
                        print(f"   ⚠️  {service}: {status}")
                else:
                    print(f"   ❌ {service}: Arrêté")
            except:
                print(f"   ❓ {service}: Indéterminé")

    def monitor_logs_realtime(self, service="spark-master", lines=5):
        """Afficher les logs en temps réel"""
        print(f"\n📋 LOGS {service.upper()} (dernières {lines} lignes):")
        print("-" * 40)

        try:
            result = subprocess.run(
                ["docker", "logs", "--tail", str(lines), service],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if result.stdout:
                for line in result.stdout.split("\n")[-lines:]:
                    if line.strip():
                        print(f"   {line}")

            if result.stderr:
                for line in result.stderr.split("\n")[-lines:]:
                    if line.strip() and "INFO" not in line:
                        print(f"   ⚠️  {line}")
        except:
            print("   Erreur lecture logs")

    def run_monitoring(self, interval=10):
        """Lancer le monitoring continu"""
        print("🚀 DÉMARRAGE DU MONITORING")
        print("   Intervalle: {} secondes".format(interval))
        print("   Ctrl+C pour arrêter")
        print("=" * 60)

        while self.running:
            try:
                self.print_status()
                self.monitor_logs_realtime()

                print(f"\n⏰ Prochaine mise à jour dans {interval}s...")
                print("=" * 60)

                time.sleep(interval)

                # Clear screen pour Windows/Unix
                import os

                os.system("cls" if os.name == "nt" else "clear")

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ Erreur monitoring: {e}")
                time.sleep(5)


def main():
    print("🔍 KAFKA SPARK PROCESSOR MONITOR")
    print("Choisissez le mode:")
    print("1. Status unique")
    print("2. Monitoring continu (10s)")
    print("3. Monitoring rapide (5s)")

    try:
        choice = input("\nVotre choix (1-3): ").strip()
    except KeyboardInterrupt:
        print("\nAnnulé.")
        return

    monitor = KafkaSparkMonitor()

    if choice == "1":
        monitor.print_status()
        monitor.monitor_logs_realtime()
    elif choice == "2":
        monitor.run_monitoring(10)
    elif choice == "3":
        monitor.run_monitoring(5)
    else:
        print("Choix invalide. Status unique par défaut.")
        monitor.print_status()


if __name__ == "__main__":
    main()
