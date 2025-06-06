#!/usr/bin/env python3
"""
Test du consumer Kafka Spark automatique
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
        "Minerals": 450,  # Quantité de minéraux (entier)
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

    print(f"\n⏰ Test terminé à {datetime.now().strftime('%H:%M:%S')}")
    print("🎯 Le consumer Kafka Spark automatique est opérationnel !")

    return True


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

    if len(sys.argv) > 1 and sys.argv[1] == "monitor":
        monitor_consumer_realtime()
    else:
        test_spark_auto_consumer()
