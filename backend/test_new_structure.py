#!/usr/bin/env python3
"""
Script de test pour la nouvelle structure de données planétaires
Basé sur les colonnes du CSV : Name, Num_Moons, Minerals, Gravity,
Sunlight_Hours, Temperature, Rotation_Time, Water_Presence, Colonisable
"""

import requests
import json
import time
import random


def test_planet_discovery_api():
    """Test de l'API avec la nouvelle structure"""

    # URL de l'API
    base_url = "http://localhost:5001"

    # Test de santé
    print("🏥 Test de santé de l'API...")
    try:
        response = requests.get(f"{base_url}/")
        print(f"✅ API accessible: {response.status_code}")
        print(f"Réponse: {response.json()}")
    except Exception as e:
        print(f"❌ Erreur connexion API: {e}")
        return

    print("\n" + "=" * 50)

    # Test avec des données d'exemple
    print("🧪 Test d'envoi de découvertes...")

    # Quelques planètes d'exemple
    example_planets = [
        {
            "Name": "Kepler-442b",
            "Num_Moons": 2,
            "Minerals": 750,
            "Gravity": 1.2,
            "Sunlight_Hours": 12.5,
            "Temperature": 15.0,
            "Rotation_Time": 24.8,
            "Water_Presence": 1,
            "Colonisable": 1,
        },
        {
            "name": "TOI-715b",
            "num_moons": 0,
            "minerals": 320,
            "gravity": 0.8,
            "Sunlight_Hours": 8.2,
            "Temperature": -12.0,
            "Rotation_Time": 18.6,
            "Water_Presence": 0,
            "Colonisable": 0,
        },
    ]

    # Envoi des planètes d'exemple
    for planet in example_planets:
        print(f"\n📡 Envoi de {planet['name']}...")
        try:
            response = requests.post(
                f"{base_url}/discoveries",
                json=planet,
                headers={"Content-Type": "application/json"},
            )

            if response.status_code == 201:
                print(f"✅ Succès: {planet['name']} envoyée")
                print(f"Réponse: {response.json()['message']}")
            else:
                print(f"❌ Erreur {response.status_code}: {response.text}")

        except Exception as e:
            print(f"❌ Erreur réseau: {e}")

        time.sleep(1)  # Pause entre les envois

    print("\n" + "=" * 50)

    # Test de chargement du dataset complet
    print("📦 Test de chargement du dataset...")
    try:
        response = requests.post(f"{base_url}/discoveries/dataset")

        if response.status_code == 200:
            result = response.json()
            print("✅ Dataset chargé avec succès!")
            print(f"Planètes traitées: {result['total_processed']}")
            print(f"Succès: {result['success_count']}")
            print(f"Erreurs: {result['error_count']}")
        else:
            print(f"❌ Erreur chargement dataset: {response.status_code}")
            print(f"Réponse: {response.text}")

    except Exception as e:
        print(f"❌ Erreur chargement dataset: {e}")

    print("\n" + "=" * 50)

    # Test des statistiques
    print("📊 Test des statistiques...")
    try:
        response = requests.get(f"{base_url}/stats")

        if response.status_code == 200:
            stats = response.json()
            print("✅ Statistiques récupérées:")
            print(f"Total planètes: {stats['total_planets']}")
            print(f"Planètes avec eau: {stats['planets_with_water']}")
            print(f"Planètes colonisables: {stats['colonisable_planets']}")
            print(f"Température moyenne: {stats['average_temperature']:.2f}°C")
            print(f"Gravité moyenne: {stats['average_gravity']:.2f}")
        else:
            print(f"❌ Erreur statistiques: {response.status_code}")

    except Exception as e:
        print(f"❌ Erreur statistiques: {e}")


def generate_random_planet():
    """Génère une planète aléatoire pour les tests"""
    planet_names = [
        "Proxima-X",
        "Alpha-Beta",
        "Gamma-Prime",
        "Delta-Seven",
        "Epsilon-Minor",
        "Zeta-Major",
        "Theta-Colony",
        "Omega-Station",
    ]

    return {
        "Name": f"{random.choice(planet_names)}-{random.randint(1000, 9999)}",
        "Num_Moons": random.randint(0, 10),
        "Minerals": random.randint(50, 1000),
        "Gravity": round(random.uniform(0.1, 3.0), 2),
        "Sunlight_Hours": round(random.uniform(0, 24), 1),
        "Temperature": round(random.uniform(-100, 100), 1),
        "Rotation_Time": round(random.uniform(10, 100), 1),
        "Water_Presence": random.choice([0, 1]),
        "Colonisable": random.choice([0, 1]),
    }


def stress_test():
    """Test de charge avec plusieurs planètes aléatoires"""
    print("\n🔥 TEST DE CHARGE")
    print("=" * 50)

    base_url = "http://localhost:5001"
    num_planets = 10

    print(f"Génération de {num_planets} planètes aléatoires...")

    success_count = 0
    error_count = 0

    for i in range(num_planets):
        planet = generate_random_planet()

        try:
            response = requests.post(
                f"{base_url}/discoveries",
                json=planet,
                headers={"Content-Type": "application/json"},
                timeout=5,
            )

            if response.status_code == 201:
                success_count += 1
                print(f"✅ {i+1}/{num_planets}: {planet['Name']}")
            else:
                error_count += 1
                print(f"❌ {i+1}/{num_planets}: Erreur {response.status_code}")

        except Exception as e:
            error_count += 1
            print(f"❌ {i+1}/{num_planets}: Erreur réseau")

    print(f"\n📈 Résultats du test de charge:")
    print(f"Succès: {success_count}/{num_planets}")
    print(f"Erreurs: {error_count}/{num_planets}")
    print(f"Taux de succès: {(success_count/num_planets)*100:.1f}%")


if __name__ == "__main__":
    print("🚀 TESTS DE LA NOUVELLE STRUCTURE PLANÉTAIRE")
    print("=" * 60)

    # Test principal
    test_planet_discovery_api()

    # Test de charge
    stress_test()

    print("\n✅ Tests terminés!")
    print("\n💡 Pour analyser les données avec Spark:")
    print("docker exec tp5_planet-spark-app python /app/kafka_consumer_simple.py")
