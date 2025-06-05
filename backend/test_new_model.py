import requests
import json
from datetime import datetime
import uuid


def test_planet_discovery_api():
    """
    Test de l'API avec le nouveau modèle de découverte de planète
    """

    # URL de l'API
    base_url = "http://localhost:5001"

    # Test du health check
    print("=== Test Health Check ===")
    try:
        response = requests.get(f"{base_url}/")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
    except requests.RequestException as e:
        print(f"Erreur health check: {e}")

    print("\n" + "=" * 50 + "\n")

    # Exemple de découverte de planète selon le modèle de l'énoncé
    planet_discovery = {
        "Name": "Planet_18329",
        "Num_Moons": 5,
        "Minerals": 59,
        "Gravity": 1.981602859469247,
        "Sunlight_Hours": 5.8168191458771705,
        "Temperature": 28.381006239674264,
        "Rotation_Time": 56.76091939405808,
        "Water_Presence": 0,
        "Colonisable": 0
    }

    # Test de l'endpoint /discoveries
    print("=== Test Endpoint /discoveries ===")
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            f"{base_url}/discoveries",
            data=json.dumps(planet_discovery),
            headers=headers,
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
    except requests.RequestException as e:
        print(f"Erreur requête: {e}")

    print("\n" + "=" * 50 + "\n")

    # Test avec données invalides
    print("=== Test Validation - Données Invalides ===")
    invalid_planet = {
        "Name": "Planet_18329",
        "Num_Moons": 5,
        "Minerals": 59,
        # Champs manquants intentionnellement
        "Water_Presence": 0,
        "Colonisable": 0
    }

    try:
        response = requests.post(
            f"{base_url}/discoveries", data=json.dumps(invalid_planet), headers=headers
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
    except requests.RequestException as e:
        print(f"Erreur requête: {e}")

    print("\n" + "=" * 50 + "\n")

    # Test avec plusieurs exemples
    print("=== Test Multiples Découvertes ===")

    planets = [
        {
            "Name": "Planet_28900",
            "Num_Moons": 8,
            "Minerals": 672,
            "Gravity": 1.3881504830806715,
            "Sunlight_Hours": 14.715293728903166,
            "Temperature": 27.48564614824687,
            "Rotation_Time": 51.0340563211323,
            "Water_Presence": 0,
            "Colonisable": 0
        },
        {
            "Name": "Planet_56161",
            "Num_Moons": 3,
            "Minerals": 764,
            "Gravity": 2.5308267251520093,
            "Sunlight_Hours": 22.902523479273974,
            "Temperature": 63.39082702246432,
            "Rotation_Time": 42.99324764351807,
            "Water_Presence": 1,
            "Colonisable": 0
        }
    ]

    for i, planet in enumerate(planets, 1):
        planet["id"] = str(uuid.uuid4())
        print(f"--- Planète {i}: {planet['Name']} ---")
        try:
            response = requests.post(
                f"{base_url}/discoveries", data=json.dumps(planet), headers=headers
            )
            print(f"Status: {response.status_code}")
            if response.status_code == 201:
                print("✅ Envoyée avec succès")
            else:
                print(f"❌ Erreur: {response.json()}")
        except requests.RequestException as e:
            print(f"❌ Erreur requête: {e}")
        print()


if __name__ == "__main__":
    print("🚀 Test de l'API Planet Discovery")
    print("=" * 60)
    test_planet_discovery_api()
    print("=" * 60)
    print("✅ Tests terminés")
