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
        "id": str(uuid.uuid4()),
        "nom": "Kepler-442b",
        "decouvreur": "Équipe Kepler",
        "date_decouverte": "2015-01-06",
        "masse": 2.34,  # multiples de la masse terrestre
        "rayon": 1.34,  # multiples du rayon terrestre
        "distance": 1206.0,  # années-lumière
        "type": "super-terre",
        "statut": "confirmée",
        "atmosphere": "inconnue",
        "temperature_moyenne": -40.0,  # Celsius
        "periode_orbitale": 112.3,  # jours terrestres
        "nombre_satellites": 0,
        "presence_eau": "inconnue",
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
        "nom": "Planète Test",
        # Champs manquants intentionnellement
        "masse": -1.0,  # Valeur invalide
        "type": "type_invalide",
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
            "nom": "HD 40307g",
            "decouvreur": "Dr. Mikko Tuomi",
            "date_decouverte": "2012-11-07",
            "masse": 7.1,
            "rayon": 1.8,
            "distance": 42.0,
            "type": "super-terre",
            "statut": "confirmée",
            "atmosphere": "dense",
            "temperature_moyenne": 15.0,
            "periode_orbitale": 197.8,
            "nombre_satellites": 2,
            "presence_eau": "oui",
        },
        {
            "nom": "Proxima Centauri b",
            "decouvreur": "Guillem Anglada-Escudé",
            "date_decouverte": "2016-08-24",
            "masse": 1.17,
            "rayon": 1.1,
            "distance": 4.24,
            "type": "terrestre",
            "statut": "confirmée",
            "atmosphere": "mince",
            "temperature_moyenne": -39.0,
            "periode_orbitale": 11.2,
            "nombre_satellites": 0,
            "presence_eau": "inconnue",
        },
    ]

    for i, planet in enumerate(planets, 1):
        planet["id"] = str(uuid.uuid4())
        print(f"--- Planète {i}: {planet['nom']} ---")
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
