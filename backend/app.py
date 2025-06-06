import os
import json
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
import csv

from validate import PlanetDiscovery, PlanetDiscoveryResponse
from producer import send_to_kafka

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)


@app.route("/", methods=["GET"])
def health_check():
    """Endpoint de vérification de santé du service"""
    return jsonify(
        {
            "service": "Planet Discovery API",
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
        }
    )


@app.route("/discoveries", methods=["POST"])
def create_discovery():
    """
    Endpoint pour enregistrer une nouvelle découverte de planète
    Utilise la structure du CSV : Name, Num_Moons, Minerals, Gravity,
    Sunlight_Hours, Temperature, Rotation_Time, Water_Presence, Colonisable
    """
    try:
        # Récupération des données de la requête
        data = request.get_json()
        if not data:
            return jsonify({"error": "Aucune donnée JSON fournie"}), 400

        logger.info(f"Nouvelle découverte reçue: {data}")

        # Validation avec Pydantic
        planet = PlanetDiscovery(**data)

        # Préparation du message pour Kafka
        kafka_message = {
            "Name": planet.Name,
            "Num_Moons": planet.Num_Moons,
            "Minerals": planet.Minerals,
            "Gravity": planet.Gravity,
            "Sunlight_Hours": planet.Sunlight_Hours,
            "Temperature": planet.Temperature,
            "Rotation_Time": planet.Rotation_Time,
            "Water_Presence": planet.Water_Presence,
            "Colonisable": planet.Colonisable,
        }



        # Envoi vers Kafka
        kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        topic = "planet_discoveries"

        success = send_to_kafka(topic, kafka_message)

        if success:
            logger.info(f"Découverte envoyée vers Kafka: {planet.Name}")
            response = PlanetDiscoveryResponse(
                message=f"Découverte de la planète '{planet.Name}' enregistrée avec succès",
                planet_data=planet,
            )
            return jsonify(response.dict()), 201
        else:
            logger.error(f"Échec de l'envoi vers Kafka pour: {planet.Name}")
            return (
                jsonify(
                    {
                        "error": "Échec de l'enregistrement dans le système de streaming",
                        "status": "failed",
                    }
                ),
                500,
            )

    except ValueError as ve:
        logger.warning(f"Erreur de validation: {str(ve)}")
        return jsonify({"error": f"Données invalides: {str(ve)}"}), 400

    except Exception as e:
        logger.error(f"Erreur interne: {str(e)}")
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.route("/discoveries/dataset", methods=["POST"])
def load_dataset():
    """
    Endpoint pour charger des données depuis le dataset CSV
    """
    try:
        # Lecture du fichier CSV
        csv_file_path = "planets_dataset.csv"
        if not os.path.exists(csv_file_path):
            return jsonify({"error": "Fichier dataset non trouvé"}), 404

        # Traitement des données du dataset avec le module csv
        kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        topic = "dataset_planets"

        success_count = 0
        error_count = 0
        total_processed = 0

        with open(csv_file_path, "r") as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                total_processed += 1
                try:
                    # Validation des données
                    planet_data = {
                        "Name": str(row["Name"]),
                        "Num_Moons": int(row["Num_Moons"]),
                        "Minerals": int(row["Minerals"]),
                        "Gravity": float(row["Gravity"]),
                        "Sunlight_Hours": float(row["Sunlight_Hours"]),
                        "Temperature": float(row["Temperature"]),
                        "Rotation_Time": float(row["Rotation_Time"]),
                        "Water_Presence": int(row["Water_Presence"]),
                        "Colonisable": int(row["Colonisable"]),
                    }

                    planet = PlanetDiscovery(**planet_data)

                    # Préparation du message pour Kafka
                    kafka_message = {
                        "Name": planet.Name,
                        "Num_Moons": planet.Num_Moons,
                        "Minerals": planet.Minerals,
                        "Gravity": planet.Gravity,
                        "Sunlight_Hours": planet.Sunlight_Hours,
                        "Temperature": planet.Temperature,
                        "Rotation_Time": planet.Rotation_Time,
                        "Water_Presence": planet.Water_Presence,
                        "Colonisable": planet.Colonisable,
                        "source": "dataset",
                    }

                    # Envoi vers Kafka
                    if send_to_kafka(topic, kafka_message):
                        success_count += 1
                    else:
                        error_count += 1
                        logger.warning(f"Échec Kafka pour planète: {planet.Name}")

                except Exception as e:
                    error_count += 1
                    logger.error(f"Erreur ligne {total_processed}: {str(e)}")

        logger.info(
            f"Dataset traité: {total_processed} planètes, {success_count} succès, {error_count} erreurs"
        )

        return (
            jsonify(
                {
                    "message": f"Dataset traité: {success_count} planètes envoyées, {error_count} erreurs",
                    "success_count": success_count,
                    "error_count": error_count,
                    "total_processed": total_processed,
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Erreur lors du chargement du dataset: {str(e)}")
        return jsonify({"error": f"Erreur lors du chargement: {str(e)}"}), 500


@app.route("/stats", methods=["GET"])
def get_stats():
    """Endpoint pour obtenir des statistiques basiques"""
    try:
        csv_file_path = "planets_dataset.csv"
        if os.path.exists(csv_file_path):
            # Calcul manuel des statistiques sans pandas
            total_planets = 0
            planets_with_water = 0
            Colonisable_planets = 0
            Temperatures = []
            gravities = []
            max_moons = 0
            total_Minerals = 0

            with open(csv_file_path, "r") as file:
                csv_reader = csv.DictReader(file)

                for row in csv_reader:
                    total_planets += 1

                    if int(row["Water_Presence"]) == 1:
                        planets_with_water += 1

                    if int(row["Colonisable"]) == 1:
                        Colonisable_planets += 1

                    Temperatures.append(float(row["Temperature"]))
                    gravities.append(float(row["Gravity"]))

                    Num_Moons = int(row["Num_Moons"])
                    if Num_Moons > max_moons:
                        max_moons = Num_Moons

                    total_Minerals += int(row["Minerals"])

            avg_Temperature = (
                sum(Temperatures) / len(Temperatures) if Temperatures else 0
            )
            avg_Gravity = sum(gravities) / len(gravities) if gravities else 0

            stats = {
                "total_planets": total_planets,
                "planets_with_water": planets_with_water,
                "Colonisable_planets": Colonisable_planets,
                "average_Temperature": round(avg_Temperature, 2),
                "average_Gravity": round(avg_Gravity, 2),
                "max_moons": max_moons,
                "total_Minerals": total_Minerals,
            }
            return jsonify(stats), 200
        else:
            return jsonify({"error": "Dataset non disponible"}), 404
    except Exception as e:
        logger.error(f"Erreur stats: {str(e)}")
        return jsonify({"error": "Erreur lors du calcul des statistiques"}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_ENV") == "development"
    app.run(host="0.0.0.0", port=port, debug=debug)
