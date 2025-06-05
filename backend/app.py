from flask import Flask, request, jsonify
from validate import validate_dataset_planet_data
from producer import send_to_kafka
import logging
import uuid
from datetime import datetime

# Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/", methods=["GET"])
def health_check():
    """Point de santé de l'API"""
    return (
        jsonify(
            {
                "status": "healthy",
                "service": "Planet Discovery API",
                "timestamp": datetime.now().isoformat(),
            }
        ),
        200,
    )


@app.route("/discoveries", methods=["POST"])
def receive_discovery():
    """
    Endpoint pour recevoir les découvertes de planètes selon le modèle de l'énoncé
    """
    try:
        # Vérification que la requête contient du JSON
        if not request.is_json:
            return jsonify({"error": "Content-Type doit être application/json"}), 400

        data = request.get_json()

        # Vérification que les données ne sont pas vides
        if not data:
            return jsonify({"error": "Aucune donnée fournie"}), 400

        # Génération automatique de l'ID si non fourni
        if "id" not in data or not data["id"]:
            data["id"] = str(uuid.uuid4())

        # Ajout du timestamp de réception
        data["timestamp_reception"] = datetime.now().isoformat()

        # Validation des données
        valid, message = validate_dataset_planet_data(data)
        if not valid:
            logger.warning(f"Validation échouée: {message}")
            return jsonify({"error": message}), 400

        # Envoi vers Kafka
        try:
            send_to_kafka("planet_discoveries", data)
            logger.info(
                f"Découverte de planète envoyée avec succès - ID: {data['id']}, Nom: {data.get('nom', 'N/A')}"
            )
        except Exception as kafka_error:
            logger.error(f"Erreur lors de l'envoi vers Kafka: {str(kafka_error)}")
            return (
                jsonify({"error": "Erreur lors de l'envoi vers le pipeline Kafka"}),
                500,
            )

        return (
            jsonify(
                {
                    "message": "Découverte reçue et envoyée vers Kafka avec succès",
                    "id": data["id"],
                    "timestamp": data["timestamp_reception"],
                }
            ),
            201,
        )

    except Exception as e:
        logger.error(f"Erreur inattendue: {str(e)}")
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.route("/discoveries/dataset", methods=["POST"])
def receive_dataset_discovery():
    """
    Endpoint pour les données du dataset CSV existant (pour compatibilité)
    """
    try:
        if not request.is_json:
            return jsonify({"error": "Content-Type doit être application/json"}), 400

        data = request.get_json()

        if not data:
            return jsonify({"error": "Aucune donnée fournie"}), 400

        # Validation des données du dataset
        valid, message = validate_dataset_planet_data(data)
        if not valid:
            logger.warning(f"Validation dataset échouée: {message}")
            return jsonify({"error": message}), 400

        # Ajout d'un ID et timestamp
        data["id"] = str(uuid.uuid4())
        data["timestamp_reception"] = datetime.now().isoformat()

        # Envoi vers Kafka avec un topic différent
        try:
            send_to_kafka("dataset_planets", data)
            logger.info(
                f"Planète du dataset envoyée - ID: {data['id']}, Nom: {data.get('Name', 'N/A')}"
            )
        except Exception as kafka_error:
            logger.error(f"Erreur Kafka: {str(kafka_error)}")
            return jsonify({"error": "Erreur lors de l'envoi vers Kafka"}), 500

        return (
            jsonify(
                {
                    "message": "Données du dataset reçues et envoyées vers Kafka",
                    "id": data["id"],
                    "timestamp": data["timestamp_reception"],
                }
            ),
            201,
        )

    except Exception as e:
        logger.error(f"Erreur inattendue: {str(e)}")
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint non trouvé"}), 404


@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({"error": "Méthode HTTP non autorisée"}), 405


if __name__ == "__main__":
    logger.info("Démarrage de l'API Planet Discovery")
    app.run(host="0.0.0.0", port=5000, debug=True)
