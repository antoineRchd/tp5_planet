from flask import Flask, request, jsonify
from validate import validate_planet_data
from producer import send_to_kafka

app = Flask(__name__)

@app.route("/discoveries", methods=["POST"])
def receive_discovery():
    data = request.get_json()
    valid, message = validate_planet_data(data)
    if not valid:
        return jsonify({"error": message}), 400

    send_to_kafka("planet_discoveries", data)
    return jsonify({"message": "Discovery received and sent to Kafka."}), 201

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
