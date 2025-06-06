import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import os
import joblib

spark = SparkSession.builder \
    .appName("ModelKafkaConsumer") \
    .getOrCreate()

consumer = KafkaConsumer(
    'planet_discoveries',
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    auto_offset_reset='latest',
    group_id='spark-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

model = joblib.load("xgboost_model.pkl")


def run_inference(data):
    required_columns = [
        "Num_Moons",
        "Minerals",
        "Gravity",
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
        "Water_Presence"
    ]

    try:
        features = [float(data[col]) for col in required_columns]
    except KeyError as e:
        raise ValueError(f"Missing required field in input: {e}")

    predictions = model.predict([features])
    return predictions[0]


print("🟢 Waiting for messages...")
for msg in consumer:
    data = msg.value
    print("📩 Received:", data)

    result = run_inference(data)

    data["Colonisable"] = int(result)
    data["timestamp_reception"] = int(result)

    print("====================> Data:", data)

    json_str = json.dumps(data)

    # Option 1 : sauvegarder localement puis copier dans HDFS (avec hdfs cli)
    with open("/tmp/mon_dict.json", "w") as f:
        f.write(json_str)

    # En console shell (hors python), exécuter :
    # hdfs dfs -put /tmp/mon_dict.json /chemin/hdfs/mon_dict.json

    # Option 2 : écrire directement dans HDFS via PySpark
    rdd = spark.sparkContext.parallelize([json_str])
    rdd.saveAsTextFile("hdfs://namenode:9000/output/planets.json")
