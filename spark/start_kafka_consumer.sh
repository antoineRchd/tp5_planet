#!/bin/bash

echo "🚀 Démarrage du consumer Kafka Spark..."
echo "================================================"

# Attendre que les services soient prêts
echo "⏳ Attente des services..."
sleep 30

# Vérifier la connectivité Kafka
echo "🔍 Vérification de Kafka..."
until nc -z kafka 29092; do
  echo "   Kafka non disponible, attente 5s..."
  sleep 5
done
echo "✅ Kafka connecté"

# Vérifier la connectivité Spark Master
echo "🔍 Vérification de Spark Master..."
until nc -z spark-master 7077; do
  echo "   Spark Master non disponible, attente 5s..."
  sleep 5
done
echo "✅ Spark Master connecté"

# Vérifier HDFS
echo "🔍 Vérification de HDFS..."
until nc -z namenode 9000; do
  echo "   HDFS non disponible, attente 5s..."
  sleep 5
done
echo "✅ HDFS connecté"

echo "📡 Lancement du consumer Kafka Spark..."
echo "Ctrl+C pour arrêter"
echo "================================================"

# Lancer le consumer Kafka avec Spark
exec /spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 2 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.adaptive.coalescePartitions.enabled=false \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.streaming.kafka.consumer.cache.enabled=false \
  /app/kafka_consumer_simple.py 