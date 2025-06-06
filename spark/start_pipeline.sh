#!/bin/bash

echo "🌍 DÉMARRAGE PIPELINE KAFKA SPARK PLANÈTES"
echo "============================================="

# Fonction d'attente des services
wait_for_service() {
    local service=$1
    local port=$2
    local name=$3
    
    echo "🔍 Vérification de $name..."
    until nc -z $service $port; do
        echo "   $name non disponible, attente 5s..."
        sleep 5
    done
    echo "✅ $name connecté"
}

# Attendre que les services soient prêts
echo "⏳ Initialisation (30s)..."
sleep 30

# Vérifier tous les services
wait_for_service kafka 29092 "Kafka"
wait_for_service spark-master 7077 "Spark Master" 
wait_for_service namenode 9000 "HDFS"

echo "🎯 Tous les services sont prêts !"

# Détecter le mode à partir de la variable d'environnement
MODE=${SPARK_MODE:-"consumer"}

echo "📊 Mode sélectionné: $MODE"
echo "============================================="

case $MODE in
    "consumer")
        echo "📡 Lancement du Consumer Kafka temps réel..."
        exec /spark/bin/spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
            --master spark://spark-master:7077 \
            --driver-memory 1g \
            --executor-memory 1g \
            --executor-cores 1 \
            --total-executor-cores 2 \
            --conf spark.sql.adaptive.enabled=false \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            /app/kafka_consumer_simple.py
        ;;
    
    "analytics")
        echo "📊 Lancement des Analytics avancées..."
        exec /spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --driver-memory 2g \
            --executor-memory 2g \
            --executor-cores 2 \
            --total-executor-cores 4 \
            /app/advanced_analytics.py
        ;;
    
    "ml")
        echo "🤖 Lancement du modèle ML..."
        exec /spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --driver-memory 2g \
            --executor-memory 2g \
            --executor-cores 2 \
            --total-executor-cores 4 \
            /app/habitability_predictor.py
        ;;
    
    "pipeline")
        echo "🔄 Lancement du pipeline complet..."
        exec /spark/bin/spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
            --master spark://spark-master:7077 \
            --driver-memory 2g \
            --executor-memory 2g \
            --executor-cores 2 \
            --total-executor-cores 4 \
            /app/main_pipeline.py
        ;;
    
    "interactive")
        echo "💻 Mode interactif - gardez le container actif..."
        echo "Utilisez docker exec pour lancer manuellement les scripts"
        exec tail -f /dev/null
        ;;
    
    *)
        echo "❌ Mode inconnu: $MODE"
        echo "Modes disponibles: consumer, analytics, ml, pipeline, interactive"
        echo "🔄 Démarrage du consumer par défaut..."
        exec /spark/bin/spark-submit \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
            --master spark://spark-master:7077 \
            --driver-memory 1g \
            --executor-memory 1g \
            /app/kafka_consumer_simple.py
        ;;
esac 