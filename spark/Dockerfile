FROM bde2020/spark-base:3.3.0-hadoop3.3

# Installation des dépendances Python
USER root

# Installation de netcat et des bibliothèques Python
RUN apk add --no-cache netcat-openbsd
COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt

WORKDIR /app

# Copie des applications Spark
COPY . /app/

# Rendre les scripts de démarrage exécutables et ajuster les permissions
RUN chmod +x /app/start_kafka_consumer.sh /app/start_pipeline.sh && \
    chown -R root:root /app/ && \
    chmod -R 755 /app/

# Variables d'environnement
ENV SPARK_APPLICATION_PYTHON_LOCATION /app
ENV PYTHONPATH="${PYTHONPATH}:/app"
ENV SPARK_MODE=consumer

CMD ["/bin/bash", "/app/start_pipeline.sh"] 