FROM bde2020/spark-base:3.3.0-hadoop3.3

# Installation des dépendances Python
USER root

# Installation des bibliothèques Python
COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt

WORKDIR /app

# Copie des applications Spark
COPY . /app/

# Variables d'environnement
ENV SPARK_APPLICATION_PYTHON_LOCATION /app
ENV PYTHONPATH="${PYTHONPATH}:/app"

CMD ["tail", "-f", "/dev/null"] 