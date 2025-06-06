FROM bitnami/spark:3.3.2

USER root

# Installer Python 3 + pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copier les dépendances
COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

# Copier les fichiers de ton app
COPY . /app
WORKDIR /app

# Lancer ton script principal Spark
CMD ["spark-submit", "--master", "spark://spark-master:7077", "main.py"]
