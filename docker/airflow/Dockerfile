# Custom Airflow image = official Airflow + Java 17 + pyspark/delta,
# so our ingestion task can run Spark inside the Airflow container.
FROM apache/airflow:2.9.3-python3.11

# --- system deps: Java 17 (Spark runs on the JVM) ---
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# --- python deps our pipeline needs (airflow already has psycopg2, pyyaml) ---
USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    delta-spark==3.2.0 \
    python-dotenv
