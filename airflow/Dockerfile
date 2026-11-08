FROM apache/airflow:3.3.2-python3.12

USER root

# Install OpenJDK 17 headless for Spark client and system utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python requirements for Lakehouse pipeline
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
