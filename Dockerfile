# Use a specific Apache Airflow version
FROM apache/airflow:2.6.3-python3.9

# Switch to root user
USER root

# Install system dependencies required for MySQL, PySpark, grpcio-tools, and Java
RUN apt-get update -yqq && \
    apt-get install -yqq --no-install-recommends \
        locales \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
        curl \
        rsync \
        netcat-openbsd \
        build-essential \
        default-libmysqlclient-dev \
        python3-dev \
        python3-pip \
        python3-setuptools \
        python3-wheel \
        python3-protobuf \
        g++ \
        openjdk-11-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java environment variables for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Switch back to airflow user
USER airflow
WORKDIR /opt/airflow

# Copy the requirements file and install dependencies
# COPY SOURCE /DESTINATION
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir numpy==1.23.5 && \
    pip install --no-cache-dir -r /requirements.txt

# Copy DAGs, plugins, and configurations
COPY dags /opt/airflow/dags
COPY plugins /opt/airflow/plugins
COPY store_files /opt/airflow/store_files
COPY sql_files /opt/airflow/sql_files

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
