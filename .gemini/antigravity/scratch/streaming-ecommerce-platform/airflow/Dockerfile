FROM apache/airflow:2.9.2-python3.11

USER root

# Install postgres client utilities for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt-postgres, boto3, pyspark, and dependencies into Airflow environment
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
