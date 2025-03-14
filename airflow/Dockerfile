# Dockerfile
FROM apache/airflow:2.10.4
USER root
RUN apt-get update && apt-get install -y python3-dev gcc git
USER airflow
RUN pip install --no-cache-dir \
    "protobuf==3.20.3" \
    "dbt-core==1.5.0" \
    "dbt-postgres==1.5.0" \
    "apache-airflow-providers-snowflake==5.3.0"  