FROM apache/airflow:3.4.0-python3.12

USER root
RUN mkdir -p /data/batch && chown -R airflow:0 /data
USER airflow

# The batch code gets its own Python environment. dbt and Airflow both pin
# many of the same libraries to different versions; keeping them apart means
# neither can break the other.
RUN python -m venv /home/airflow/batch-venv \
 && PIP_USER=false /home/airflow/batch-venv/bin/pip install --no-cache-dir \
      "dbt-duckdb>=1.11,<2" "confluent-kafka>=2.8,<3" "pytest>=8" "ruff>=0.8"

ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false
COPY --chown=airflow:0 . /opt/rivus
