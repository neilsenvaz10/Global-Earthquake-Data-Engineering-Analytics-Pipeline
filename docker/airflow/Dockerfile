FROM apache/airflow:3.3.2-python3.12
USER root
RUN python -m venv /opt/quaketrack-venv && chown -R airflow:0 /opt/quaketrack-venv
USER airflow
WORKDIR /workspace
COPY --chown=airflow:0 pyproject.toml ./
COPY --chown=airflow:0 src ./src
COPY --chown=airflow:0 sql ./sql
RUN /opt/quaketrack-venv/bin/pip install --no-cache-dir -e '.[dev]'
COPY --chown=airflow:0 docker/airflow/start.py /opt/airflow-start.py
WORKDIR /opt/airflow
CMD ["python", "/opt/airflow-start.py"]
