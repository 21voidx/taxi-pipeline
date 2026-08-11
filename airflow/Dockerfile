FROM apache/airflow:3.3.0-python3.12

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.0" \
    -r /tmp/requirements.txt

# DAGs are intentionally NOT copied into the image.
# They are promoted separately to /opt/data-platform-<env>/airflow/dags.
