FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y git

COPY docker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy dbt project
COPY dbt_project/ /app

# copy profiles (opsional: bisa juga di-mount)
COPY dbt_profiles/ /root/.dbt/

# ✅ set env dir
ENV DBT_PROJECT_DIR=/app
ENV DBT_PROFILES_DIR=/root/.dbt

# install deps
RUN dbt deps
