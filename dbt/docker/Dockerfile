FROM python:3.10-slim

RUN apt-get update && apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

COPY docker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt_profiles/ /root/.dbt/

WORKDIR /app

# ── Copy packages.yml DULU sebelum project ──────────────────
COPY dbt_project/packages.yml /app/packages.yml
COPY dbt_project/dbt_project.yml /app/dbt_project.yml

# Install deps — layer ini hanya re-run kalau packages.yml berubah
RUN dbt deps

# ── Copy sisa project SETELAH deps ──────────────────────────
# Layer ini sering berubah (edit SQL, YAML, dll) tapi tidak
# trigger dbt deps ulang
COPY dbt_project/ /app

ENV DBT_PROJECT_DIR=/app
ENV DBT_PROFILES_DIR=/root/.dbt

CMD ["dbt", "--version"]

## Perbandingan Build Time

# Tanpa optimasi — edit 1 file SQL:
#   COPY dbt_project/  → cache MISS (ada file berubah)
#   dbt deps           → re-run (~2-3 menit download packages)
#   Total: ~3 menit

# Dengan optimasi — edit 1 file SQL:
#   COPY packages.yml  → cache HIT (tidak berubah)
#   dbt deps           → cache HIT (skip!)
#   COPY dbt_project/  → cache MISS (ada file berubah)
#   Total: ~10 detik 
#   