uv venv
source .venv/bin/activate
uv pip install dbt-bigquery

dbt init
dbt deps --project-dir . --profiles-dir .
dbt debug --project-dir . --profiles-dir .
dbt build --select tag:daily --project-dir . --profiles-dir . --target dev