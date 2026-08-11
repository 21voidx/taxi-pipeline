# CI/CD Flow — Development and Production

## 1. Architecture

```text
Laptop / Git
    |
    +-- feature branch from production
    |          |
    |          +--> PR -> development -> Development stack
    |          |                         |
    |          |                         +--> manual Airflow/dbt validation
    |          |
    |          +--> PR -> production  -> Production stack
    |
    +-- GitHub-hosted runner: CI/build
    |
    +-- GHCR: immutable custom images
    |
    +-- Self-hosted runner: deployment on home server
```

Runtime isolation:

```text
/opt/data-platform-development
  compose project: ride-hailing-development
  Airflow UI:      :8011
  Trino host port: :8091 (localhost by default)
  Source PG port:  :5434 (localhost by default)
  BigQuery raw:    dev_raw_ride_hailing
  dbt target:      dev -> dev_analytics_ride_hailing / dev_mart_ride_hailing

/opt/data-platform-production
  compose project: ride-hailing-production
  Airflow UI:      :8010
  Trino host port: :8090 (localhost by default)
  Source PG port:  :5433 (localhost by default)
  BigQuery raw:    prod_raw_ride_hailing
  dbt target:      prod -> prod_analytics_ride_hailing / prod_mart_ride_hailing
```

Airflow metadata PostgreSQL is not exposed on a host port.

## 2. Why one Compose file

`deploy/compose.yaml` contains the platform definition once. Each server directory has a different `.env` and `COMPOSE_PROJECT_NAME`, so Compose creates separate containers, networks, and volumes without duplicating infrastructure code.

Do not use fixed `container_name` values; Compose project isolation supplies unique names automatically.

## 3. What is an image and what is synced

Custom images:

- Airflow runtime: Airflow + providers/dependencies.
- dbt runtime: Python + dbt-core + dbt-bigquery.
- Generator: generator application code.

Not baked into images:

- Airflow DAGs — synced independently, so adding a DAG does not rebuild/restart Airflow.
- dbt project SQL/YAML/macros — synced independently, so model changes do not rebuild dbt.
- Trino configuration — mounted from the deployment directory.

Official images are used directly for PostgreSQL and Trino.

## 4. Workflow files

```text
.github/workflows/
  airflow-dags.yml
  airflow-runtime.yml
  dbt.yml
  data-generator.yml
  platform.yml
```

### airflow-dags.yml

PR: pulls the target environment Airflow runtime image and tests all DAG imports with `DagBag`.

Push to `development` or `production`: syncs `airflow/dags/` to the matching `/opt/data-platform-<env>/airflow/dags/`. No Compose restart.

### airflow-runtime.yml

Triggered only by Airflow Dockerfile/dependency changes. CI builds the image and validates current DAGs. Push publishes:

```text
:<commit-sha>
:development-runtime or :production-runtime
```

The deployment stores the immutable Airflow SHA image in the server `.env`, pulls it, runs Airflow initialization/migrations, and recreates Airflow services only.

### dbt.yml

A small change detector separates project changes from runtime changes:

- `dbt/project/**`: existing environment runtime image runs `dbt deps` + `dbt parse`; after merge the project is synced only.
- dbt Dockerfile/requirements: build/test/publish both an immutable SHA image and an environment-stable runtime tag; Airflow uses the stable tag with `force_pull=True`, so no Airflow restart is required.

### data-generator.yml

Builds and tests the generator. Push publishes an immutable image and recreates only the `generator` service.

### platform.yml

Validates Compose and deploys shared platform configuration. It also applies forward PostgreSQL migration files after the source database is healthy.

## 5. One-time home-server bootstrap

From a checkout of this repository on the home server:

```bash
sudo bash scripts/bootstrap-server.sh gha-runner
```

The script creates:

```text
/opt/data-platform-development/
  airflow/{dags,logs,plugins}/
  dbt/project/
  trino/{catalog,etc}/
  postgres-source/{init,migrations}/

/opt/data-platform-production/
  ...same structure...
```

It prints the real `AIRFLOW_UID` and Docker group GID.

Create the runtime env files manually:

```bash
sudo -u gha-runner cp deploy/.env.development.example /opt/data-platform-development/.env
sudo -u gha-runner cp deploy/.env.production.example  /opt/data-platform-production/.env
```

Edit both and replace:

- `YOUR_GITHUB_USER/YOUR_REPOSITORY`
- database passwords
- Airflow admin password
- Fernet key
- Airflow secret key
- API JWT secret
- `AIRFLOW_UID`
- `DOCKER_GID`

Generate secrets, for example:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
openssl rand -hex 32
openssl rand -hex 32
```

Then protect the files:

```bash
sudo chown gha-runner:gha-runner /opt/data-platform-development/.env /opt/data-platform-production/.env
sudo chmod 600 /opt/data-platform-development/.env /opt/data-platform-production/.env
```

The GCP service-account JSON stays outside Git, e.g.:

```text
/home/void/credentials/service-account.json
```

and is referenced through `GCP_CREDENTIALS_HOST_PATH`.

## 6. Branches and GitHub Environments

Create branches if they do not exist:

```bash
git checkout main
git pull
git checkout -b production
git push -u origin production
git checkout -b development
git push -u origin development
```

Set `production` as the default/source-of-truth branch.

Create GitHub Environments named exactly:

```text
development
production
```

Recommended repository controls:

- `production`: PR required, CI required, no direct push.
- GitHub Environment `production`: require manual approval before deployment.
- `development`: PR + CI, deployment can be automatic after merge.

## 7. Initial image bootstrap

Before the first platform start, the three custom environment runtime tags must exist.

For branch `development`, manually run from GitHub Actions:

1. Airflow Runtime CI/CD
2. dbt CI/CD
3. Data Generator CI/CD

Run the same three workflows on branch `production`.

During this first run, their deploy jobs may print that `compose.yaml` is not bootstrapped yet. That is expected: the images are still published to GHCR.

Then run `Platform CI/CD` once for `development` and once for `production`. It copies `compose.yaml` and config to the server and starts each isolated stack.

## 8. Seed historical source data once

The continuous generator starts real-time generation. If you want the historical bootstrap dataset first:

Development:

```bash
cd /opt/data-platform-development
docker compose --env-file .env -f compose.yaml --profile tools run --rm generator-bootstrap
```

Production:

```bash
cd /opt/data-platform-production
docker compose --env-file .env -f compose.yaml --profile tools run --rm generator-bootstrap
```

The environment values determine days, rides/day, customers, and drivers.

## 9. Day-to-day DAG workflow

```bash
git checkout production
git pull origin production
git checkout -b feature/new-ingestion-dag
```

Add/edit DAGs, push the feature branch, then PR to `development`.

After merge:

```text
CI DagBag
 -> sync /opt/data-platform-development/airflow/dags
 -> no image build
 -> no Airflow restart
```

Open the Development Airflow UI and test manually. New development DAGs are paused by default.

When safe, open a PR from the same feature branch to `production`. After merge, production DAGs are synced. New production DAGs are configured not to be paused at creation, so their schedule can continue normally.

## 10. Day-to-day dbt workflow

Changing only SQL/YAML/macros under `dbt/project/**`:

```text
PR -> dbt deps + dbt parse
merge development -> sync project
manual Airflow Dev execution -> dbt container -> BigQuery dev datasets
PR production -> sync production project
```

No dbt image rebuild is performed unless `dbt/Dockerfile` or `dbt/requirements.txt` changes.

## 11. PostgreSQL schema changes

`postgres-source/init/` is bootstrap-only. PostgreSQL's init directory is not re-run for an existing persistent volume.

For future source schema changes create forward migrations in:

```text
postgres-source/migrations/
```

Example:

```text
001_add_driver_rating_index.sql
002_add_new_ride_column.sql
```

`platform.yml` applies each filename once and records successful migrations in:

```text
public._schema_migrations
```

For production, the GitHub Environment approval is the deployment gate before the migration runs.

## 12. Operational checks

Development:

```bash
cd /opt/data-platform-development
docker compose --env-file .env -f compose.yaml ps
```

Production:

```bash
cd /opt/data-platform-production
docker compose --env-file .env -f compose.yaml ps
```

Version markers:

```text
DAG_VERSION
DBT_PROJECT_VERSION
AIRFLOW_RUNTIME_VERSION
DBT_RUNTIME_VERSION
GENERATOR_VERSION
```

`AIRFLOW_IMAGE_NAME` and `GENERATOR_IMAGE_NAME` are updated to immutable SHA tags. `DBT_IMAGE_NAME` intentionally remains the environment-stable runtime tag; `DBT_RUNTIME_VERSION` records the corresponding commit SHA.

## 13. Important migration note from the old stack

This refactor intentionally uses new Compose project names and therefore new named volumes. It does not delete or reuse old Docker volumes automatically.

That makes the new development and production stacks isolated and prevents an automated CI/CD migration from accidentally destroying your existing source/metadata data.

Inspect old volumes before deleting them:

```bash
docker volume ls
```

If old data must be preserved, export/import it deliberately before retiring the old Compose stack.

## 14. KISS boundary

This is intentionally a single-node, production-like platform. It does not add Kubernetes, Terraform, Vault, ArgoCD, multi-node Trino, or HA PostgreSQL.

The main production controls are instead:

- development/production isolation
- PR and environment promotion
- component-specific CI/CD
- immutable custom image tags
- no secret files in Git
- health checks and Compose wait
- forward database migrations
- persistent volumes
- explicit version markers

Add more infrastructure only when the workload actually requires it.
