# Ride-Hailing Data Platform

Single-repository, single-home-server data platform with two isolated environments:

- `development` — integration and manual validation.
- `production` — continuously scheduled trusted workloads.

The platform contains Airflow, Trino, PostgreSQL source data, a synthetic data generator, dbt, and BigQuery transformations.

## Branch model

`production` is the source of truth.

Day-to-day feature flow:

1. Create a feature branch from `production`.
2. Open PR `feature -> development`.
3. CI validates only the components that changed.
4. Merge to `development`; CI/CD deploys to the development stack.
5. Validate DAGs/dbt from the Airflow development UI.
6. Open PR from the same feature branch to `production`.
7. After review/CI, merge; CI/CD promotes the same code to production.

Do not routinely merge the whole `development` branch into `production`, because development may contain unrelated features that are still under test.

## Runtime layout

```text
/opt/data-platform-development/
/opt/data-platform-production/
```

Both use the same `deploy/compose.yaml`; `COMPOSE_PROJECT_NAME` and separate `.env` files isolate containers, networks and named volumes.

## CI/CD by component

| Change | CI/CD action | Restarts platform? |
|---|---|---|
| `airflow/dags/**` | DagBag test + sync DAGs | No |
| `airflow/Dockerfile`, `requirements.txt` | build/push Airflow image + recreate Airflow | Airflow only |
| `dbt/project/**` | `dbt deps` + `dbt parse` + sync project | No |
| `dbt/Dockerfile`, `requirements.txt` | build/push dbt image | No long-running service |
| `generator/**` | tests + build/push + recreate generator | Generator only |
| `trino/**` | validate + sync + Compose reconcile | Changed services only |
| `postgres-source/migrations/**` | apply forward migration once | No rebuild |
| `deploy/**` | validate Compose + reconcile stack | Only services whose config changed |

See `CI_CD_FLOW.md` for setup and operational commands.
