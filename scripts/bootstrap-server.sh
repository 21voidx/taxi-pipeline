#!/usr/bin/env bash
set -euo pipefail

RUNNER_USER="${1:-gha-runner}"

if [ "${EUID}" -ne 0 ]; then
  echo "Run as root: sudo bash scripts/bootstrap-server.sh ${RUNNER_USER}" >&2
  exit 1
fi

if ! id "$RUNNER_USER" >/dev/null 2>&1; then
  echo "User not found: $RUNNER_USER" >&2
  exit 1
fi

for env in development production; do
  root="/opt/data-platform-$env"
  mkdir -p \
    "$root/airflow/dags" \
    "$root/airflow/logs" \
    "$root/airflow/plugins" \
    "$root/dbt/project" \
    "$root/trino/catalog" \
    "$root/trino/etc" \
    "$root/postgres-source/init" \
    "$root/postgres-source/migrations"

  chown -R "$RUNNER_USER:$RUNNER_USER" "$root"
done

runner_uid="$(id -u "$RUNNER_USER")"
docker_gid="$(getent group docker | cut -d: -f3 || true)"

cat <<MSG
Bootstrap directories created.

Runner user : $RUNNER_USER
AIRFLOW_UID : $runner_uid
DOCKER_GID  : ${docker_gid:-NOT_FOUND}

Next:
1. Create /opt/data-platform-development/.env from deploy/.env.development.example
2. Create /opt/data-platform-production/.env from deploy/.env.production.example
3. Put AIRFLOW_UID=$runner_uid in both files.
4. Put DOCKER_GID=${docker_gid:-<docker-group-gid>} in both files.
5. chmod 600 both .env files and chown them to $RUNNER_USER.
6. Make sure rsync and docker compose are installed for the self-hosted runner.
MSG
