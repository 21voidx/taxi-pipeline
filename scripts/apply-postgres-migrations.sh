#!/usr/bin/env bash
set -euo pipefail

DEPLOY_ROOT="${1:?usage: apply-postgres-migrations.sh /opt/data-platform-ENV}"
MIGRATIONS_DIR="$DEPLOY_ROOT/postgres-source/migrations"

cd "$DEPLOY_ROOT"

if [ ! -d "$MIGRATIONS_DIR" ]; then
  echo "No migrations directory; nothing to apply."
  exit 0
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

compose() {
  docker compose --env-file .env -f compose.yaml "$@"
}

compose exec -T postgres-source psql \
  -U "$POSTGRES_SOURCE_USER" \
  -d "$POSTGRES_SOURCE_DB" \
  -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE IF NOT EXISTS public._schema_migrations (
  filename text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
SQL

shopt -s nullglob
files=("$MIGRATIONS_DIR"/*.sql)

if [ ${#files[@]} -eq 0 ]; then
  echo "No PostgreSQL migrations to apply."
  exit 0
fi

for file in "${files[@]}"; do
  filename="$(basename "$file")"
  if [[ ! "$filename" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "Invalid migration filename: $filename" >&2
    exit 1
  fi

  applied="$(compose exec -T postgres-source psql \
    -U "$POSTGRES_SOURCE_USER" \
    -d "$POSTGRES_SOURCE_DB" \
    -tAc "SELECT 1 FROM public._schema_migrations WHERE filename = '$filename'" \
    | tr -d '[:space:]')"

  if [ "$applied" = "1" ]; then
    echo "SKIP: $filename"
    continue
  fi

  echo "APPLY: $filename"
  {
    echo "BEGIN;"
    cat "$file"
    printf "\nINSERT INTO public._schema_migrations(filename) VALUES ('%s');\n" "$filename"
    echo "COMMIT;"
  } | compose exec -T postgres-source psql \
        -U "$POSTGRES_SOURCE_USER" \
        -d "$POSTGRES_SOURCE_DB" \
        -v ON_ERROR_STOP=1

done
