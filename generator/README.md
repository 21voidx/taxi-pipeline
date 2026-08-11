# Ride Generator

Synthetic operational generator for the ride-hailing portfolio. It supports:

- deterministic reference/master seeding;
- historical bootstrap with timestamps distributed across each business day;
- realistic ride lifecycle intervals in Jakarta business time, stored as UTC;
- realtime ride creation spread across each generator interval;
- configurable customer, driver, historical, and realtime volumes;
- payment creation and settlement;
- a temporary CRUD demonstration.

## Configuration sources

The generator reads defaults from `.env`. Bootstrap CLI arguments override `.env`:

```bash
docker compose run --rm generator python -m ride_generator.cli bootstrap \
  --days 90 \
  --rides-per-day 2000 \
  --customers 20000 \
  --drivers 2000
```

Relevant variables:

```env
GENERATOR_SEED=42
GENERATOR_BOOTSTRAP_DAYS=90
GENERATOR_BOOTSTRAP_RIDES_PER_DAY=2000
GENERATOR_CUSTOMER_COUNT=20000
GENERATOR_DRIVER_COUNT=2000
GENERATOR_INTERVAL_SECONDS=15
GENERATOR_RIDES_PER_TICK=5
```

## Ready-to-use profiles

- `.env.portfolio-standard.example`: about 180,000 historical rides plus multipliers.
- `.env.large.example`: about 900,000 historical rides plus multipliers.
- `.env.example`: identical to the Portfolio Standard profile.

Select a profile before starting containers:

```bash
cp .env.portfolio-standard.example .env
# or
cp .env.large.example .env
```

Changing `.env` does not require rebuilding the image. Recreate the realtime
container so it receives the new environment:

```bash
docker compose up -d --force-recreate generator
```

## Bootstrap versus realtime

Bootstrap is a one-time historical load:

```bash
docker compose run --rm generator python -m ride_generator.cli bootstrap
```

Realtime is a persistent service:

```bash
docker compose up -d generator
```

Do not run two realtime generator instances at the same time.

## Timestamp conventions

- PostgreSQL columns use `TIMESTAMP` and store naive UTC values.
- Business-pattern rules are evaluated in `Asia/Jakarta`.
- `created_at` represents entity/event creation.
- `updated_at` represents the latest source event-time.
- Historical request timestamps are unique to the second, therefore
  `GENERATOR_BOOTSTRAP_RIDES_PER_DAY` must remain below 86,400.

## Large-profile note

The current implementation performs many row-level inserts. The Large profile is
appropriate for a heavier portfolio test, but multi-million-row generation should
be migrated to PostgreSQL `COPY` or batched inserts.

## Validation

```bash
docker compose exec -T postgres-source \
  psql -U ride_user -d ride_hailing \
  < scripts/validate_generator_timestamps.sql
```
