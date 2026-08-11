# PostgreSQL source migrations

`init/` is only used when the PostgreSQL volume is created for the first time.

For schema changes after the database already exists, add ordered SQL files here, for example:

- `001_add_driver_rating.sql`
- `002_add_payment_index.sql`

CI/CD applies each filename once and records it in `public._schema_migrations`.
Keep migrations forward-only and review production migrations before approval.
