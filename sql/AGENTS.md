# Warehouse instructions

Read `../docs/warehouse-schema.md`, relevant `../docs/data-contract.md` sections, and `supabase-postgres-best-practices` before writing SQL, changing schemas, or loading PostgreSQL data. Use `weather-dw-design` for domain rules.

- Inspect existing DDL and its consumers. Preserve source × location × UTC-hour observation grain; station multiplicity must be resolved before loading.
- Document calendar basis for date/time keys. Do not infer it from the synthetic Gold sample.
- `init_dwh_schema.sql` initializes new databases; `CREATE TABLE IF NOT EXISTS` does not migrate existing tables. Provide a separate migration and compatibility checks when needed; do not reset volumes to apply changes.
- Current initializer targets the literal database `weather_dwh`; a configurable `DWH_DB` does not establish that other names work.
- Load dimensions before facts, verify lookups and cardinality, and enforce uniqueness with transactions/constraints for idempotent loading.
- Keep sources separate in rain totals; distinguish zero from missing measurements and publish coverage with partial aggregates.
- Validate affected foreign keys, grain, reruns, and representative roll-ups. Report SQL authored separately from SQL executed; documentation review does not verify a live database.
