# Project instructions

## Scope and architecture

This repository implements a northern Vietnam weather Data Lake, PostgreSQL DW, Power BI analytics, and CART RainNext24h classification. Keep the existing `scripts/`, `dags/`, and `sql/` layout. Do not reorganize code or change infrastructure merely to match a documentation example.

Canonical timestamps are timezone-aware UTC. Observation grain is source × stable location × UTC hour. Project-specific definitions and their decision status belong in the documents below; skills describe reusable procedures.

## Read according to the task

- Setup or architecture: `README.md`, `docs/architecture.md`, and relevant configuration.
- ETL or data quality: `scripts/AGENTS.md`, `docs/data-contract.md`, `docs/NAMING_CONVENTIONS.md`.
- Airflow: `dags/AGENTS.md` and the contracts of affected producers/consumers.
- SQL or DW: `sql/AGENTS.md`, `docs/warehouse-schema.md`.
- Labels, features, models, or forecast evaluation: `docs/rain-label.md` and relevant data contracts.
- BI: `docs/warehouse-schema.md`, plus `docs/rain-label.md` for forecast pages.
- Progress reporting: `docs/progress.md` and the evidence supporting affected entries.

Read scoped instructions before editing files in their directory, even when starting from the repository root. Do not require a complete repository/documentation review for a small unrelated edit.

## Skills

Select relevant skills under `.agents/skills/`: `weather-etl-airflow`, `weather-dw-design`, `weather-data-quality`, `powerbi-weather-analytics`, and `cart-rain-forecast`. Read `supabase-postgres-best-practices` before SQL/schema changes or PostgreSQL data operations. Other installed skills apply only when their actual workflow matches the task.

## Decisions and changes

- Distinguish confirmed project requirements, documented conventions, implemented behavior, and pending proposals. Do not silently turn an open decision into an approved requirement.
- The proposal PDF defines objectives; the allocation workbook defines task ownership. Domain documents record implementation contracts. If they conflict with each other or with code, report the discrepancy and update affected documentation within the requested scope.
- Change producers and consumers together when modifying data contracts. Preserve raw provenance and stable identifiers; use migrations for existing databases.
- Skills and instructions do not authorize additional operations. Do not trigger full historical downloads, reset volumes, or publish dashboards merely to validate a documentation/code change.

## Verification and reporting

Use checks appropriate to the affected behavior. For Markdown/skill changes, validate links, frontmatter, metadata, and consistency without starting services. For Python, perform syntax checks and relevant behavioral tests. Test ETL/DW reruns on a small defined window when changing loading behavior; DAG syntax alone is not runtime validation.

Report changed files, checks actually run, data scope, and unresolved limits. Never infer full backfill, DW population, or model accuracy from file existence, upload samples, or connection checks. Update relevant `docs/progress.md` evidence after substantive work; keep task ownership in the existing workbook.
