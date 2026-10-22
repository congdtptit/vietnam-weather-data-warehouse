# Business Implementation Roadmap for AI

Date: 2026-10-10. Read with [business requirements](business-requirements.md) and [AGENTS.md](../AGENTS.md). This is an implementation plan aligned with the workbook, not authorization to implement the entire project. Each turn covers only the task and scope assigned by the user.

## 1. Selecting work and establishing the baseline

1. Read the current request, [progress](progress.md), the relevant workbook task ID, and the affected domain contracts.
2. Identify inputs/outputs, dependencies, source/location/time scope, and acceptance criteria. Read scoped AGENTS instructions before editing a directory.
3. Implement independent work within the assigned scope. If an open decision affects data, document options and obtain the missing decision before dependent implementation.
4. Write code and documentation in English. Preserve `scripts/`, `dags/`, and `sql/`. Update producers and consumers together when changing a contract; migrate existing databases.
5. Run checks appropriate to the changed behavior and update progress before reporting results. Do not expand a code/documentation task into historical downloads, volume resets, or dashboard publication.

The base includes the repository, Docker runtime, locked dependencies, initial DDL, bucket initialization, Open-Meteo/example DAGs, tooling, and 21 passing local fixture tests at the time (145 unit and 18 integration tests on 2026-10-22). Since then the development stack, real data, and Airflow runs were verified on one local machine, which is the whole team's machine since the user works alone; GitHub CI runs remain unverified. Read the limits in progress; later-stage work does not require rebuilding the base.

**Week 1 reconciliation:** the workbook also requires ID 4 real GeoJSON, ID 5 CDS/ERA5 requests, ID 6 an agreed Silver contract, and ID 7 an agreed label contract. Status 2026-10-22: IDs 6 and 7 are complete; IDs 1–4 have their evidence in [progress](progress.md) and await the owner's acceptance; ID 5 has its first request done and the full download in progress. The technical base and all week 1 deliverables have separate acceptance scopes.

## 2. Stages and dependencies

| Stage | Workbook weeks | IDs | Prerequisites | Outputs and checks before advancing |
| --- | --- | --- | --- | --- |
| A. Base and contracts | 1 | 1–7 | Repository and proposal sources | Reproducible setup, draft schema/grain, buckets/naming, GeoJSON/ERA5 evidence, and recorded approved decisions |
| B. Ingestion/Silver/dimensions | 2–3 | 8–15 | A and source metadata | Small-window checks per adapter: UTC, units, missing/duplicate records, boundaries, lineage, and reruns; then expand only within assigned scope |
| C. Gold/DW/initial BI and ML dataset | 4–5 | 16–23 | Silver/dimensions and relevant decisions | Separate Gold observations from ML; load facts with correct keys, DQ and reconciliation; prevent label/feature leakage |
| D. Operations, complete BI, and CART | 6–7 | 24–30 | C, contracts, and data covering the required scope | DQ tasks, incremental watermarks, actual benchmarks, OLAP interactions, train/validation; freeze model/threshold before test |
| E. Prediction integration | 8 | 31–33 | Frozen model/feature contracts and DW migration | Duplicate-free prediction reruns, explicit origin/target, scoring task, and dashboard ground-truth reconciliation |
| F. Integration verification | 9 | 34–36 | Integrated components | Clean-machine setup, defined end-to-end scope, BI/SQL reconciliation, and error analysis; no threshold reselection using test |
| G. Handover | 10 | 37–40 | Evidence from A–F | Report, slides, demo, and nine PDF deliverables with scope/limits and reproduction instructions |

N1/N2/N3 workstreams may proceed independently when dependencies are satisfied. Do not change workbook assignments or scheduling. Basic DQ checks belong in each ETL step from the start. ID 24 integrates operational quality gates; it is not a reason to omit Silver validation.

## 3. Mapping all 40 tasks to acceptance evidence

IDs/weeks below are a snapshot of `Theo dõi công việc`; Excel row = ID + 4. The workbook owns owners/support/status. Acceptance evidence is supplementary technical guidance, not an automatic completion mark or a change to the original output scope.

| ID | Weeks | Business task | Required acceptance evidence |
| --- | --- | --- | --- |
| 1 | 1 | Docker Compose and Git | Clone/setup instructions; stack runs on team machines; init/webserver/scheduler/DB/MinIO status and connectivity; isolated builds/tests are insufficient |
| 2 | 1 | Buckets and naming | Three actual buckets, correctly named sample keys, separate bucket/key semantics, and raw/provenance handling |
| 3 | 1 | Draft Star Schema | Dim/fact diagram and grain, keys/relationships, and explicit unresolved calendar/station decisions |
| 4 | 1 | Boundary GeoJSON | Real Bronze file with source/license/version/effective date/CRS, valid geometries, and point mapping |
| 5 | 1 | CDS and ERA5 request | Submitted request evidence, area/variables/product/window; credentials outside Git; no completed-download claim without evidence |
| 6 | 1 | Agree Silver contract | Approved columns/units/UTC, contract version and decision confirmation; separate unresolved decisions |
| 7 | 1 | Agree RainNext24h | Source, mm threshold, interval semantics, fallback, and coverage recorded in the label contract |
| 8 | 2–3 | Open-Meteo → Bronze | Raw files/manifests, failure/retry/resume, full 2016–2025 × 8-point coverage for full-task acceptance; distinguish samples |
| 9 | 2–3 | Meteostat or NOAA → Bronze | Station metadata, raw files, covered/missing windows and lineage; explicit source choice |
| 10 | 2–3 | API/stations → Silver | Standard columns/UTC/units, null/duplicate/gap reports, reruns and boundary samples; approved handling policies |
| 11 | 2–3 | DimLocation and DimStation | Old/new province mapping, point/station distances, source namespaces, selection rules, and cardinality |
| 12 | 2–3 | DimDate/Time/Source | Seeds covering the loading scope, calendar/weekday basis, stable source lookups, and seed reruns |
| 13 | 2–3 | Star Schema DDL | DDL/migrations and PK/FK/unique/grain checks; distinguish schema presence from completed migration |
| 14 | 2–3 | ERA5 NetCDF → Bronze | Raw files, request/product metadata, dimensions/coordinates/time/units/coverage; xarray reads real files |
| 15 | 2–3 | ERA5 → Silver | Metadata-confirmed conversions, grid selection/distance, accumulation/reset/gap cases, and UTC |
| 16 | 4–5 | Silver → Gold/ML pivot | Separate observation and ML contracts, preserved source identity, pivot cardinality, and lineage |
| 17 | 4–5 | Bronze → DW DAG | Task behavior with explicit windows, dependency/retry/publication, failure/resume, and reruns; parsing alone is insufficient |
| 18 | 4–5 | Observation fact loading | Dimension lookups, transactional upserts, before/after rerun key/row counts, and full 10-year coverage report |
| 19 | 4–5 | OLAP views | Hour/day/month/year rollups with source/location/calendar/coverage; reconciled totals without fan-out |
| 20 | 4–5 | Initial Power BI dashboard | Real PBIX, relationships/measures, refresh checks, and SQL reconciliation under identical filters |
| 21 | 4–5 | EDA | Reproducible notebook, season/location/source distributions, nulls and support; no feature selection using test |
| 22 | 4–5 | Label generation | t+1/t+24/t+25 cases, missing hours/end-of-series/split boundaries, label provenance, and coverage |
| 23 | 4–5 | Feature generation | Per-location lag/rolling using information available at t; changes after t do not change features at t |
| 24 | 6–7 | Airflow DQ task | Row/key counts, missing/range reports, severity/gates; failed prerequisites cannot become PASS |
| 25 | 6–7 | Incremental DAG | Data intervals/windows, watermarks after commit, late-data lookback, and duplicate-free overlapping reruns |
| 26 | 6–7 | DW indexing/performance | Actual queries/workloads/EXPLAIN/latency before and after within a defined scope; agree latency targets |
| 27 | 6–7 | Complete dashboard | Maps/source/region comparisons and four working OLAP operations; units/coverage and correct province mapping |
| 28 | 6–7 | Train/tune CART | Fit on 2016–2023; evaluate/select configurations using 2024; save preprocessing/features/artifacts/seed |
| 29 | 6–7 | Test evaluation on 2025 | Model/threshold frozen from validation; TP/FP/TN/FN, Accuracy/Precision/Recall/F1, and coverage |
| 30 | 6–7 | Decision tree and rules | Figures/rules from the evaluated artifact, feature names/units, and explanatory examples |
| 31 | 8 | Prediction DW and DimModel | Migration, origin/window/model/label/threshold, probability/class, FK/unique checks, and reruns |
| 32 | 8 | Scoring DAG | Feature availability, artifact version, window, failure status, and prediction publication |
| 33 | 8 | Forecast/actual BI page | Match origin/location/target/version; missing ground truth is neither No nor an incorrect forecast |
| 34 | 9 | Clean-machine/end-to-end verification | Reproduction commands, runtime versions, scope/run ID, key/row/object counts, logs, reruns, and failure checks |
| 35 | 9 | BI ↔ SQL reconciliation | Identical source/window/calendar/filters for daily/monthly/totals; explain discrepancies |
| 36 | 9 | Error/threshold analysis | Breakdowns with support and leakage notes; select thresholds on validation and disclose test post-analysis |
| 37 | 10 | Source/Data Lake/ETL report | Actual code/data/run evidence, source/coverage, and limitations |
| 38 | 10 | DW/OLAP/BI report | Schema, loaders, queries/measures, and reconciled demo screenshots |
| 39 | 10 | ML/evaluation report | Labels/features/splits/artifacts, metrics/baselines/threshold, and error interpretation |
| 40 | 10 | Shared report/slides/demo | Nine PDF section 15 deliverables, a bounded demo script, reproduction instructions, and limitations |

## 4. Sizing each implementation turn

Split a large ID into turns with explicit inputs/outputs and checks. For ID 8, for example: failure status → raw/manifests → retry/resume/coverage tests → an assigned real-data window → backfill within the assigned scope. Smaller turns do not change the ID/owner or the full-task output.

Prioritize a small-window Open-Meteo → Bronze → Silver → Gold observations → DW slice after its dependent contracts are agreed. This reduces integration risk without changing workbook scheduling/ownership. Assigned team members may prepare ERA5/GeoJSON/station work independently. This plan does not authorize AI to spawn agents or start historical downloads.

Fixtures support edge-case tests. Real-source acceptance requires explicit requests/windows and coverage evidence. Do not use the CLI default 2016–2025 range as validation. Without an assigned real-data scope, use offline fixtures and report real-data verification as pending.

## 5. Recording progress after each coding task

After coding and appropriate checks, **update `docs/progress.md` in the same turn before finishing**, including failed checks and partial completion. Record actual verification level: authored, fixture-verified, real-window-verified, full-scope-verified, or unverified. Do not derive completion percentages from file counts or test counts.

Minimum entry template:

```markdown
## [Task] — YYYY-MM-DD

- Related work: workbook ID, requirement/contract source, and version.
- Objective/scope: assigned scope and completed portion.
- Changes: files, producers/consumers, migrations, or artifacts.
- Checks: actual commands, environment/runtime, PASS/FAIL/warnings.
- Data: synthetic or real, source/location, [start, end) UTC,
  before/after row/key/object counts, coverage, and rerun evidence.
- Evidence: logs/reports/artifacts and commit if available.
- Limits: checks not run, remaining errors/decisions, downstream effects.
- Next step: a bounded action; whether the full ID or only a portion is accepted.
```

Update domain documents and affected consumers in the same turn when changing a contract. `progress.md` records technical evidence; the workbook owns assignments/support/status/percentages and scheduling. Update workbook status only within an authorized workbook-update scope and with sufficient evidence. This documentation task does not edit the workbook. Completion statements must agree with the limits recorded in progress.

## 6. Task prompt template for AI

```text
Implement workbook ID [ID], increment [specific bounded work].
Read AGENTS.md, docs/business-requirements.md, docs/implementation-roadmap.md,
docs/progress.md, relevant domain contracts, and scoped AGENTS instructions.
Scope: [source/location/[start, end) UTC; synthetic or real data].
Output: [specific code/artifact]. Acceptance: [specific checks].
Write code and documentation in English; preserve repository layout and stable IDs.
Do not approve open decisions or expand data-processing scope autonomously.
After coding/testing, update docs/progress.md with evidence and limitations,
then report changed files, actual checks, and remaining work.
```
