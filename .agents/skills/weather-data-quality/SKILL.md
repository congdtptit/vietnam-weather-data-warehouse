---
name: weather-data-quality
description: "Build or analyze weather data quality checks for missing/duplicate hours, keys, units, valid ranges, temporal continuity, and cross-source reconciliation in Bronze/Silver/Gold/DW. Use for Airflow DQ tasks or discrepancy investigations; inspection alone does not authorize repair or imputation."
---

# Weather Data Quality

## Define scope

Read `docs/data-contract.md`, scoped AGENTS instructions, relevant producers/consumers, and schemas relative to the repository root. Read `docs/NAMING_CONVENTIONS.md` for object discovery and `docs/rain-label.md` for feature/label checks. Reports identify layer, source/product, location/station, `[start, end)` window, timezone, version, and run ID. Read `supabase-postgres-best-practices` if available when writing SQL checks.

The data contract owns project thresholds, coverage rules, and decision status. Do not replace pending decisions with universal thresholds or mistake synthetic samples for production data; check `docs/progress.md` for implementation evidence.

Target keys are source × location × UTC hour. For raw station-grain data, use that layer's actual keys. Inspection does not automatically authorize deduplication, interpolation, or repairs.

## Checks

1. **Structure:** columns/types/units/schema version, timezone-aware timestamps, valid source/location keys, precipitation/radiation interval metadata. Check foreign keys and lookup cardinality entering the DW.
2. **Duplicates:** count duplicate business keys before/after joins; distinguish identical payloads from conflicting values. Report groups, excess rows, and sample keys; do not just `drop_duplicates()` and report PASS.
3. **Missing hours:** build expected hourly grids per source/location from research scope and declared actual coverage. Anti-join for gaps; distinguish absent records from records with null measurements. Calculate longest gaps and missing rates by variable/month/source.
4. **Continuity:** out-of-window/off-hour/unsorted timestamps, spacing other than 1h, timezone errors, leap days, month/year boundaries, accumulation resets. Do not assume 24 × 365 for every year.
5. **Valid ranges:** humidity/cloud percent [0,100], nonnegative rain/speed, probability [0,1], valid lat/lon for the CRS. Wind direction follows agreed [0,360) or [0,360]; calm wind may have null direction.
6. **Plausibility:** configure temperature/pressure/extreme-rain/abrupt-change thresholds by source, location, altitude, season, and units. Distinguish impossible values from outlier warnings; do not clip extremes merely because they differ from usual distributions.
7. **Layer reconciliation:** row/key counts, timestamp ranges, uniqueness, null rates, and rain aggregates over the same window. Filtering/resampling/deduplication must explain count changes; equal counts do not establish equal data.

Missing-rate denominators use defined expected coverage. Limited-history stations do not automatically FAIL for lacking ten years, but disclose coverage against research scope. Hours not yet due for publication are pending, not healthy zeros.

## Cross-source reconciliation

- Match UTC instant/interval, equivalent variables/units, and coordinates or documented distance mappings. Retain station/grid distances and pressure type.
- Compute bias/MAE/RMSE/correlation only on valid pairs; report pair count, coverage, window. No pairs must not become zero discrepancy or PASS.
- Station rain and reanalysis can differ. Investigate disagreements; do not automatically overwrite one source with another.
- Historical Open-Meteo may use reanalysis related to ERA5. Check product metadata before treating agreement as independent confirmation.
- Do not compare UTC daily rain with Vietnam-local daily rain or sum sources to reconcile totals.

## Quality gates and remediation

- Classify ERROR/WARN/INFO by consumer impact and agreed thresholds. Key/grain/schema/timestamp errors corrupting target data should block affected publication; outliers may be warnings under the contract.
- Machine-readable reports include check ID, severity, actual/expected, numerator/denominator, threshold, sample keys, and PASS/FAIL/NOT_EVALUATED. Expose missing inputs and failed checks; do not catch exceptions and return zero issues.
- Quarantine invalid rows with provenance; retain Bronze for reprocessing. Repair through the requested pipeline, not silent changes to originals.
- Interpolate only when permitted, with gap limits and imputation flags. For ML, no bidirectional interpolation/future data; missing rain labels must not be filled with zero.

## Verification

For changed checks, test conflicting duplicates, missing hours, null records, leap days, UTC/local boundaries, and wrong units. A failed prerequisite must still block affected publication when other checks pass. Deliver reports/coverage, separating confirmed errors from discrepancies needing investigation.
