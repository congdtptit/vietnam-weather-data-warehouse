---
name: powerbi-weather-analytics
description: "Design, fix, or review Power BI weather models, relationships, DAX, OLAP, and trend/forecast dashboards backed by a weather DW. Use for semantic models, measures, maps, and BI reconciliation, not general web interfaces or fabricated PBIX files."
---

# Power BI Weather Analytics

## Inspect the model first

Read `docs/warehouse-schema.md`, relevant `docs/data-contract.md` definitions, existing DDL/OLAP views, and actual BI artifacts. For forecast pages, read `docs/rain-label.md`. Paths are relative to the repository root. Check tool versions/capabilities; consult official Microsoft documentation when verifying DAX, connectors, or Power BI operations.

Domain documents own project definitions and pending decisions. Use `docs/progress.md` to distinguish implemented views/models from targets; do not invent artifacts or calendar/source policies.

If the environment cannot create/edit/test PBIX, deliver DAX, relationship mappings, queries, and verifiable implementation instructions. State what remains unverified in Power BI Desktop; do not rename other files to `.pbix` or claim the dashboard ran.

## Relationships and calendars

- Star Schema, unique dimension keys, 1:* dimension-to-fact relationships, single-direction filtering by default. Use many-to-many/bidirectional filtering only with a justified business case and ambiguity checks.
- Observations/predictions share appropriate dimensions. Avoid direct hourly fact-to-fact joins causing fan-out; reconcile through measures/views at defined common grain.
- Establish UTC or `Asia/Ho_Chi_Minh` for charts/rainy days. Use a continuous Date table, mark it when applicable, sort month names by month number; distinguish observation dates, forecast origins, target windows.
- Multiple prediction date roles need role-playing dimensions or inactive relationships with explicit measures. One `target_date_key` cannot describe every rolling 24h window spanning two days; show origin and target start/end.
- Geography uses stable location codes/coordinates, versioned old/new province names, and GeoJSON. Do not assume old names are current provinces or geocode ambiguous province names.

## Measures and aggregation

- Explicit measures with units in labels/formats; do not use default SUM for all columns. Follow `weather-dw-design` aggregation rules.
- Rain: total nonoverlapping intervals for the same source/location. For multiple sources, compare separate series or use an agreed reference source; never present summed-source grand totals as actual rain.
- Temperature/humidity/pressure/wind speed: appropriate mean/min/max and valid counts. Roll up raw data or sums/counts, not averages of averages with unequal coverage. Wind direction uses circular/vector aggregation.
- Rainy days: flags at source × location × day grain using agreed mm threshold/minimum coverage, then count. Distinguish location-days from calendar days for multiple locations.
- BLANK differs from zero; do not conceal missing data with `COALESCE(...,0)`. Display coverage/sample size when it changes interpretation.
- DAX measures document intended filter context, source policy, and total behavior. Check single/multiple locations, multiple sources, no data, totals. Do not let `SELECTEDVALUE` make multi-selection meaningless.

## Dashboards and OLAP

- Trends: temperature/rain over time, seasons, rainy/hot days with defined thresholds.
- Comparisons: separate sources/regions; tooltips show units, coverage, provenance.
- Maps: locations, versioned boundaries, legends, units; verify points against reporting boundaries.
- Forecasts: origin, 24h target, probability, class threshold, model/label version, actual rain, incomplete-ground-truth status. Do not mark forecasts wrong before the target ends.
- Roll-up hour → day → month → year; Drill-down reverses it; Slice filters source/time/location; Dice combines dimensions. Demonstrations must show filters changing data.
- Configure interactions so slicers neither accidentally combine sources nor apply observation calendars to wrong prediction date roles. Distinguishable colors/labels; rain/no-rain must not rely only on color.

## Forecasts versus actuals

Match location, forecast origin, target window, label version, and agreed ground-truth source. Use complete labels only. Confusion counts use a defined forecast population without observation-join multiplication. Precision/Recall/F1 derive from TP/FP/FN in the filtered population, not averages of subgroup F1. Return explained BLANK/n.a. for denominators with no samples.

## Verification and delivery

Reconcile measures with SQL using identical filters/calendars/sources for a day, month, totals. Check relationships, slicers, drill hierarchies, missing data, prediction date roles, refresh. Deliver actual artifacts, authored DAX/queries, implementation steps, and observed tests; do not publish to a workspace for a design-only request.
