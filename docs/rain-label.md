# RainNext24h and Evaluation Methodology

Version v0.2, 2026-10-21 (label decisions confirmed by the project member working on N1/N2 on 2026-10-21; that member accepted their confirmation as team approval). Original draft v0.1: 2026-10-08. The proposal requires CART classification of rain/no-rain within the next 24 hours. This document records established decisions and items requiring team agreement. Label generation and split purging are implemented in `scripts/rain_labels.py` (tests: `tests/test_rain_labels.py`) and exported to Gold ML by `scripts/export_rain_labels.py` (`gold/ml_labels/rain_next24h_labels_v1.parquet` with a JSON manifest); no feature, model, or DW prediction implementation exists yet.

## Decisions and status

| Item | Status |
| --- | --- |
| CART, RainNext24h Yes/No classes | Proposal requirement |
| Train 2016–2023, validation 2024, test 2025 | Proposal requirement |
| Emphasize Recall; report Confusion Matrix/Accuracy/Precision/Recall/F1 | Proposal requirement |
| Choose probability threshold on validation and freeze before test | Methodological rule preventing test-based model selection |
| Rainfall label threshold | Decided 2026-10-21: **1 mm** summed over `(t, t+24h]`. Measured on Open-Meteo 2016–2025 (8 locations, 701,184 labelled hours): positive rate 50.5% overall; train 2016–2023 50.9%, validation 2024 49.3%, test 2025 49.0%; yearly location means 42.0–56.5%. At 0.1 mm the overall rate is 69.5% (up to 91.4% for one location-year), which makes an always-Yes baseline trivially strong. LCA is the wettest location (67.7% at 1 mm). Strong seasonality: monthly rate 25% (December) to 78% (August). The threshold is a parameter; changing it means regenerating labels only |
| Label source | Decided 2026-10-21: Open-Meteo for the first experiment (complete coverage for 8 locations × 2016–2025). Label is gridded reanalysis rainfall, not gauge observation; Open-Meteo is largely ERA5-derived, so the experiment is retrospective. Station-first with ERA5 fallback (workbook proposal) stays open as a later extension; ERA5 fallback is not decided |
| Whole-window versus hourly source selection | Moot for the first experiment (single source per label); revisit with any multi-source label |
| Missing-label handling | Decided 2026-10-21: a window missing any of its 24 hourly values has no label and is excluded from evaluation; missing is not No. Minimum coverage beyond this stays open |
| Calendar basis | `date_key`/`time_key` use UTC+7 (see data contract); labels, forecast origins, and splits use UTC |
| Rain timestamp convention | Verified 2026-10-21 from the Open-Meteo Historical Weather API documentation: hourly `precipitation` is the "sum of the preceding hour". The value at timestamp h covers `(h-1h, h]`, so `(t, t+24h]` uses the values timestamped t+1h through t+24h |
| Recall versus Precision/false-alarm tradeoff | Quantitative targets unresolved |

The rainfall threshold in mm defines ground truth. The probability threshold defines predicted classes. They are not interchangeable.

## Timing and labels

- Forecast origin `t` is when information is used to forecast, not necessarily job execution time.
- For hourly rain timestamped at interval end, target `(t, t+24h]` consists of t+1h through t+24h. For interval-start timestamps, convert/interpret source intervals before calculation; do not blindly reuse identical `shift` logic.
- Rain labels indicate target rainfall reaching the agreed mm threshold. Retain target start/end, source/provenance, label version, and valid-hour count.
- Proposal: missing required hours make labels unavailable and exclude them from evaluation; missing is not No. An end-of-series window shorter than 24 hours is invalid. Any alternative policy requires an explicit decision.
- Calculate separately per location on a gap-checked hourly grid. `shift(-24)` is not necessarily 24 hours ahead when hours are missing.

## Features and leakage prevention

- Use only information available at or before t: lags, trailing rolling statistics, 3h/24h pressure changes, and hour/month. Account for publication latency; measurement timestamps do not prove availability.
- No centered rolling, future shifts, bfill, or future-informed interpolation. Fit imputation/preprocessing/feature selection on train; validation/test only transform.
- Lag/rolling calculations must not cross locations. Fix source pivot/fusion rules; do not use test to select sources or features.
- If only final ERA5 reanalysis is available, report a retrospective experiment, not a verified real-time forecast. Check feature availability before operational use.

## Splits and threshold selection

- Split by UTC forecast origin: train `[2016-01-01, 2024-01-01)`, validation `[2024-01-01, 2025-01-01)`, test `[2025-01-01, 2026-01-01)`. Do not randomly shuffle.
- Purge train/validation samples whose targets use any rain interval belonging to the next split. For interval-end timestamps, an hour exactly at the next split boundary must also be excluded from previous-split labels under the selected convention. Past history may support subsequent-split features; test targets require complete ground truth.
- Tune `max_depth`, `min_samples_leaf`, `ccp_alpha`, `class_weight`, and probability threshold using train/validation; pruning paths use train only. Do not resample before splitting.
- Emphasize Recall while reporting Precision/F1, support, and false alarms; all-Yes is not automatically a good model. Compare an always-No baseline and other suitable baselines.
- Freeze artifacts/features/threshold/label contract before test evaluation. If test results change the design, disclose that test participated in development.
- Workbook ID 29 mentions threshold adjustment alongside test 2025. Choose the threshold on validation 2024 and reserve test for evaluation. The workbook was not edited during documentation organization.
- ID 36 (week 9) requests error analysis and threshold comparisons: threshold selection must still use validation. Disclose post-analysis on test; if test results change the design, do not claim independent test evaluation. See [business reconciliation](business-requirements.md).

## Artifacts and acceptance

Save model/preprocessing, ordered features/units, model/label versions, source policy, training window, threshold, seed, and dependency versions. Predictions retain UTC origin, target window, location, probability, and class; design loading keys according to [warehouse schema](warehouse-schema.md).

Report TP/FP/TN/FN with Rain as the positive class; Accuracy/Precision/Recall/F1, support, coverage, and threshold. Explain undefined metrics. Test labels at t, t+1h, t+24h, t+25h, gaps, and year boundaries; changing data after t must not change features at t.
