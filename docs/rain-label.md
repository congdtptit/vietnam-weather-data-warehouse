# RainNext24h and Evaluation Methodology

Version v0.2, 2026-10-21 (label decisions confirmed by the project member working on N1/N2 on 2026-10-21; that member accepted their confirmation as team approval). Original draft v0.1: 2026-10-08. The proposal requires CART classification of rain/no-rain within the next 24 hours. This document records established decisions and items requiring team agreement. Label generation and split purging are implemented in `scripts/rain_labels.py` (tests: `tests/test_rain_labels.py`) and exported to Gold ML by `scripts/export_rain_labels.py` (`gold/ml_labels/rain_next24h_labels_v1.parquet` with a JSON manifest). Exploratory analysis of the train split is in `notebooks/eda_rain_next24h.ipynb` (helpers: `scripts/rain_eda.py`). The feature dataset v1 is built by `scripts/build_rain_features.py`, and CART model v1 is trained and tuned on train/validation by `scripts/train_cart.py` (see below). Test evaluation (ID 29) and DW prediction loading are not implemented yet.

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

### Feature dataset v1 (implemented 2026-10-21)

- Source: Silver Open-Meteo hourly only (decided 2026-10-21; GHCNh station rain is kept for verification). Built by `scripts/build_rain_features.py` from `gold/ml_labels/rain_next24h_labels_v1.parquet`; tests in `tests/test_rain_features.py`.
- Rows: usable labels only, one per location × UTC origin, written per split to `gold/ml_features/cart_rain_{train_2016_2023,val_2024,test_2025}_v1.parquet` with the manifest `cart_rain_features_v1.json`. The manifest holds the ordered feature list with units; training must use that order.
- 22 numeric features, all computed per location on a gap-checked hourly grid from values stamped at or before t:
  - At t: `temperature_c`, `dew_point_c`, `dew_point_depression_c`, `humidity_percent`, `pressure_hpa` (surface), `cloud_cover_percent`, `wind_speed_ms`, `precipitation_mm` (hour ending at t).
  - Lags: `humidity_percent_lag3h`/`lag6h`, `cloud_cover_percent_lag3h`/`lag6h`.
  - Trailing windows `(t − n h, t]`: `rain_past_{3,6,12,24}h_mm`, `humidity_mean_6h_percent`, `cloud_cover_mean_6h_percent`.
  - Pressure tendency: `pressure_change_3h_hpa`, `pressure_change_24h_hpa`.
  - Calendar of the origin in UTC+7: `hour_local`, `month_local`.
- `location_code` is an identifier and a candidate categorical feature; encoding is decided in the training pipeline (ID 28).
- `rain_next24h_mm` is kept for error analysis only and is never a feature.
- A lag or window touching a missing hour is NaN; nothing is filled. In the 2016–2025 build, missing values occur only in the first 24 origin hours of 2016 (192 train rows). Imputation, if needed, is fitted on train in ID 28.
- Solar radiation and wind direction were left out because the train-split EDA showed no separation (solar radiation also mostly encodes the hour). Open-Meteo is reanalysis with publication delay, so results are retrospective.

## Splits and threshold selection

- Split by UTC forecast origin: train `[2016-01-01, 2024-01-01)`, validation `[2024-01-01, 2025-01-01)`, test `[2025-01-01, 2026-01-01)`. Do not randomly shuffle.
- Purge train/validation samples whose targets use any rain interval belonging to the next split. For interval-end timestamps, an hour exactly at the next split boundary must also be excluded from previous-split labels under the selected convention. Past history may support subsequent-split features; test targets require complete ground truth.
- Tune `max_depth`, `min_samples_leaf`, `ccp_alpha`, `class_weight`, and probability threshold using train/validation; pruning paths use train only. Do not resample before splitting.
- Emphasize Recall while reporting Precision/F1, support, and false alarms; all-Yes is not automatically a good model. Compare an always-No baseline and other suitable baselines.
- Freeze artifacts/features/threshold/label contract before test evaluation. If test results change the design, disclose that test participated in development.
- Workbook ID 29 mentions threshold adjustment alongside test 2025. Choose the threshold on validation 2024 and reserve test for evaluation. The workbook was not edited during documentation organization.
- ID 36 (week 9) requests error analysis and threshold comparisons: threshold selection must still use validation. Disclose post-analysis on test; if test results change the design, do not claim independent test evaluation. See [business reconciliation](business-requirements.md).

### CART model v1 (ID 28, trained 2026-10-21)

Procedure agreed on 2026-10-21 by the N1/N2 member doing N3's ML work:

- Inputs: the v1 train and validation feature files only; the test file is not read (`report.json` records `test_split_read: false`). Rows with a missing feature are dropped (192 train rows, the first 24 origin hours of 2016); nothing is imputed.
- Pipeline: the 22 numeric features unscaled plus one-hot `location_code`, then `DecisionTreeClassifier(random_state=42)`. Fit on train only; no refit on train + validation.
- Grid (120 fits): `max_depth` {4, 6, 8, 10, 12} × `min_samples_leaf` {50, 200, 1000} × `class_weight` {None, balanced} × `ccp_alpha` {0, 1e-5, 1e-4, 1e-3}.
- Selection: highest validation F1 of the Rain class at probability 0.5. Configurations within 0.005 F1 of the best count as tied, and the shallowest, then smallest, tree is chosen.
- Probability threshold: the value from 0.30 to 0.70 (step 0.02) with the highest validation F1 for the selected model; ties go to the value closest to 0.5.
- Baselines on validation: always No, always Yes, persistence (past 24 h rain ≥ 1 mm), and the train-period Rain rate by location × month (Yes when ≥ 0.5).
- Artifacts: `gold/ml_models/cart_rain_next24h_v1/model.joblib` (preprocessing + tree) and `report.json` (features and units in order, parameters, threshold, seed, dependency versions, grid results, validation metrics, baselines). Load the joblib file only from this bucket, because loading can execute code.

Result: `max_depth=8`, `min_samples_leaf=50`, `class_weight=balanced`, `ccp_alpha=1e-4`; 88 leaves; probability threshold 0.40 (frozen for ID 29). Validation 2024 has 70,080 rows, of which 49.4% are Rain:

| Predictor | Precision | Recall | F1 | Accuracy |
| --- | --- | --- | --- | --- |
| CART v1, threshold 0.40 | 0.752 | 0.822 | 0.786 | 0.778 |
| CART v1, threshold 0.50 | 0.787 | 0.775 | 0.781 | 0.785 |
| Persistence (past 24 h ≥ 1 mm) | 0.752 | 0.754 | 0.753 | 0.755 |
| Location × month Rain rate | 0.704 | 0.716 | 0.710 | 0.711 |
| Always Yes | 0.494 | 1.000 | 0.662 | 0.494 |
| Always No | undefined | 0.000 | undefined | 0.506 |

Validation chose both the configuration and the threshold, so these figures are optimistic; the independent estimate is the 2025 test evaluation (ID 29).

## Artifacts and acceptance

Save model/preprocessing, ordered features/units, model/label versions, source policy, training window, threshold, seed, and dependency versions. Predictions retain UTC origin, target window, location, probability, and class; design loading keys according to [warehouse schema](warehouse-schema.md).

Report TP/FP/TN/FN with Rain as the positive class; Accuracy/Precision/Recall/F1, support, coverage, and threshold. Explain undefined metrics. Test labels at t, t+1h, t+24h, t+25h, gaps, and year boundaries; changing data after t must not change features at t.
