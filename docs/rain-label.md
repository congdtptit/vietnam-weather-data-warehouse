# RainNext24h and Evaluation Methodology

Version v0.2, 2026-10-21; implementation v0.2.1, 2026-10-22 (label decisions confirmed by the project member working on N1/N2 on 2026-10-21; that member accepted their confirmation as team approval). Original draft v0.1: 2026-10-08. The proposal requires CART classification of rain/no-rain within the next 24 hours. This document records established decisions and items requiring team agreement. Label generation and split purging are implemented in `scripts/rain_labels.py` (tests: `tests/test_rain_labels.py`) and exported to Gold ML by `scripts/export_rain_labels.py` (current: `gold/ml_labels/rain_next24h_labels_v2.parquet` with a JSON manifest, label version `rain_next24h_v0.2.1`). **Implementation fix v0.2.1 (2026-10-22):** v0.2 summed rain in floating point, so 24-hour totals of exactly 1.0 mm could come out as 0.99999999999999 and were labelled No. That affected 5,147 usable labels (0.73%: train 4,237, validation 474, test 436), all in that direction. v0.2.1 rounds rain sums to 3 decimals before comparing or storing; the definition is unchanged. Labels/features v1 are kept because model v1 was trained on them. Exploratory analysis of the train split is in `notebooks/eda_rain_next24h.ipynb` (helpers: `scripts/rain_eda.py`). The feature dataset (v1 for model v1; v2 with corrected labels for future models) is built by `scripts/build_rain_features.py`, and CART model v1 is trained and tuned on train/validation by `scripts/train_cart.py` (see below). The frozen model was evaluated once on test 2025 by `scripts/evaluate_cart_test.py` (ID 29) and re-scored against corrected labels by `scripts/rescore_cart_corrected_labels.py`. The tree figure and rules of that artifact are in `notebooks/cart_tree_rules.ipynb` (ID 30; helpers `scripts/cart_rules.py`). Test 2025 predictions are loaded into the DW by `scripts/load_predictions.py` (ID 31; see [warehouse schema](warehouse-schema.md)).

## Decisions and status

| Item | Status |
| --- | --- |
| CART, RainNext24h Yes/No classes | Proposal requirement |
| Train 2016–2023, validation 2024, test 2025 | Proposal requirement |
| Emphasize Recall; report Confusion Matrix/Accuracy/Precision/Recall/F1 | Proposal requirement |
| Choose probability threshold on validation and freeze before test | Methodological rule preventing test-based model selection |
| Rainfall label threshold | Decided 2026-10-21: **1 mm** summed over `(t, t+24h]`. Measured on Open-Meteo 2016–2025 (8 locations, 701,184 labelled hours): positive rate 50.5% overall; train 2016–2023 50.9%, validation 2024 49.3%, test 2025 49.0%; yearly location means 42.0–56.5%. At 0.1 mm the overall rate is 69.5% (up to 91.4% for one location-year), which makes an always-Yes baseline trivially strong. LCA is the wettest location (67.7% at 1 mm). Strong seasonality: monthly rate 25% (December) to 78% (August). The threshold is a parameter; changing it means regenerating labels only. These rates were measured with the v0.2 implementation. With v0.2.1: train 51.6%, validation 50.1%, test 49.7%; LCA 67.8%; on train by Vietnam local month 26.5% (December) to 78.6% (August) (`notebooks/eda_rain_next24h.ipynb`, which used the UTC month before 2026-10-22) |
| Label source | Decided 2026-10-21: Open-Meteo for the first experiment (complete coverage for 8 locations × 2016–2025). Label is gridded reanalysis rainfall, not gauge observation; Open-Meteo is largely ERA5-derived, so the experiment is retrospective. Station-first with ERA5 fallback (workbook proposal) stays open as a later extension; ERA5 fallback is not decided |
| Whole-window versus hourly source selection | Moot for the first experiment (single source per label); revisit with any multi-source label |
| Missing-label handling | Decided 2026-10-21: a window missing any of its 24 hourly values has no label and is excluded from evaluation; missing is not No. Minimum coverage beyond this stays open |
| Calendar basis | `date_key`/`time_key` use UTC+7 (see data contract); labels, forecast origins, and splits use UTC |
| Rain timestamp convention | Verified 2026-10-21 from the Open-Meteo Historical Weather API documentation: hourly `precipitation` is the "sum of the preceding hour". The value at timestamp h covers `(h-1h, h]`, so `(t, t+24h]` uses the values timestamped t+1h through t+24h |
| Recall versus Precision/false-alarm tradeoff | Decided 2026-10-22 by the N1/N2 member doing N3's ML work (N3 review pending): the Recall operating point is the threshold with the highest validation precision among those with validation recall ≥ 0.85. CART v1 (corrected validation labels): 0.36, precision 0.710, recall 0.880, F1 0.786. The 0.85 target was set after viewing the validation scan. The frozen 0.40 remains the official test 2025 result; metrics at 0.36 on test are post-hoc (ID 36). See [business requirements §5a](business-requirements.md#5a-proposals-awaiting-team-decision-2026-10-22) |

The rainfall threshold in mm defines ground truth. The probability threshold defines predicted classes. They are not interchangeable.

## Timing and labels

- Forecast origin `t` is when information is used to forecast, not necessarily job execution time.
- For hourly rain timestamped at interval end, target `(t, t+24h]` consists of t+1h through t+24h. For interval-start timestamps, convert/interpret source intervals before calculation; do not blindly reuse identical `shift` logic.
- Rain labels indicate target rainfall reaching the agreed mm threshold. Retain target start/end, source/provenance, label version, and valid-hour count.
- Decided 2026-10-21 (see the table above): missing required hours make labels unavailable and exclude them from evaluation; missing is not No. An end-of-series window shorter than 24 hours is invalid. Any alternative policy requires an explicit decision.
- Calculate separately per location on a gap-checked hourly grid. `shift(-24)` is not necessarily 24 hours ahead when hours are missing.

## Features and leakage prevention

- Use only information available at or before t: lags, trailing rolling statistics, 3h/24h pressure changes, and hour/month. Account for publication latency; measurement timestamps do not prove availability.
- No centered rolling, future shifts, bfill, or future-informed interpolation. Fit imputation/preprocessing/feature selection on train; validation/test only transform.
- Lag/rolling calculations must not cross locations. Fix source pivot/fusion rules; do not use test to select sources or features.
- If only final ERA5 reanalysis is available, report a retrospective experiment, not a verified real-time forecast. Check feature availability before operational use.

### Feature dataset v1/v2 (v1 2026-10-21; v2 2026-10-22)

- Source: Silver Open-Meteo hourly only (decided 2026-10-21; GHCNh station rain is kept for verification). Built by `scripts/build_rain_features.py` from the label dataset of the same version (v1: labels v0.2; v2: labels v0.2.1 and trailing rain sums rounded to 3 decimals); tests in `tests/test_rain_features.py`. Both versions have identical feature columns; their rain sums differ only by float error, so model v1 gives identical predictions on either.
- Rows: usable labels only, one per location × UTC origin, written per split to `gold/ml_features/cart_rain_{train_2016_2023,val_2024,test_2025}_<version>.parquet` with the manifest `cart_rain_features_<version>.json`. The manifest holds the ordered feature list with units; training must use that order.
- 22 numeric features, all computed per location on a gap-checked hourly grid from values stamped at or before t:
  - At t: `temperature_c`, `dew_point_c`, `dew_point_depression_c`, `humidity_percent`, `pressure_hpa` (surface), `cloud_cover_percent`, `wind_speed_ms`, `precipitation_mm` (hour ending at t).
  - Lags: `humidity_percent_lag3h`/`lag6h`, `cloud_cover_percent_lag3h`/`lag6h`.
  - Trailing windows `(t − n h, t]`: `rain_past_{3,6,12,24}h_mm`, `humidity_mean_6h_percent`, `cloud_cover_mean_6h_percent`.
  - Pressure tendency: `pressure_change_3h_hpa`, `pressure_change_24h_hpa`.
  - Calendar of the origin in UTC+7: `hour_local`, `month_local`.
- `location_code` is an identifier and a candidate categorical feature; encoding is decided in the training pipeline (ID 28).
- `rain_next24h_mm` is kept for error analysis only and is never a feature.
- A lag or window touching a missing hour is NaN; nothing is filled. In the 2016–2025 build, missing values occur only in the first 24 origin hours of 2016 (192 train rows). Imputation, if needed, is fitted on train in ID 28.
- Solar radiation was left out because the train-split EDA showed no separation and it mostly encodes the hour. Wind direction was also left out, but the EDA never assessed it (correction recorded 2026-10-21 in ID 30), so its exclusion has no evidence behind it. It is a candidate for the next feature version (v3; "v2" above is the corrected-label rebuild of the same 22 features). **Disclosure:** this idea came from a test 2025 example (ID 30), so it must first be justified by train-only EDA, and a model built on it no longer has an independent 2025 test. The EDA kept month as an integer `month_local` rather than the sin/cos it first suggested, because CART splits on thresholds (recorded 2026-10-22). Open-Meteo is reanalysis with publication delay, so results are retrospective.

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

- Inputs: the v1 train and validation feature files only (labels v0.2, see the implementation fix above); the test file is not read (`report.json` records `test_split_read: false`). Rows with a missing feature are dropped (192 train rows, the first 24 origin hours of 2016); nothing is imputed.
- Pipeline: the 22 numeric features unscaled plus one-hot `location_code`, then `DecisionTreeClassifier(random_state=42)`. Fit on train only; no refit on train + validation.
- Grid (120 fits): `max_depth` {4, 6, 8, 10, 12} × `min_samples_leaf` {50, 200, 1000} × `class_weight` {None, balanced} × `ccp_alpha` {0, 1e-5, 1e-4, 1e-3}.
- Selection: highest validation F1 of the Rain class at probability 0.5. Configurations within 0.005 F1 of the best count as tied, and the shallowest, then smallest, tree is chosen.
- Probability threshold: the value from 0.30 to 0.70 (step 0.02) with the highest validation F1 for the selected model; ties go to the value closest to 0.5.
- Recall emphasis (stated 2026-10-22): v1 does not optimize Recall. Both choices maximize F1; Recall is favoured only because the chosen threshold (0.40) is below 0.5. On validation (v0.2 labels) 0.40 gives recall 0.822 and precision 0.752; 0.34 would give recall 0.901, precision 0.693, and F1 0.784 versus 0.786 (`report.json` `threshold_scan`). A Recall rule for later work is a proposal (see the decisions table).
- Baselines on validation: always No, always Yes, persistence (past 24 h rain ≥ 1 mm), and the train-period Rain rate by location × month (Yes when ≥ 0.5).
- Artifacts: `gold/ml_models/cart_rain_next24h_v1/model.joblib` (preprocessing + tree) and `report.json` (features and units in order, parameters, threshold, seed, dependency versions, grid results, validation metrics, baselines). Load the joblib file only from this bucket, because loading can execute code. Written with scikit-learn 1.5.1 and joblib 1.5.3 (pipeline image); the notebook environment pins the same versions. `train_cart.py` refuses to overwrite an existing model or report of the same version (`--overwrite` only for a model never evaluated on test), and the label/feature exporters refuse an existing version; added 2026-10-22 after the audit found that a default run would have replaced model v1. Labels v0.2 and features v1 can be rebuilt only from code before commit `0287eaa` (for example `14c4827`); current code requires v0.2.1.

Result: `max_depth=8`, `min_samples_leaf=50`, `class_weight=balanced`, `ccp_alpha=1e-4`; 88 leaves; probability threshold 0.40 (frozen for ID 29). Validation 2024 has 70,080 rows, of which 49.4% are Rain:

| Predictor | Precision | Recall | F1 | Accuracy |
| --- | --- | --- | --- | --- |
| CART v1, threshold 0.40 | 0.752 | 0.822 | 0.786 | 0.778 |
| CART v1, threshold 0.50 | 0.787 | 0.775 | 0.781 | 0.785 |
| Persistence (past 24 h ≥ 1 mm)¹ | 0.752 | 0.754 | 0.753 | 0.755 |
| Location × month Rain rate | 0.704 | 0.716 | 0.710 | 0.711 |
| Always Yes | 0.494 | 1.000 | 0.662 | 0.494 |
| Always No | undefined | 0.000 | undefined | 0.506 |

¹ Computed on features v1, whose rain sums were not rounded: 427 validation rows fall on the wrong side of 1 mm. On features v2 (same v0.2 labels) persistence gives 0.748 / 0.759 / 0.754; with corrected labels and features (both v2) 0.756 / 0.757 / 0.757, against CART 0.760 / 0.819 / 0.789 (recomputed 2026-10-22). The model is unaffected (its thresholds lie between 0.1 mm steps, and its probabilities on v1 and v2 agree within 1e-12).

Validation chose both the configuration and the threshold, so these figures are optimistic; the independent estimate is the 2025 test evaluation (ID 29). These figures use v0.2 labels. With corrected v0.2.1 validation labels (50.1% Rain) and the same model and threshold: precision 0.760, recall 0.819, F1 0.789. The selection was not repeated.

### Test 2025 evaluation (ID 29, run once on 2026-10-21)

- `scripts/evaluate_cart_test.py` loads the ID 28 model and threshold unchanged. Before reading test, it checks that the artifact reproduces the validation confusion matrix stored in `report.json`. It refuses to run when `test_evaluation.json` already exists, so test 2025 is evaluated once. Model SHA-256: `1f667b91365752a25f1cad0b31258ea39b9d4bd5a17c2d8faa4939260cd494b6`.
- Outputs: `gold/ml_models/cart_rain_next24h_v1/test_evaluation.json` (metrics, baselines, breakdowns) and `test_predictions.parquet` (origin, target window, location, probability, predicted class, actual label) for error analysis (ID 36). Its `rain_next24h`/`rain_next24h_mm` columns hold the original v0.2 labels (436 wrong) and carry no label version; error analysis must take labels from `cart_rain_test_2025_v2.parquet` (joined on location and origin), as the ID 30 notebook does. The file is not rewritten.
- Coverage: 69,888 of 69,888 usable test labels were evaluated (no missing features).
- **Corrected labels (2026-10-22).** `test_evaluation.json` used v0.2 labels, so 436 test windows with exactly 1.0 mm were wrongly No. `scripts/rescore_cart_corrected_labels.py` re-scored the same predictions against v0.2.1 labels. It refuses to run unless the model reproduces the stored predictions exactly, so the model, features, and threshold are unchanged, and test 2025 still played no part in any design choice. The result is in `test_evaluation_corrected_labels.json` and is the reference test result below (49.65% Rain). The ID 31 DW view computes the same confusion matrix independently from the observation fact.

| Predictor on test 2025 | Precision | Recall | F1 | Accuracy |
| --- | --- | --- | --- | --- |
| CART v1, frozen threshold 0.40 | 0.762 | 0.792 | 0.777 | 0.774 |
| CART v1, original v0.2 labels (superseded) | 0.756 | 0.795 | 0.775 | 0.774 |
| Persistence (past 24 h ≥ 1 mm) | 0.730 | 0.729 | 0.729 | 0.731 |
| Location × month Rain rate (train) | 0.679 | 0.689 | 0.684 | 0.684 |
| Always Yes | 0.497 | 1.000 | 0.664 | 0.497 |
| Always No | undefined | 0.000 | undefined | 0.503 |

- Confusion matrix at 0.40 (Rain positive, corrected labels): TP 27,467, FP 8,569, TN 26,617, FN 7,235. Original v0.2 labels: TP 27,254, FP 8,782, TN 26,840, FN 7,012.
- F1 by location: 0.763 (HBI) to 0.789 (TUQ). Wet season May–Oct: F1 0.836 (recall 0.875). Dry season Nov–Apr: F1 0.624 (recall 0.597). By month, F1 ranges from 0.517 (January) to 0.903 (June).
- Share of Rain cases detected by rain amount: 1–5 mm 66%, 5–20 mm 84%, ≥ 20 mm 94%.
- Test F1 is 1.2 points below the corrected validation F1 (0.789), consistent with validation being optimistic after selection. No design choice was changed after seeing test. Results are a retrospective experiment on Open-Meteo reanalysis.

### Tree and rules of CART v1 (ID 30, 2026-10-21)

`notebooks/cart_tree_rules.ipynb` checks that the model SHA-256 equals the one in `test_evaluation.json` before drawing anything. It shows the first 3 of 8 levels, every leaf as a merged rule with train rows and observed Rain share (corrected v0.2.1 labels), the importances, and four worked test-2025 examples chosen by fixed rules: largest-rain true positive, highest-probability false positive, largest-rain false negative, and a typical false negative. Thresholds are in original units; Open-Meteo rain has 0.1 mm resolution, so `≤ 0.05 mm` means no rain.

- Root: past-12-hour rain ≤ 0.35 mm → 288,873 train rows, 27% Rain; otherwise 271,767 rows, 78% Rain.
- Largest Rain rule (69,273 rows, 97% Rain): past-12-hour rain > 0.35 mm, rain at *t* > 0.25 mm, past-24-hour rain > 2.95 mm, surface pressure ≤ 1015.35 hPa, 6-hour mean humidity > 80.42%.
- Largest No-rain rule (39,695 rows, 3% Rain): past-12-hour rain ≤ 0.05 mm, dew point ≤ 14.55 °C, cloud cover ≤ 96.5%, location ≠ LCA.
- The first split on Lào Cai is at depth 5 and on month at depth 6. Surface pressure is lower at higher points (train mean about 1001 hPa at Lào Cai versus 1007–1010 hPa elsewhere), so pressure thresholds also encode location.
- Missed rain on validation 2024 with corrected labels (post-selection description, no design change; computed in the notebook): 6,338 of 35,110 Rain cases (18.1%) are missed.
  - 64.8% of misses are under 5 mm (median 2.8 mm), and 54.8% had a probability of 0.30–0.40, just under the threshold.
  - Recall falls with rain onset: 91% when rain starts within 3 h of *t*, 44% when it starts after 19 h.
  - Rain after a dry 24 h (≤ 0.05 mm) accounts for 36.3% of misses and is rarely caught: recall 16.6% overall, 2.5% in the dry season, 31.5% in the wet season. After rain in the past 24 h, recall is 87.5%.
- The largest-rain miss in test 2025 (Ninh Bình, 57.9 mm, probability 0.39) is not typical. Rain began one hour after *t* under a fully overcast sky with a northerly wind and pressure 6.9 hPa higher than a day earlier; the model has no wind-direction feature, and its leaf holds about 40% Rain in train. A typical miss (fixed rule) is Ninh Bình on 2025-01-26: 2.9 mm, probability 0.397.
- 52 of 88 leaves predict Rain; the 10 largest rules cover 45.6% of train rows; rules have 4–7 merged conditions. Leaf probabilities (fitted on v0.2 labels) differ from the observed train Rain share under corrected labels by at most 0.053; on the v0.2 labels the gap was at most 0.009.

## Review by N3 (pending)

Decisions below were made by the N1/N2 member doing N3's ML work. N3 confirms or rejects each one; confirmed items are then recorded with the date.

1. Label: 1 mm over `(t, t+24h]`, Open-Meteo as the source, a missing hour means no label.
2. Implementation fix v0.2.1 (rounded rain sums) and the choice not to retrain model v1.
3. Features v1/v2: the 22 features, integer `month_local`, wind direction left out.
4. Training rule: grid, selection by validation F1 with the 0.005 tie rule, threshold by validation F1 (0.40).
5. Recall operating point (validation recall ≥ 0.85, highest precision: 0.36), reported post-hoc on test.
6. Re-score of the frozen model on corrected test labels as the reference test result.
7. Tree interpretation (ID 30), including the correction of "rain without warning".
8. Disclosures: the wind-direction idea came from a test 2025 case, so later models have no independent 2025 test.

## Artifacts and acceptance

Save model/preprocessing, ordered features/units, model/label versions, source policy, training window, threshold, seed, and dependency versions. Predictions retain UTC origin, target window, location, probability, and class; design loading keys according to [warehouse schema](warehouse-schema.md).

Report TP/FP/TN/FN with Rain as the positive class; Accuracy/Precision/Recall/F1, support, coverage, and threshold. Explain undefined metrics. Test labels at t, t+1h, t+24h, t+25h, gaps, and year boundaries; changing data after t must not change features at t.
