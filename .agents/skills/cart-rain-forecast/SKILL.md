---
name: cart-rain-forecast
description: "Build, fix, or evaluate CART RainNext24h models: rain-label definitions, leakage-free features, chronological splits, validation threshold selection, and Recall evaluation. Use for weather ML notebooks/pipelines or forecast integration into the DW, not general weather ETL."
---

# CART RainNext24h

## Scope and contract

Read `docs/rain-label.md`, relevant `docs/data-contract.md` definitions, feature datasets, source pipelines, and scoped AGENTS instructions relative to the repository root. Use the algorithm and chronological split contract recorded in `docs/rain-label.md`; do not switch algorithms automatically.

`docs/rain-label.md` owns target intervals, split years, thresholds, source priorities, and their decision status. Keep definitions there; this skill supplies reusable leakage-prevention and evaluation procedures. Check `docs/progress.md` before claiming labels/models have been implemented.

Establish label contract before training: origin t, target interval, precipitation interval semantics, mm threshold, source priority, coverage, missing-data handling. If undecided, propose 0.1 or 1 mm without calling either an approved requirement. Distinguish precipitation label thresholds from probability classification thresholds.

## Correct label timing

- For hourly rain timestamped at interval **end**, sum t+1h through t+24h, representing `(t, t+24h]`. For interval-start timestamps, convert/interpret intervals first; do not reuse `shift` logic unchanged across different source semantics.
- Positive labels mean future rain reaches the agreed mm threshold. Retain label source/version, target start/end, valid hour count, coverage flag.
- By default, any missing required hour makes the label unavailable and excludes it from evaluation unless the contract explicitly allows another rule. Missing is not No; a final window shorter than 24h is not valid.
- For station priority with ERA5 fallback, specify whole-window versus hourly source selection and retain provenance. Do not mix sources automatically or choose ground truth based on prediction results.
- Sort and calculate labels separately per location on a gap-checked hourly grid. `shift(-24)` with missing hours is not necessarily 24h ahead.

## Features and leakage prevention

- Features use only information available at origin t: lags, trailing rolling statistics, 3h/24h pressure changes, hour/month, available weather. No centered rolling, future shifts, backfilling, or bidirectional interpolation.
- Lag/rolling operations stay within location and source/fusion configuration. Establish whether measurements at t were published by forecast time, considering interval-end timestamps and source latency.
- Multisource Gold requires pivot/selection rules and source-specific feature names. Sources at the same location/hour are not independent samples permitted to fall into different splits.
- Fit imputers, feature selection, preprocessing on train; validation/test only transform. Do not select CART parameters/class weights from test.
- ERA5/final historical reanalysis does not prove real-time forecasting feasibility. Describe final-reanalysis experiments as retrospective; deployment requires checking feature availability/latency at origin.

## Splits and model/threshold selection

- Split by UTC origin using stated year boundaries, without random shuffling. Check calendar basis and sample counts by split/location/class.
- Purge train/validation samples whose targets cross the next split boundary defined in the label contract. Remove incomplete test ground truth. Earlier-split trailing history may support later features without future data.
- Tune `max_depth`, `min_samples_leaf`, `ccp_alpha`, `class_weight`, seeds through train/validation or chronological folds within train. Pruning paths use train only. No resampling before splitting.
- Compare always-No and suitable simple baselines; report class prevalence. Prioritize Recall without choosing all-Yes merely for Recall 1: define Precision/F1 tradeoffs or false-alarm limits on validation.
- Choose probability thresholds on the designated validation period, freeze model/preprocessing/features/threshold/label contract, then evaluate the designated test period. If refitting, version artifacts and verify calibration/threshold suitability without tuning against test.
- If design changes after viewing test, disclose test participation in development; do not call it independent final evaluation.

## Evaluation and interpretation

Confusion Matrix uses Rain as positive. Report TP/FP/TN/FN, Accuracy, Precision, Recall, F1, support, coverage, threshold. Handle zero denominators explicitly, not as good performance. Analyze location/season, light/heavy rain, missing sources; report support for small groups.

Plot trees or extract rules using actual feature names/units; disclose display-depth limits. Importance describes the model, not causality. Analyze FN for missed rain and FP for false alarms.

## Artifacts and integration

- Save preprocessing + model, ordered features, units, model/label versions, source policy, train window, threshold, seed, dependency versions, metrics.
- Scoring retains origin, target window, location, probability, predicted class, model version. Load predictions idempotently at forecast grain using `DimModel`/Fact compatible with actual schema.
- For SQL/DW loading, read `supabase-postgres-best-practices` if available; apply `weather-dw-design` for schema changes. Airflow scoring must not automatically retrain on test.
- Verify labels with small sequences containing rain at t, t+1h, t+24h, t+25h, gaps, year boundaries. Changing post-t data must not change features at t; repeat inference with the same artifact must be stable.
- Deliver scoped notebooks/code/artifacts, actual computed metrics, and retrospective/operational limitations. Do not launch large downloads/training for design-only tasks.
