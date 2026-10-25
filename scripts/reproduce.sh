#!/usr/bin/env bash
# scripts/reproduce.sh: rebuild the whole project with one command (`make reproduce`).
#
# Runs the steps of docs/reproduce-from-scratch.md in order and stops at the first failure:
#   1. build the Airflow image, start the Compose stack, wait for Airflow and its DAGs
#   2. province boundaries (Bronze, Silver)
#   3. weather_bronze_to_dw_dag for START_YEAR..END_YEAR: every source Bronze -> Silver -> Gold
#      -> DW, quality gate, cube refresh, reconcile; the script waits for the run to finish
#   4. RainNext24h labels and features (Gold ML)
#   5. CART v1 from its release bundle (never retrained) and its test predictions in the DW
#   6. weather_rain_scoring_dag for SCORE_START..SCORE_END
#   7. Gold ML multi-source pivot and the GeoPandas station check
#   8. final checks: cubes against their views, DW row counts
#
# Every step is idempotent. On a machine that already holds the data, no source is downloaded
# again (current manifests), loads leave identical rows unchanged, and published Gold ML
# datasets are skipped (--skip-existing), so a rerun only verifies.
#
# Settings (environment variables; `make reproduce` passes them through):
#   START_YEAR, END_YEAR     pipeline window in UTC years, default 2016..2025 (project scope);
#                            a smaller window is for testing, since labels and features then
#                            cover only that window on a new machine
#   MODEL_RELEASE            folder of the CART v1 release bundle (scripts/model_release.py
#                            export), default outputs/model_release_cart_v1
#   SCORE_START, SCORE_END   scoring window, default 2025-01-01..2025-12-30 (test split)
#   DRY_RUN=1                print the steps without running anything
set -euo pipefail

cd "$(dirname "$0")/.."

START_YEAR=${START_YEAR:-2016}
END_YEAR=${END_YEAR:-2025}
MODEL_RELEASE=${MODEL_RELEASE:-outputs/model_release_cart_v1}
SCORE_START=${SCORE_START:-2025-01-01}
SCORE_END=${SCORE_END:-2025-12-30}
DRY_RUN=${DRY_RUN:-0}
PIPELINE_TIMEOUT_S=${PIPELINE_TIMEOUT_S:-10800}
SCORING_TIMEOUT_S=${SCORING_TIMEOUT_S:-1800}

STEP=0
STARTED=$(date +%s)

fail() {
    echo "[!] $*" >&2
    exit 1
}

# Value of KEY in .env without sourcing the file (values are never printed)
env_value() {
    local key=$1 default=$2 value=""
    if [ -f .env ]; then
        value=$(grep -E "^${key}=" .env | tail -n 1 | cut -d= -f2- | sed -e 's/^["'\'']//' -e 's/["'\'']$//')
    fi
    echo "${value:-$default}"
}

AIRFLOW_URL="http://localhost:$(env_value AIRFLOW_WEBSERVER_PORT 8080)"
AIRFLOW_AUTH="$(env_value _AIRFLOW_WWW_USER_USERNAME admin):$(env_value _AIRFLOW_WWW_USER_PASSWORD admin)"

step() {
    local title=$1
    shift
    STEP=$((STEP + 1))
    echo
    echo "==> [$STEP] $title"
    if [ "$DRY_RUN" = 1 ]; then
        echo "    (dry run) $*"
        return 0
    fi
    local t0
    t0=$(date +%s)
    "$@"
    echo "    done in $(($(date +%s) - t0)) s"
}

run_script() {
    local name=$1
    shift
    docker compose run --rm --no-deps -T --entrypoint python airflow-webserver \
        /opt/airflow/scripts/"$name" "$@"
}

api() {
    local method=$1 path=$2
    shift 2
    curl -sS -f -u "$AIRFLOW_AUTH" -X "$method" -H 'Content-Type: application/json' \
        "$AIRFLOW_URL/api/v1$path" "$@"
}

json_field() {
    python3 -c 'import json, sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1"
}

preflight() {
    command -v docker >/dev/null || fail "docker is not installed"
    command -v curl >/dev/null || fail "curl is not installed"
    command -v python3 >/dev/null || fail "python3 is not installed"
    [ -f .env ] || fail ".env is missing: cp .env.example .env, then fill CDSAPI_URL and CDSAPI_KEY"
    if [ -z "$(env_value CDSAPI_KEY "")" ]; then
        echo "    warning: CDSAPI_KEY is empty in .env; ERA5 can only be reused from Bronze, not downloaded"
    fi
    case "$START_YEAR$END_YEAR" in *[!0-9]*) fail "START_YEAR and END_YEAR must be years" ;; esac
    if [ "$START_YEAR" -lt 2016 ] || [ "$END_YEAR" -gt 2025 ] || [ "$START_YEAR" -gt "$END_YEAR" ]; then
        fail "window $START_YEAR-$END_YEAR is outside 2016-2025 or reversed"
    fi
    [ -f "$MODEL_RELEASE/release.json" ] ||
        fail "no release bundle at $MODEL_RELEASE (set MODEL_RELEASE=/path/to/model_release_cart_v1)"
    echo "    window $START_YEAR-$END_YEAR, scoring $SCORE_START..$SCORE_END, model bundle $MODEL_RELEASE"
}

start_stack() {
    docker compose build
    docker compose up -d --no-build
    local deadline=$(($(date +%s) + 600))
    until curl -sf "$AIRFLOW_URL/health" >/dev/null 2>&1; do
        [ "$(date +%s)" -lt "$deadline" ] || fail "Airflow did not become healthy within 10 minutes"
        sleep 5
    done
    local dag
    for dag in weather_bronze_to_dw_dag weather_rain_scoring_dag; do
        until api GET "/dags/$dag" >/dev/null 2>&1; do
            [ "$(date +%s)" -lt "$deadline" ] || fail "$dag was not parsed within 10 minutes"
            sleep 5
        done
    done
    docker compose ps
}

# Trigger a DAG run with the given conf and wait until it ends; fail unless it succeeded
trigger_and_wait() {
    local dag=$1 conf=$2 timeout_s=$3
    local run_id
    run_id="reproduce__$(date -u +%Y%m%dT%H%M%SZ)"
    # Both DAGs have no schedule, so unpausing only lets the triggered run start
    api PATCH "/dags/$dag?update_mask=is_paused" -d '{"is_paused": false}' >/dev/null
    api POST "/dags/$dag/dagRuns" -d "{\"dag_run_id\": \"$run_id\", \"conf\": $conf}" >/dev/null
    echo "    triggered $dag run $run_id with $conf"
    local deadline=$(($(date +%s) + timeout_s)) state="" last=""
    while true; do
        state=$(api GET "/dags/$dag/dagRuns/$run_id" | json_field state)
        if [ "$state" != "$last" ]; then
            echo "    $(date +%H:%M:%S) state: $state"
            last=$state
        fi
        case "$state" in
            success) return 0 ;;
            failed) fail "$dag run $run_id failed; see the task logs in the Airflow UI ($AIRFLOW_URL)" ;;
        esac
        [ "$(date +%s)" -lt "$deadline" ] || fail "$dag run $run_id still $state after $timeout_s s"
        sleep 20
    done
}

pipeline() {
    trigger_and_wait weather_bronze_to_dw_dag \
        "{\"start_year\": $START_YEAR, \"end_year\": $END_YEAR, \"location\": \"\"}" "$PIPELINE_TIMEOUT_S"
}

ml_datasets() {
    run_script export_rain_labels.py --start-year "$START_YEAR" --end-year "$END_YEAR" --skip-existing
    run_script build_rain_features.py --start-year "$START_YEAR" --end-year "$END_YEAR" --skip-existing
}

model_and_predictions() {
    local release
    release=$(cd "$MODEL_RELEASE" && pwd)
    docker compose run --rm --no-deps -T -v "$release:/release:ro" --entrypoint python \
        airflow-webserver /opt/airflow/scripts/model_release.py import --from /release
    run_script load_predictions.py --version v1
}

scoring() {
    trigger_and_wait weather_rain_scoring_dag \
        "{\"start_date\": \"$SCORE_START\", \"end_date\": \"$SCORE_END\", \"model_version\": \"v1\"}" \
        "$SCORING_TIMEOUT_S"
}

other_products() {
    run_script build_ml_pivot.py --start-year "$START_YEAR" --end-year "$END_YEAR" --skip-existing
    run_script assign_stations_geo.py
}

final_checks() {
    run_script refresh_olap_cubes.py --check-only
    docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d weather_dwh' <<'SQL'
SELECT 'fact_weather_observation' AS object, count(*) AS rows FROM fact_weather_observation
UNION ALL SELECT 'fact_station_daily_rain', count(*) FROM fact_station_daily_rain
UNION ALL SELECT 'fact_weather_prediction', count(*) FROM fact_weather_prediction
UNION ALL SELECT 'mv_obs_daily', count(*) FROM mv_obs_daily
UNION ALL SELECT 'mv_obs_monthly', count(*) FROM mv_obs_monthly;
SQL
}

echo "Reproduce the weather Data Lake, DW, and CART predictions (docs/reproduce-from-scratch.md)"
echo
echo "==> [0] Preflight checks (run in a dry run too)"
preflight
step "Build the image and start the Compose stack" start_stack
step "Province boundaries" run_script ingest_boundaries.py
step "Pipeline DAG: sources -> Bronze -> Silver -> Gold -> DW" pipeline
step "Labels and features (Gold ML)" ml_datasets
step "Model v1 from its release bundle; test predictions into the DW" model_and_predictions
step "Scoring DAG" scoring
step "Gold ML pivot and station check" other_products
step "Final checks" final_checks
echo
echo "Finished in $(($(date +%s) - STARTED)) s. Airflow: $AIRFLOW_URL; DW: localhost:5434, database weather_dwh."
