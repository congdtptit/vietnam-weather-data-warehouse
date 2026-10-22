import importlib
import io
import json
from pathlib import Path
from unittest.mock import Mock

import psycopg2
import pytest

pytestmark = pytest.mark.integration


def test_bronze_upload_and_rerun(minio_client, ingestion, monkeypatch):
    # A synthetic complete year; never request historical data from the real API.
    from test_ingestion import _year_payload

    raw = json.dumps(_year_payload(2024)).encode()
    fetch = Mock(return_value=raw)
    monkeypatch.setattr(ingestion, "fetch_open_meteo_year", fetch)
    monkeypatch.setattr(ingestion, "get_minio_client", lambda: minio_client)
    ingestion.ingest_data(2024, 2024, target_location="HAN", overwrite=True)
    ingestion.ingest_data(2024, 2024, target_location="HAN")  # current: skipped
    names = sorted(o.object_name for o in minio_client.list_objects("bronze", recursive=True))
    assert names == sorted(ingestion.bronze_keys("HAN", 2024))
    result = minio_client.get_object("bronze", ingestion.bronze_keys("HAN", 2024)[0])
    try:
        assert result.read() == raw  # received bytes, unchanged
    finally:
        result.close()
        result.release_conn()
    assert fetch.call_count == 1


def test_minio_health_task(minio_client):
    importlib.import_module("weather_pipeline_health_dag").check_minio_connection()


def test_schema_and_seed_rerun(pg_connection):
    """Rerun the real initializer order (base schema + every numbered migration)."""
    sql_dir = Path(__file__).resolve().parents[1] / "sql"
    files = [sql_dir / "init_dwh_schema.sql", *sorted(sql_dir.glob("[0-9][0-9]_*.sql"))]
    assert [f.name for f in files[1:]] == [
        "02_olap_views.sql",
        "03_station_daily_rain.sql",
        "04_predictions.sql",
        "05_metadata_corrections.sql",
    ]
    with pg_connection, pg_connection.cursor() as cursor:
        for f in files:
            cursor.execute(f.read_text())
        cursor.execute("SELECT count(*) FROM dim_location")
        assert cursor.fetchone()[0] == 8
        cursor.execute("SELECT count(*) FROM dim_time")
        assert cursor.fetchone()[0] == 24
        cursor.execute("SELECT source_name FROM dim_source ORDER BY source_key")
        assert [r[0] for r in cursor.fetchall()] == [
            "Open-Meteo",
            "ERA5",
            "Meteostat",
            "NOAA",
            "GHCNh",
        ]
        cursor.execute(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema='public'"
        )
        objects = dict(cursor.fetchall())
        assert {t for t, kind in objects.items() if kind == "BASE TABLE"} == {
            "dim_date",
            "dim_time",
            "dim_location",
            "dim_source",
            "dim_station",
            "dim_model",
            "fact_weather_observation",
            "fact_weather_prediction",
            "fact_station_daily_rain",
        }
        assert {t for t, kind in objects.items() if kind == "VIEW"} >= {
            "vw_obs_hourly",
            "vw_obs_daily",
            "vw_coverage",
            "vw_prediction_vs_actual",
        }
        cursor.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE column_name IN ('pressure_type', 'station_role', 'distance_km', "
            "'evaluation_label_version', 'validation_f1_evaluation_labels') "
            "AND table_name IN ('fact_weather_observation', 'dim_station', 'dim_model')"
        )
        assert set(cursor.fetchall()) == {
            ("fact_weather_observation", "pressure_type"),
            ("dim_station", "station_role"),
            ("dim_station", "distance_km"),
            ("dim_model", "evaluation_label_version"),
            ("dim_model", "validation_f1_evaluation_labels"),
        }


def test_location_code_uniqueness(pg_connection):
    with (
        pytest.raises(psycopg2.errors.UniqueViolation),
        pg_connection,
        pg_connection.cursor() as cursor,
    ):
        cursor.execute(
            "INSERT INTO dim_location (location_code, location_name, province, latitude, longitude) VALUES ('HAN', 'Duplicate', 'Fixture', 21, 105)"
        )


def test_station_foreign_key(pg_connection):
    with (
        pytest.raises(psycopg2.errors.ForeignKeyViolation),
        pg_connection,
        pg_connection.cursor() as cursor,
    ):
        cursor.execute(
            "INSERT INTO dim_station (station_id, station_name, location_key) VALUES ('synthetic-invalid', 'Fixture', -1)"
        )


def test_postgres_health_task(pg_connection):
    importlib.import_module("weather_pipeline_health_dag").check_postgres_dwh_connection()


def _put_silver(minio_client, objects):
    for key, body in objects.items():
        minio_client.put_object("silver", key, io.BytesIO(body), len(body))


def test_quality_gate_task_fails_in_airflow_on_bad_silver(minio_client, dag_bag):
    """The DAG's own `quality_gate` command, run by a BashOperator against a real MinIO."""
    import pandas as pd
    from airflow.exceptions import AirflowException
    from airflow.operators.bash import BashOperator
    from test_quality_gate import OM_KEY, _hourly, _silver

    scripts = Path(__file__).resolve().parents[1] / "scripts"
    gate = dag_bag.dags["weather_bronze_to_dw_dag"].get_task("quality_gate")
    params = {"start_year": 2024, "end_year": 2024, "location": "HAN"}
    command = gate.render_template(gate.bash_command, {"params": params})
    command = command.replace("/opt/airflow/scripts", str(scripts))

    def run():
        BashOperator(task_id="quality_gate_probe", bash_command=command).execute(context={})

    def reports():
        return {
            o.object_name
            for o in minio_client.list_objects("silver", prefix="quality_reports/", recursive=True)
        }

    _put_silver(minio_client, _silver())
    before = reports()
    run()  # complete synthetic Silver: exit 0
    assert len(reports() - before) == 1

    bad = _hourly("Open-Meteo", 1)
    bad.loc[3, "humidity_percent"] = 150.0
    buf = io.BytesIO()
    bad.to_parquet(buf)
    _put_silver(minio_client, {OM_KEY.format(m=1): buf.getvalue()})
    before = reports()
    with pytest.raises(AirflowException, match="exit code 1"):
        run()
    (key,) = reports() - before
    resp = minio_client.get_object("silver", key)
    try:
        report = json.loads(resp.read())
    finally:
        resp.close()
        resp.release_conn()
    failed = [c for c in report["checks"] if c["status"] == "FAIL"]
    assert not report["passed"] and [c["check_id"] for c in failed] == ["H06_valid_ranges"]
    assert failed[0]["violations"] == {"out_of_range_humidity": 1}
    assert pd.Timestamp(report["checked_at_utc"]).tzinfo is not None
