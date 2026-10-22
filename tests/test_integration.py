import importlib
import json
from pathlib import Path
from unittest.mock import Mock

import psycopg2
import pytest

pytestmark = pytest.mark.integration


def test_bronze_upload_and_rerun(minio_client, ingestion, hourly_payload, monkeypatch):
    # Three synthetic hours; never request historical data from the real API.
    fetch = Mock(side_effect=lambda **kwargs: json.loads(json.dumps(hourly_payload)))
    monkeypatch.setattr(ingestion, "fetch_open_meteo_year", fetch)
    monkeypatch.setattr(ingestion, "get_minio_client", lambda: minio_client)
    for _ in range(2):
        ingestion.ingest_data(2024, 2024, target_location="HAN", overwrite=True)
    objects = list(minio_client.list_objects("bronze", recursive=True))
    assert len(objects) == 1
    result = minio_client.get_object("bronze", objects[0].object_name)
    try:
        saved = json.loads(result.read())
    finally:
        result.close()
        result.release_conn()
    assert saved["hourly"] == hourly_payload["hourly"]
    assert fetch.call_count == 2


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
