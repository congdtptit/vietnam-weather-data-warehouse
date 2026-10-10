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
    schema = Path(__file__).resolve().parents[1] / "sql/init_dwh_schema.sql"
    with pg_connection, pg_connection.cursor() as cursor:
        cursor.execute(schema.read_text())
        cursor.execute("SELECT count(*) FROM dim_location")
        assert cursor.fetchone()[0] == 8
        cursor.execute("SELECT count(*) FROM dim_time")
        assert cursor.fetchone()[0] == 24
        cursor.execute("SELECT count(*) FROM dim_source")
        assert cursor.fetchone()[0] == 4
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )
        assert {row[0] for row in cursor.fetchall()} == {
            "dim_date",
            "dim_time",
            "dim_location",
            "dim_source",
            "dim_station",
            "fact_weather_observation",
            "fact_weather_prediction",
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
