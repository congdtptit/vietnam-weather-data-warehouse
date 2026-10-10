"""Synthetic fixtures. Live services are reachable only in the test Compose stack."""

import importlib
import json
import os
import socket
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]


def pytest_sessionstart(session):
    if os.getenv("WEATHER_TESTING") != "1":
        raise pytest.UsageError(
            "Run `make test` or `make test-integration`; WEATHER_TESTING=1 required"
        )


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch, request):
    # Never allow dotenv or inherited production endpoints to configure unit tests.
    if request.node.get_closest_marker("integration") is None:
        for name, value in {
            "MINIO_ENDPOINT": "minio-test:9000",
            "MINIO_EXTERNAL_ENDPOINT": "minio-test:9000",
            "MINIO_ROOT_USER": "weather_test",
            "MINIO_ROOT_PASSWORD": "weather_test_only",
            "MINIO_BUCKET_BRONZE": "bronze",
            "POSTGRES_HOST": "pg-test",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "weather_test",
            "POSTGRES_PASSWORD": "weather_test_only",
            "DWH_DB": "weather_test",
        }.items():
            monkeypatch.setenv(name, value)

        def deny_network(*args, **kwargs):
            raise AssertionError("Unit tests must mock network calls")

        monkeypatch.setattr(socket.socket, "connect", deny_network)
        monkeypatch.setattr(socket.socket, "connect_ex", deny_network)


@pytest.fixture
def hourly_payload():
    return json.loads((ROOT / "tests/fixtures/open_meteo_hourly.json").read_text())


@pytest.fixture
def ingestion(monkeypatch):
    module = importlib.import_module("ingest_open_meteo")
    monkeypatch.setattr(module.time, "sleep", Mock())
    return module


@pytest.fixture
def dag_bag():
    from airflow.models import DagBag

    bag = DagBag(str(ROOT / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag


@pytest.fixture(scope="session")
def minio_client():
    from minio import Minio

    assert os.getenv("MINIO_ENDPOINT") == "minio-test:9000"
    assert os.getenv("MINIO_EXTERNAL_ENDPOINT") == "minio-test:9000"
    assert os.getenv("MINIO_ROOT_USER") == "weather_test"
    client = Minio(
        "minio-test:9000", access_key="weather_test", secret_key="weather_test_only", secure=False
    )
    deadline = time.monotonic() + 45
    while True:
        try:
            client.list_buckets()
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)
    for bucket in ["bronze", "silver", "gold"]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    return client


@pytest.fixture
def pg_connection():
    import psycopg2

    assert os.getenv("POSTGRES_HOST") == "pg-test"
    assert os.getenv("DWH_DB") == "weather_test"
    assert os.getenv("POSTGRES_USER") == "weather_test"
    connection = psycopg2.connect(
        host="pg-test",
        port=5432,
        dbname="weather_test",
        user="weather_test",
        password="weather_test_only",
        connect_timeout=5,
    )
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute((ROOT / "sql/init_dwh_schema.sql").read_text())
        yield connection
    finally:
        connection.close()
