"""Repeatable versions of the environment smoke checks."""

import sys
from importlib.metadata import version

import geopandas as gpd
import pandas as pd
import xarray as xr
from shapely.geometry import Point
from sklearn.tree import DecisionTreeClassifier


def test_runtime_versions():
    assert sys.version_info[:2] == (3, 10)
    assert version("apache-airflow") == "2.9.3"
    assert version("SQLAlchemy") == "1.4.52"


def test_parquet_preserves_utc_and_missing_values(tmp_path):
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range("2024-02-29T23:00", periods=3, freq="h", tz="UTC"),
            "precipitation_mm": [0.0, None, 1.2],
        }
    )
    path = tmp_path / "sample.parquet"
    frame.to_parquet(path, engine="pyarrow", index=False)
    pd.testing.assert_frame_equal(frame, pd.read_parquet(path, engine="pyarrow"))


def test_netcdf_round_trip(tmp_path):
    dataset = xr.Dataset({"temperature_c": ("hour", [20.0, 21.0, 22.0])})
    path = tmp_path / "sample.nc"
    dataset.to_netcdf(path, engine="netcdf4")
    with xr.open_dataset(path, engine="netcdf4") as loaded:
        xr.testing.assert_equal(dataset, loaded.load())


def test_geojson_round_trip(tmp_path):
    frame = gpd.GeoDataFrame(
        {"location_code": ["HAN"]}, geometry=[Point(105.8542, 21.0285)], crs="EPSG:4326"
    )
    path = tmp_path / "sample.geojson"
    frame.to_file(path, driver="GeoJSON")
    loaded = gpd.read_file(path)
    assert loaded.loc[0, "location_code"] == "HAN"
    assert loaded.crs.to_epsg() == 4326
    assert loaded.geometry.iloc[0].equals(frame.geometry.iloc[0])


def test_cart_library_smoke():
    model = DecisionTreeClassifier(max_depth=2, random_state=42).fit(
        [[0], [1], [2], [3]], [0, 0, 1, 1]
    )
    assert model.predict([[0], [3]]).tolist() == [0, 1]
