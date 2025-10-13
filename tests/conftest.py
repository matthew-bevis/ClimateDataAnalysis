import os
import io
import types
import pandas as pd
import numpy as np
import pytest

@pytest.fixture(autouse=True)
def _env(monkeypatch, tmp_path):
    # Azure Storage creds (fake)
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "fakeacct")
    monkeypatch.setenv("AZURE_STORAGE_KEY", "fakekey")
    # Working dirs
    monkeypatch.chdir(tmp_path)

@pytest.fixture
def fake_xarray_ds():
    """
    Minimal xarray-like object: context manager with .to_dataframe()
    Produces a small grid inside and outside the bbox.
    """
    class _DS:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def to_dataframe(self):
            # mix in/out of bounds, include >180 longitudes to test normalization
            df = pd.DataFrame({
                "latitude":  [25.1, 25.4, 30.0],
                "longitude": [279.49, -80.9, -120.0],
                "time":      pd.to_datetime(["2020-01-01","2020-01-01","2020-01-01"])
            })
            # Add LAI/FAPAR like the Parquet schema
            df["LAI"] = [1.5, 2.0, 3.0]
            df["FAPAR"] = [0.35, 0.40, 0.50]
            return df
    return _DS()
