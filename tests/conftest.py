import os
import pandas as pd
import numpy as np
import pytest

class _FakeXRDataset:
    """Minimal xarray-like context manager that returns a DataFrame."""
    def __init__(self, df):
        self._df = df
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def to_dataframe(self): return self._df

@pytest.fixture
def tmp_outdir(tmp_path):
    d = tmp_path / "out"
    d.mkdir()
    return str(d)

@pytest.fixture
def fake_xarray_ds_factory():
    def _factory(rows=None):
        # Make a tiny DF that includes plausible columns + mixed time types
        if rows is None:
            rows = pd.DataFrame({
                "latitude":  [25.10, 25.40, 30.00],     # last should be outside bbox
                "longitude": [279.49, -80.90, -120.0],  # 279.49 => -80.51 after -360
                # a mix: datetime64, seconds (int), ms (int), string is covered in transformer tests
                "time": pd.to_datetime(["2020-01-01","2020-01-01","2020-01-01"]),
                "LAI": [1.0, 2.0, 3.0],
                "FAPAR": [0.2, 0.4, 0.6],
            })
        return _FakeXRDataset(rows)
    return _factory

