import os, pandas as pd, numpy as np, pyarrow.parquet as pq
import pytest
from transformation.data_transformer import DataTransformer

class FakeDS:
    def __init__(self, df): self._df = df
    def to_dataframe(self): return self._df
    def __enter__(self): return self
    def __exit__(self,*a): return False

@pytest.fixture
def fake_xarray_ds_factory():
    def _factory(df): return FakeDS(df)
    return _factory

def test_transformer_filters_bbox_and_coerces_time(monkeypatch, tmp_path, fake_xarray_ds_factory):
    df = pd.DataFrame({
        "latitude":  [25.10, 25.40, 30.00],
        "longitude": [279.49, -80.90, -120.0],
        "time":      [pd.Timestamp("2020-01-01T06:00Z"), 1577836800, 1577836800000],
        "LAI": [1.0, 2.0, 3.0],
        "FAPAR": [0.2, 0.4, 0.6],
    })
    fake = fake_xarray_ds_factory(df)
    monkeypatch.setattr("transformation.data_transformer.xr.open_dataset", lambda *a, **k: fake)

    outdir = tmp_path / "out"
    t = DataTransformer(25.0, 26.5, -81.5, -80.5, str(outdir))
    nc_file = tmp_path / "VIIRS-Land_v001_20200101.nc"
    nc_file.write_bytes(b"fake")

    p = t.process(str(nc_file))
    assert p and os.path.exists(p)

    table = pq.read_table(p)
    cols = table.schema.names
    assert cols == ["latitude","longitude","time","LAI","FAPAR"]
    # confirm time is stored as int64
    assert np.dtype(table.schema.field("time").type.to_pandas_dtype()).kind == "i"
    
def test_coerce_time_empty_and_str():
    t = DataTransformer(0,0,0,0,".")
    # Empty series
    s = pd.Series([], dtype="float64")
    out = t._coerce_time_to_ms(s, 123456)
    assert all(out == 123456)
    # Strings
    s2 = pd.Series(["2020-01-01T00:00Z"])
    out2 = t._coerce_time_to_ms(s2, 0)
    assert out2.iloc[0] > 1_500_000_000_000

def test_normalizes_longitude(monkeypatch, tmp_path):
    df = pd.DataFrame({
        "latitude":[25.0],
        "longitude":[280.0],  # >180, should subtract 360
        "time":[1577836800000],
        "LAI":[1.0],
        "FAPAR":[0.1],
    })
    class FakeDS:
        def to_dataframe(self): return df
        def __enter__(self): return self
        def __exit__(self,*a): return False
    monkeypatch.setattr("transformation.data_transformer.xr.open_dataset", lambda *a, **k: FakeDS())
    outdir = tmp_path / "out"
    t = DataTransformer(20,30,-90,-80,str(outdir))
    nc_file = tmp_path / "foo_20200101.nc"
    nc_file.write_text("x")
    p = t.process(str(nc_file))
    import pyarrow.parquet as pq
    table = pq.read_table(p)
    lon = table.column("longitude")[0].as_py()
    assert lon < 0