import os
import pandas as pd
import pytest
from transformation.data_transformer import DataTransformer

def test_transformer_filters_and_writes(monkeypatch, fake_xarray_ds, tmp_path):
    # Fake xarray.open_dataset to return our minimal DS
    monkeypatch.setattr("transformation.data_transformer.xr.open_dataset",
                        lambda *a, **k: fake_xarray_ds)

    outdir = tmp_path / "parquet_output"
    t = DataTransformer(
        lat_min=25.0, lat_max=26.5,
        lon_min=-81.5, lon_max=-80.5,
        output_dir=str(outdir),
    )

    # Input file with date in name
    nc_file = tmp_path / "CR20200101.nc"
    nc_file.write_bytes(b"fake")

    parquet_path = t.process(str(nc_file))
    assert parquet_path is not None
    assert os.path.exists(parquet_path)

    df = pd.read_parquet(parquet_path)
    # Expect only rows inside bbox after lon normalization:
    # longitudes: 279.6 -> -80.4 (inside), -80.9 (inside), -120 (out)
    # latitudes: 25.1 (in), 25.4 (in), 30.0 (out)
    assert len(df) == 2
    assert df["longitude"].between(-81.5,-80.5).all()
    assert df["latitude"].between(25.0,26.5).all()
    # LAI/FAPAR persisted
    assert {"LAI","FAPAR"}.issubset(df.columns)

def test_returns_none_on_empty_roi(monkeypatch, tmp_path):
    class DS:
        def __enter__(self): return self
        def __exit__(self,*a): pass
        def to_dataframe(self):
            return pd.DataFrame({"latitude":[10.0], "longitude":[10.0], "LAI":[1.0], "FAPAR":[0.3]})
    monkeypatch.setattr("transformation.data_transformer.xr.open_dataset", lambda *a,**k: DS())

    t = DataTransformer(25,26.5,-81.5,-80.5, str(tmp_path/"out"))
    (tmp_path/"foo.nc").write_bytes(b"x")
    assert t.process(str(tmp_path/"foo.nc")) is None  # outside bbox

def test_no_date_in_filename(monkeypatch, tmp_path, fake_xarray_ds):
    monkeypatch.setattr("transformation.data_transformer.xr.open_dataset", lambda *a,**k: fake_xarray_ds)
    t = DataTransformer(25,26.5,-81.5,-80.5, str(tmp_path/"out"))
    (tmp_path/"no_date.nc").write_bytes(b"x")
    assert t.process(str(tmp_path/"no_date.nc")) is None

def test_missing_lai_fapar_columns(monkeypatch, tmp_path):
    class DS:
        def __enter__(self): return self
        def __exit__(self,*a): pass
        def to_dataframe(self):
            return pd.DataFrame({"latitude":[25.2], "longitude":[-81.0], "time":[pd.Timestamp("2020-01-01")]})
    monkeypatch.setattr("transformation.data_transformer.xr.open_dataset", lambda *a,**k: DS())
    t = DataTransformer(25,26.5,-81.5,-80.5, str(tmp_path/"out"))
    (tmp_path/"CR20200101.nc").write_bytes(b"x")
    # Should still write a parquet with available cols
    path = t.process(str(tmp_path/"CR20200101.nc"))
    assert path and (tmp_path/"out"/"2020").exists()