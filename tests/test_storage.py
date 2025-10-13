import os
import pytest
from storage.data_storage import DataStorage

class _BlobClient:
    def __init__(self): self.uploaded = None
    def upload_blob(self, data, overwrite=False): 
        self.uploaded = (data.read(), overwrite)

class _BlobServiceClient:
    def __init__(self): self.created=[]
    def create_container(self, name): self.created.append(name)
    def get_blob_client(self, container, blob):
        bc = _BlobClient(); bc.container=container; bc.blob=blob; return bc

def test_upload_builds_path(monkeypatch, tmp_path):
    # Patch SDK class constructor
    monkeypatch.setattr("storage.data_storage.BlobServiceClient",
                        lambda account_url, credential: _BlobServiceClient())

    ds = DataStorage(container_name="climate-data-analysis", base_dir="climate_data")
    # create a fake parquet file within year folder
    year_dir = tmp_path / "parquet_output" / "2020"
    year_dir.mkdir(parents=True)
    f = year_dir / "CR20200101.parquet"
    f.write_bytes(b"PARQUET")

    cloud_uri = ds.upload(str(f))
    # abfss://climate-data-analysis@fakeacct.dfs.core.windows.net/climate_data/2020/CR20200101.parquet
    assert "abfss://climate-data-analysis@fakeacct.dfs.core.windows.net/climate_data/2020/CR20200101.parquet" in cloud_uri

def test_missing_env_vars(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_KEY", raising=False)
    with pytest.raises(KeyError):
        DataStorage()

def test_upload_failure(monkeypatch, tmp_path):
    class _BlobClient:
        def upload_blob(self, data, overwrite=True): raise RuntimeError("nope")
    class _Svc:
        def create_container(self,*a,**k): pass
        def get_blob_client(self, container, blob): return _BlobClient()
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT","acct")
    monkeypatch.setenv("AZURE_STORAGE_KEY","key")
    monkeypatch.setattr("storage.data_storage.BlobServiceClient", lambda **k: _Svc())

    ds = DataStorage(container_name="c", base_dir="b")
    p = tmp_path/"2020"/"CR20200101.parquet"; p.parent.mkdir(parents=True); p.write_bytes(b"x")
    assert ds.upload(str(p)) is None  # logged error path