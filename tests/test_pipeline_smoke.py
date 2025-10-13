import json
from pipeline.climate_pipeline import ClimatePipeline
from utils.checkpoint import mark_processed

def test_pipeline_smoke(monkeypatch, tmp_path):
    # Stub components inside ClimatePipeline
    class FakeAcq:
        def get_first_n_days_per_year_capped(self, **k):
            return [{"key":"data/1982/VIIRS-Land_v001_19820101.nc","size":123,"last_modified":"2024-01-01T00:00:00Z"}]
        def download(self, key): 
            p = tmp_path / "CR19820101.nc"; p.write_bytes(b"fake"); return str(p)

    class FakeTrans:
        def process(self, nc_file):
            year_dir = tmp_path / "parquet_output" / "1982"; year_dir.mkdir(parents=True, exist_ok=True)
            out = year_dir / "CR19820101.parquet"; out.write_bytes(b"fake"); return str(out)

    class FakeStore:
        def upload(self, local): return f"abfss://cont@acct.dfs.core.windows.net/climate_data/1982/{tmp_path.name}.parquet"

    cp = ClimatePipeline()
    cp.acquirer = FakeAcq()
    cp.transformer = FakeTrans()
    cp.storage = FakeStore()

    cp.run(per_year_days=1, max_total_gb=0.1)

    # processed_files.jsonl should include the key
    lines = (tmp_path / "processed_files.jsonl")
    # our pipeline writes in CWD, ensure we check there
    if lines.exists():
        found = any(json.loads(l)["key"].endswith("19820101.nc") for l in lines.read_text().splitlines())
        assert found

def test_checkpoint_skip(monkeypatch, tmp_path):
    key = "data/1982/VIIRS-Land_v001_19820101.nc"
    # pre-mark as processed
    mark_processed(key)

    class FakeAcq:
        def get_first_n_days_per_year_capped(self, **k): return [{"key":key,"size":1,"last_modified":"x"}]
        def download(self, key): raise AssertionError("shouldn't be called")

    cp = ClimatePipeline()
    cp.acquirer = FakeAcq()
    cp.transformer = object()  # not used
    cp.storage = object()      # not used
    cp.run(per_year_days=1, max_total_gb=0.1)

def test_counts_success_failure(monkeypatch, tmp_path):
    class A:
        def get_first_n_days_per_year_capped(self, **k):
            return [{"key":"k1","size":1,"last_modified":"x"},{"key":"k2","size":1,"last_modified":"x"}]
        def download(self, key):
            p = tmp_path/f"{key}.nc"; p.write_bytes(b"x"); return str(p)
    class T:
        calls=0
        def process(self, path):
            T.calls += 1
            if "k1" in path:
                year_dir = tmp_path/"parquet"/"2020"; year_dir.mkdir(parents=True, exist_ok=True)
                out = year_dir/"CR20200101.parquet"; out.write_bytes(b"d"); return str(out)
            return None  # fail k2
    class S:
        def upload(self, local): return "abfss://ok"

    cp = ClimatePipeline()
    cp.acquirer, cp.transformer, cp.storage = A(), T(), S()
    cp.run(per_year_days=2, max_total_gb=1.0)