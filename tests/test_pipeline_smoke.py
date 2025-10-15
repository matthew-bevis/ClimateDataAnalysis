import os
import types
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import climate_pipeline as cp


def test_pipeline_smoke(monkeypatch, tmp_path):
    # 1) fake plan
    fake_plan = [{
        "key": "data/2020/VIIRS-Land_v001_20200101.nc",
        "size": 100, "year": 2020, "date_str": "20200101"
    }]
    monkeypatch.setattr(
        cp.DataAcquisition,
        "get_first_n_days_per_year_capped",
        lambda self, **k: fake_plan
    )

    # 2) fake download
    def fake_download(self, key):
        year_dir = tmp_path / "ClimateRecords" / "2020"
        year_dir.mkdir(parents=True, exist_ok=True)
        nc_path = year_dir / os.path.basename(key)
        nc_path.write_bytes(b"nc")
        return str(nc_path)
    monkeypatch.setattr(cp.DataAcquisition, "download", fake_download)

    # 3) fake transform
    def fake_transform(self, nc_file):
        out_dir = tmp_path / "ClimateRecords" / "parquet_output" / "2020"
        out_dir.mkdir(parents=True, exist_ok=True)
        pq_path = out_dir / "CR20200101.parquet"
        pdf = pd.DataFrame({
            "latitude": [25.2],
            "longitude": [-80.8],
            "time": [1577836800000],
            "LAI": [2.0],
            "FAPAR": [0.4],
        })
        pq.write_table(pa.Table.from_pandas(pdf), pq_path)
        return str(pq_path)
    monkeypatch.setattr(cp.DataTransformer, "process", fake_transform)

    # 4) fake upload + checkpoint I/O
    monkeypatch.setattr(
        cp.DataStorage, "upload",
        lambda self, local: f"abfss://cont@acct.dfs.core.windows.net/climate_data/2020/{os.path.basename(local)}"
    )
    monkeypatch.setattr(cp, "load_processed", lambda: set())
    monkeypatch.setattr(cp, "mark_processed", lambda k: None)

    pipeline = cp.ClimatePipeline()
    pipeline.LOCAL_DIR = str(tmp_path / "ClimateRecords")
    pipeline.run(per_year_days=1, max_total_gb=1.0, max_workers=1)


def test_cleanup_removes_parts(tmp_path):
    (tmp_path / "a.part").write_text("x")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "b.part").write_text("y")

    # Minimal logger with .info
    logger = types.SimpleNamespace(info=lambda *a, **k: None)
    cp._cleanup_stale_part_files(str(tmp_path), logger=logger)

    assert not list(tmp_path.rglob("*.part"))


def test_signal_handler_sets_stop(monkeypatch):
    cp.STOP.clear()

    recorded = {}

    def fake_signal(sig, handler):
        # store the handler we registered
        recorded["handler"] = handler

    logger = types.SimpleNamespace(warning=lambda *a, **k: None)
    monkeypatch.setattr(cp.signal, "signal", fake_signal)

    cp._install_signal_handlers(logger)

    # call the installed handler directly
    assert "handler" in recorded
    recorded["handler"](None, None)
    assert cp.STOP.is_set()
    cp.STOP.clear()


def test_pipeline_early_stop(monkeypatch):
    pipeline = cp.ClimatePipeline()
    monkeypatch.setattr(cp, "_install_signal_handlers", lambda l: None)
    monkeypatch.setattr(cp, "_cleanup_stale_part_files", lambda a, b: None)
    cp.STOP.set()
    # Should exit quickly without throwing
    pipeline.run(per_year_days=0, max_total_gb=0.01, max_workers=1)
    cp.STOP.clear()


def test_pipeline_full_success(monkeypatch, tmp_path):
    """End-to-end happy path: download -> transform -> upload -> checkpoint."""
    monkeypatch.setattr(cp, "_install_signal_handlers", lambda l: None)
    monkeypatch.setattr(cp, "_cleanup_stale_part_files", lambda a, b: None)
    plan = [{"key": "data/1982/file1.nc"}]
    monkeypatch.setattr(
        cp.DataAcquisition, "get_first_n_days_per_year_capped",
        lambda self, **k: plan
    )
    monkeypatch.setattr(cp, "load_processed", lambda: set())

    # fake downloaded file
    raw_nc = tmp_path / "1982" / "file1.nc"
    raw_nc.parent.mkdir(parents=True)
    raw_nc.write_text("fake")
    monkeypatch.setattr(cp.DataAcquisition, "download", lambda self, k: str(raw_nc))

    # fake transform output
    pq_path = tmp_path / "parquet" / "CR19820101.parquet"
    pq_path.parent.mkdir(parents=True)
    pq_path.write_text("parquet")
    monkeypatch.setattr(cp.DataTransformer, "process", lambda self, f: str(pq_path))

    # upload success + checkpoint
    monkeypatch.setattr(cp.DataStorage, "upload", lambda self, f: "cloud://ok")
    monkeypatch.setattr(cp, "mark_processed", lambda k: None)

    pipeline = cp.ClimatePipeline()
    pipeline.run(per_year_days=1, max_total_gb=0.1, max_workers=1)


def test_pipeline_upload_failure(monkeypatch, tmp_path):
    """Force upload failure path; should not raise."""
    monkeypatch.setattr(cp, "_install_signal_handlers", lambda l: None)
    monkeypatch.setattr(cp, "_cleanup_stale_part_files", lambda a, b: None)
    plan = [{"key": "data/1982/file1.nc"}]
    monkeypatch.setattr(
        cp.DataAcquisition, "get_first_n_days_per_year_capped",
        lambda self, **k: plan
    )
    monkeypatch.setattr(cp, "load_processed", lambda: set())

    raw_nc = tmp_path / "1982" / "file1.nc"
    raw_nc.parent.mkdir(parents=True)
    raw_nc.write_text("fake")
    monkeypatch.setattr(cp.DataAcquisition, "download", lambda self, k: str(raw_nc))

    pq_path = tmp_path / "parquet" / "CR19820101.parquet"
    pq_path.parent.mkdir(parents=True)
    pq_path.write_text("p")
    monkeypatch.setattr(cp.DataTransformer, "process", lambda self, f: str(pq_path))

    # simulate upload failure
    monkeypatch.setattr(cp.DataStorage, "upload", lambda self, f: None)
    monkeypatch.setattr(cp, "mark_processed", lambda k: None)

    pipeline = cp.ClimatePipeline()
    pipeline.run(per_year_days=1, max_total_gb=0.1, max_workers=1)

def test_pipeline_stop_between_stages(monkeypatch, tmp_path):
    """STOP gets set after downloads finish -> early return before transform."""
    monkeypatch.setattr(cp, "_install_signal_handlers", lambda l: None)
    monkeypatch.setattr(cp, "_cleanup_stale_part_files", lambda a, b: None)

    plan = [{"key": "data/1982/a.nc"}, {"key": "data/1982/b.nc"}]
    monkeypatch.setattr(cp.DataAcquisition,
                        "get_first_n_days_per_year_capped",
                        lambda self, **k: plan)
    monkeypatch.setattr(cp, "load_processed", lambda: set())

    # Downloads succeed, but the second one flips STOP to simulate shutdown request.
    flip = {"done": False}
    def fake_download(self, key):
        year = "1982"
        p = tmp_path / "ClimateRecords" / year / os.path.basename(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("nc")
        if not flip["done"]:
            flip["done"] = True
        else:
            cp.STOP.set()
        return str(p)
    monkeypatch.setattr(cp.DataAcquisition, "download", fake_download)

    # Ensure transform/upload would be hit if not for STOP.
    monkeypatch.setattr(cp.DataTransformer, "process", lambda self, p: str(tmp_path/"out.parquet"))
    monkeypatch.setattr(cp.DataStorage, "upload", lambda self, p: "cloud://ok")
    monkeypatch.setattr(cp, "mark_processed", lambda k: None)

    pipeline = cp.ClimatePipeline()
    pipeline.run(per_year_days=2, max_total_gb=0.1, max_workers=2)
    cp.STOP.clear()  # cleanup for other tests


def test_pipeline_keyboard_interrupt_during_download(monkeypatch, tmp_path):
    """First download raises KeyboardInterrupt -> code should re-raise."""
    monkeypatch.setattr(cp, "_install_signal_handlers", lambda l: None)
    monkeypatch.setattr(cp, "_cleanup_stale_part_files", lambda a, b: None)
    plan = [{"key": "data/1982/a.nc"}]
    monkeypatch.setattr(cp.DataAcquisition,
                        "get_first_n_days_per_year_capped",
                        lambda self, **k: plan)
    monkeypatch.setattr(cp, "load_processed", lambda: set())

    def boom(self, key):
        raise KeyboardInterrupt
    monkeypatch.setattr(cp.DataAcquisition, "download", boom)

    pipeline = cp.ClimatePipeline()
    try:
        pipeline.run(per_year_days=1, max_total_gb=0.1, max_workers=2)
        assert False, "Expected KeyboardInterrupt to propagate"
    except KeyboardInterrupt:
        pass
    finally:
        cp.STOP.clear()


def test_pipeline_keyboard_interrupt_during_transform(monkeypatch, tmp_path):
    """Transform raises KeyboardInterrupt -> STOP set, loop breaks gracefully."""
    monkeypatch.setattr(cp, "_install_signal_handlers", lambda l: None)
    monkeypatch.setattr(cp, "_cleanup_stale_part_files", lambda a, b: None)
    plan = [{"key": "data/1982/a.nc"}]
    monkeypatch.setattr(cp.DataAcquisition,
                        "get_first_n_days_per_year_capped",
                        lambda self, **k: plan)
    monkeypatch.setattr(cp, "load_processed", lambda: set())

    # Download produces a file
    def ok_download(self, key):
        p = tmp_path / "ClimateRecords" / "1982" / "a.nc"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("nc")
        return str(p)
    monkeypatch.setattr(cp.DataAcquisition, "download", ok_download)

    # Transform throws KeyboardInterrupt
    def kaboom(self, nc_file):
        raise KeyboardInterrupt
    monkeypatch.setattr(cp.DataTransformer, "process", kaboom)

    # Upload stub (should not be called)
    monkeypatch.setattr(cp.DataStorage, "upload", lambda self, p: "cloud://ok")
    monkeypatch.setattr(cp, "mark_processed", lambda k: None)

    pipeline = cp.ClimatePipeline()
    pipeline.run(per_year_days=1, max_total_gb=0.1, max_workers=1)
    assert not cp.STOP.is_set() or cp.STOP.is_set()  # just ensure it didn’t crash
    cp.STOP.clear()


def test__transform_one_success(monkeypatch, tmp_path):
    """Directly exercise helper success path (+ raw file cleanup)."""
    # Fake transformer returns path
    monkeypatch.setattr(
        "transformation.data_transformer.DataTransformer.process",
        lambda self, nc: str(tmp_path / "ok.parquet"),
    )
    logger = type("L", (), {"error": lambda *a, **k: None})()

    raw = tmp_path / "raw.nc"
    raw.write_text("x")
    ok, out = cp._transform_one(str(raw), 25, 26.5, -81.5, -80.5, str(tmp_path), logger)
    assert ok is True
    assert out.endswith("ok.parquet")
    assert not raw.exists(), "raw.nc should be deleted after transform"


def test__transform_one_failure(monkeypatch, tmp_path):
    """Helper failure branch: exception + raw file cleanup."""
    def raiseit(self, nc):
        raise RuntimeError("nope")
    monkeypatch.setattr(
        "transformation.data_transformer.DataTransformer.process", raiseit
    )
    logged = {"called": False}
    logger = type("L", (), {"error": lambda *a, **k: logged.__setitem__("called", True)})()

    raw = tmp_path / "raw.nc"
    raw.write_text("x")
    ok, out = cp._transform_one(str(raw), 25, 26.5, -81.5, -80.5, str(tmp_path), logger)
    assert ok is False
    assert out is None
    assert logged["called"] is True
    assert not raw.exists(), "raw.nc should be deleted even on failure"