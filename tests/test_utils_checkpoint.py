import os, json
from utils import checkpoint

def test_mark_and_load(tmp_path):
    f = tmp_path / "processed.jsonl"
    old = checkpoint.CHECKPOINT_FILE
    checkpoint.CHECKPOINT_FILE = str(f)
    checkpoint.mark_processed("foo/bar.nc")
    checkpoint.mark_processed("baz.nc")
    out = checkpoint.load_processed()
    assert "foo/bar.nc" in out and "baz.nc" in out
    checkpoint.CHECKPOINT_FILE = old

def test_load_processed_empty_and_corrupt(tmp_path):
    f = tmp_path / "chk.jsonl"
    f.write_text("{}garbage\n")
    checkpoint.CHECKPOINT_FILE = str(f)
    res = checkpoint.load_processed()
    assert isinstance(res, set)