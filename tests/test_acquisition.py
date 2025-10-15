import os, io, pytest
from acquisition.data_acquisition import DataAcquisition

# -------------------- Helpers --------------------

class _Resp:
    def __init__(self, content=b"<root/>", headers=None, code=200, chunks=None):
        self.content = content
        self.headers = headers or {}
        self.status_code = code
        self._chunks = chunks or [b"x" * 10]
    def raise_for_status(self): 
        if self.status_code != 200: raise RuntimeError("bad")
    def iter_content(self, n): 
        yield from self._chunks
    def __enter__(self): return self
    def __exit__(self,*a): return False

# -------------------- Tests --------------------

def test_parse_listing_xml(monkeypatch):
    xml = b"""
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Contents><Key>data/1981/AVHRR-Land_foo.nc</Key><Size>123</Size><LastModified>x</LastModified></Contents>
      <NextContinuationToken>abc</NextContinuationToken>
    </ListBucketResult>
    """
    acq = DataAcquisition("https://bucket", "tmp")
    objs, token = acq._parse_listing_xml(xml)
    assert objs[0]["key"].endswith(".nc")
    assert token == "abc"

def test_size_cap_enforced(monkeypatch, tmp_path):
    objs = [
        {"key":"data/1982/VIIRS-Land_v001_19820101.nc","size":100,"last_modified":"x"},
        {"key":"data/1982/VIIRS-Land_v001_19820102.nc","size":100,"last_modified":"x"},
        {"key":"data/1982/VIIRS-Land_v001_19820103.nc","size":100,"last_modified":"x"},
    ]
    monkeypatch.setattr(DataAcquisition, "_list_objects", lambda self: objs)
    acq = DataAcquisition("https://bucket", str(tmp_path))
    plan = acq.get_first_n_days_per_year_capped(per_year_days=3, years=[1982], max_total_bytes=200)
    assert len(plan) == 2


def test_download_atomic_success(monkeypatch, tmp_path):
    # Simulate small streaming download
    monkeypatch.setattr("acquisition.data_acquisition.requests.get",
                        lambda url, stream, timeout: _Resp(headers={"Content-Length":"30"},
                                                           chunks=[b"x"*10, b"y"*20]))
    acq = DataAcquisition("https://bucket", str(tmp_path))
    key = "data/1982/VIIRS-Land_v001_19820101.nc"
    out = acq.download(key)
    assert out and os.path.exists(out)
    with open(out,"rb") as f:
        assert b"x" in f.read()


def test_download_size_mismatch(monkeypatch, tmp_path):
    # mismatched size triggers cleanup and None
    monkeypatch.setattr("acquisition.data_acquisition.requests.get",
                        lambda url, stream, timeout: _Resp(headers={"Content-Length":"999"},
                                                           chunks=[b"abc"]))
    acq = DataAcquisition("https://bucket", str(tmp_path))
    key = "data/1982/VIIRS-Land_v001_19820101.nc"
    out = acq.download(key)
    assert out is None
    # .part file should not remain
    assert not list(tmp_path.rglob("*.part"))

def test_download_handles_exception(monkeypatch, tmp_path):
    def bad_get(*a, **k): raise ConnectionError("boom")
    monkeypatch.setattr("acquisition.data_acquisition.requests.get", bad_get)
    acq = DataAcquisition("https://bucket", str(tmp_path))
    assert acq.download("data/1981/foo.nc") is None

def test_strip_ns_variants():
    assert DataAcquisition._strip_ns("{ns}Key") == "Key"
    assert DataAcquisition._strip_ns("NoNS") == "NoNS"

def test_parse_listing_xml_malformed(monkeypatch):
    acq = DataAcquisition("https://bucket", "tmp")
    objs, token = acq._parse_listing_xml(b"<broken><xml")
    assert objs == [] and token is None

def test_list_objects_pagination(monkeypatch):
    xml_page_1 = b"""
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Contents><Key>data/1981/AVHRR-Land_v001_19810101.nc</Key><Size>10</Size><LastModified>x</LastModified></Contents>
        <NextContinuationToken>abc</NextContinuationToken>
    </ListBucketResult>
    """
    xml_page_2 = b"""
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Contents><Key>data/1981/AVHRR-Land_v001_19810102.nc</Key><Size>20</Size><LastModified>y</LastModified></Contents>
    </ListBucketResult>
    """
    calls = []
    def fake_get(url, timeout):
        calls.append(url)
        return type("R",(),{"content": xml_page_1 if len(calls)==1 else xml_page_2, "raise_for_status": lambda self: None})()
    monkeypatch.setattr("acquisition.data_acquisition.requests.get", fake_get)
    acq = DataAcquisition("https://bucket", "tmp")
    out = acq._list_objects()
    assert len(out) == 2
    assert "continuation-token=abc" in calls[-1]

def test_download_existing_file(monkeypatch, tmp_path):
    # existing file with proper date
    path = tmp_path / "1982" / "AVHRR-Land_v005_19820121.nc"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"x")

    acq = DataAcquisition("https://bucket", str(tmp_path))
    result = acq.download("data/1982/AVHRR-Land_v005_19820121.nc")
    assert result == str(path)

