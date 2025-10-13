
import os
import io
import re
import pytest
from acquisition.data_acquisition import DataAcquisition, NAMESPACE

def _xml_page(keys, next_token=None):
    items = "".join([
        f"""
        <Contents xmlns="{NAMESPACE}">
          <Key>{k}</Key>
          <Size>12345</Size>
          <LastModified>2024-03-01T00:00:00Z</LastModified>
        </Contents>
        """ for k in keys
    ])
    nt = f"<NextContinuationToken xmlns=\"{NAMESPACE}\">{next_token}</NextContinuationToken>" if next_token else ""
    return f"""<ListBucketResult xmlns="{NAMESPACE}">{items}{nt}</ListBucketResult>"""

def test_list_and_cap(monkeypatch, tmp_path):
    # Two pages of results
    page1 = _xml_page([
        "data/1981/AVHRR-Land_v001_19810101.nc",
        "data/1981/VIIRS-Land_v001_19810105.nc",
        "data/1982/VIIRS-Land_v001_19820101.nc",
        "data/1982/VIIRS-Land_v001_19820102.nc",
        "data/garbage/readme.txt"
    ], next_token="abc")
    page2 = _xml_page([
        "data/1983/VIIRS-Land_v001_19830101.nc",
    ])

    class R:
        def __init__(self, content): self.content = content.encode(); self.status_code=200
        def raise_for_status(self): pass

    calls = {"n":0}
    def fake_get(url, *a, **k):
        calls["n"] += 1
        return R(page1 if "continuation-token" not in url else page2)

    monkeypatch.setattr("acquisition.data_acquisition.requests.get", fake_get)

    acq = DataAcquisition("https://bucket", str(tmp_path))
    all_objs = acq._list_objects()
    assert len(all_objs) == 5

    plan = acq.get_first_n_days_per_year_capped(per_year_days=1, years=range(1981,1984), max_total_bytes=10**9)
    # should choose first per year in order
    assert [re.search(r"(\d{8})", r["key"]).group(1) for r in plan] == ["19810101","19820101","19830101"]

def test_list_objects_ignores_non_nc(monkeypatch):
    xml = f"""
    <ListBucketResult xmlns="{NAMESPACE}">
      <Contents><Key>data/1982/VIIRS-Land_v001_19820101.nc</Key><Size>10</Size><LastModified>2024-01-01T00:00:00Z</LastModified></Contents>
      <Contents><Key>data/README.txt</Key><Size>1</Size><LastModified>2024-01-01T00:00:00Z</LastModified></Contents>
    </ListBucketResult>"""
    class R: status_code=200; 
    def fake_get(url,*a,**k): 
        r=R(); r.content=xml.encode(); r.raise_for_status=lambda:None; return r
    monkeypatch.setattr("acquisition.data_acquisition.requests.get", fake_get)

    acq = DataAcquisition("https://bucket","/tmp")
    out = acq._list_objects()
    assert len(out) == 1 and out[0]["key"].endswith(".nc")

def test_list_objects_http_error(monkeypatch):
    class R: 
        status_code=500
        content=b""
        def raise_for_status(self): raise RuntimeError("boom")
    monkeypatch.setattr("acquisition.data_acquisition.requests.get", lambda *a,**k: R())
    acq = DataAcquisition("https://bucket","/tmp")
    with pytest.raises(RuntimeError):
        acq._list_objects()

def test_bad_xml_tolerated(monkeypatch):
    class R: 
        status_code=200; content=b"<not-xml"
        def raise_for_status(self): pass
    monkeypatch.setattr("acquisition.data_acquisition.requests.get", lambda *a,**k: R())
    acq = DataAcquisition("https://bucket","/tmp")
    # Should not raise; should return empty
    assert acq._list_objects() == []

def test_size_cap_enforced(monkeypatch):
    xml = f"""
    <ListBucketResult xmlns="{NAMESPACE}">
      <Contents><Key>data/1982/VIIRS-Land_v001_19820101.nc</Key><Size>100</Size><LastModified>2024-01-01T00:00:00Z</LastModified></Contents>
      <Contents><Key>data/1982/VIIRS-Land_v001_19820102.nc</Key><Size>100</Size><LastModified>2024-01-01T00:00:00Z</LastModified></Contents>
      <Contents><Key>data/1982/VIIRS-Land_v001_19820103.nc</Key><Size>100</Size><LastModified>2024-01-01T00:00:00Z</LastModified></Contents>
   </ListBucketResult>"""
    class R: status_code=200; 
    def fake_get(url,*a,**k): r=R(); r.content=xml.encode(); r.raise_for_status=lambda:None; return r
    monkeypatch.setattr("acquisition.data_acquisition.requests.get", fake_get)
    acq = DataAcquisition("https://bucket","/tmp")
    plan = acq.get_first_n_days_per_year_capped(per_year_days=3, years=[1982], max_total_bytes=200)
    # Should only include first two to stay under 200
    assert len(plan) == 2

def test_strip_ns_unit():
    da = DataAcquisition("https://bucket","/tmp")
    assert da._strip_ns("{ns}Contents") == "Contents"
    assert da._strip_ns("Key") == "Key"

def test_pagination(monkeypatch):
    page1 = f"""
    <ListBucketResult xmlns="{NAMESPACE}">
      <Contents><Key>data/1982/VIIRS-Land_v001_19820101.nc</Key><Size>1</Size><LastModified>2024-01-01T00:00:00Z</LastModified></Contents>
      <NextContinuationToken>abc</NextContinuationToken>
    </ListBucketResult>"""
    page2 = f"""
    <ListBucketResult xmlns="{NAMESPACE}">
      <Contents><Key>data/1982/VIIRS-Land_v001_19820102.nc</Key><Size>1</Size><LastModified>2024-01-02T00:00:00Z</LastModified></Contents>
    </ListBucketResult>"""

    class R: status_code=200
    calls = {"n":0}
    def fake_get(url,*a,**k):
        calls["n"] += 1
        r = R()
        r.raise_for_status = lambda: None
        r.content = (page1 if "continuation-token" not in url else page2).encode()
        return r

    monkeypatch.setattr("acquisition.data_acquisition.requests.get", fake_get)
    acq = DataAcquisition("https://bucket","/tmp")
    out = acq._list_objects()
    # both pages included
    assert [o["key"] for o in out] == [
        "data/1982/VIIRS-Land_v001_19820101.nc",
        "data/1982/VIIRS-Land_v001_19820102.nc",
    ]
    assert calls["n"] == 2

def test_warns_on_nc_without_sensor(monkeypatch, caplog):
    xml = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Contents><Key>data/1982/other_19820101.nc</Key><Size>1</Size><LastModified>2024-01-01T00:00:00Z</LastModified></Contents>
    </ListBucketResult>"""
    class R: status_code=200
    def fake_get(url,*a,**k):
        r = R(); r.raise_for_status=lambda: None; r.content = xml.encode(); return r
    monkeypatch.setattr("acquisition.data_acquisition.requests.get", fake_get)

    acq = DataAcquisition("https://bucket","/tmp")
    with caplog.at_level("WARNING"):
        out = acq._list_objects()
    # skipped
    assert out == []
    assert any("without AVHRR/VIIRS" in m for _,_,m in caplog.record_tuples)

def test_download_already_present(tmp_path, monkeypatch):
    acq = DataAcquisition("https://bucket", str(tmp_path))
    key = "data/1982/VIIRS-Land_v001_19820101.nc"
    year_dir = tmp_path / "1982"
    year_dir.mkdir()
    (year_dir / "VIIRS-Land_v001_19820101.nc").write_bytes(b"x")
    # Should early-return without HTTP
    path = acq.download(key)
    assert path.endswith("1982/VIIRS-Land_v001_19820101.nc")

def test_download_success(tmp_path, monkeypatch):
    acq = DataAcquisition("https://bucket", str(tmp_path))
    key = "data/1982/VIIRS-Land_v001_19820102.nc"

    class R:
        status_code=200
        def __enter__(self): return self
        def __exit__(self,*a): pass
        def raise_for_status(self): pass
        def iter_content(self, chunk_size):
            yield b"abc"; yield b"def"
    monkeypatch.setattr("acquisition.data_acquisition.requests.get", lambda *a,**k: R())

    path = acq.download(key)
    with open(path,"rb") as f:
        assert f.read() == b"abcdef"

def test_download_error(tmp_path, monkeypatch):
    acq = DataAcquisition("https://bucket", str(tmp_path))
    key = "data/1982/VIIRS-Land_v001_19820103.nc"

    class R:
        def __enter__(self): return self
        def __exit__(self,*a): pass
        def raise_for_status(self): raise RuntimeError("boom")
    monkeypatch.setattr("acquisition.data_acquisition.requests.get", lambda *a,**k: R())

    assert acq.download(key) is None