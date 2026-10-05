from pathlib import Path


PROCESS_SOURCE = (Path(__file__).resolve().parents[1] / "dags" / "process.py").read_text()


def test_process_scan_params_include_geo_id():
    assert '"geoId"' in PROCESS_SOURCE
    assert "geo_id: str | None" in PROCESS_SOURCE
    assert 'params["geoId"] = str(geo_id).strip()' in PROCESS_SOURCE


def test_process_scan_headers_include_geo_id_in_referer():
    assert "def _build_scan_headers(" in PROCESS_SOURCE
    assert '"Accept-Language"' in PROCESS_SOURCE
    assert '"Referer"' in PROCESS_SOURCE
    assert 'referer_params["geoId"] = str(geo_id).strip()' in PROCESS_SOURCE
    assert "urlencode(referer_params)" in PROCESS_SOURCE
