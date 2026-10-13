from dags import process


def _search_config(**overrides):
    config = {
        "profile_id": 1,
        "profile_key": "xingyou",
        "search_config_id": 10,
        "search_config_name": "xingyou-data",
        "location": "Netherlands",
        "geo_id": "102890719",
        "distance": 50,
        "hours_old": 120,
        "results_per_term": 300,
        "terms": ["Data Engineer"],
    }
    config.update(overrides)
    return config


def _collect(monkeypatch, search_config):
    calls = []

    class DummySession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_scan_one_term(*, session, term, target_count, scan_config):
        calls.append(
            {
                "term": term,
                "target_count": target_count,
                "hours_old": scan_config["hours_old"],
            }
        )
        return [{"id": f"{term}-{scan_config['hours_old']}"}]

    monkeypatch.setattr(process.requests, "Session", DummySession)
    monkeypatch.setattr(process, "_scan_one_term", fake_scan_one_term)
    monkeypatch.setattr(process.time, "sleep", lambda seconds: None)
    rows = process._collect_scan_rows([search_config])
    return calls, rows


def test_collect_scan_rows_scans_recent_window_before_lookback(monkeypatch):
    calls, rows = _collect(monkeypatch, _search_config())

    assert [call["hours_old"] for call in calls] == [24, 120]
    assert [call["term"] for call in calls] == ["Data Engineer", "Data Engineer"]
    assert calls[0]["target_count"] == 300
    assert len(rows) == 2


def test_collect_scan_rows_skips_lookback_when_hours_old_is_within_recent_window(
    monkeypatch,
):
    calls, rows = _collect(monkeypatch, _search_config(hours_old=24))

    assert [call["hours_old"] for call in calls] == [24]
    assert len(rows) == 1


def test_scan_windows_use_default_lookback_when_hours_old_is_missing():
    assert process._scan_windows(None) == [("recent", 24), ("lookback", 120)]
    assert process._scan_windows(12) == [("recent", 24)]
    assert process._scan_windows(120) == [("recent", 24), ("lookback", 120)]
