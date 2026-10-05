"""Tests for JD worker shutdown handling of unfinished (pending/processing) rows."""

import pytest

from dags import database, process


@pytest.fixture
def captured_results(monkeypatch):
    """Record save_jd_result and fail_jd_queue_row calls."""
    calls = []

    def fake_save(job_id, description=None, description_error=None):
        calls.append((job_id, description_error))

    def fake_fail(job_id, error):
        calls.append((job_id, error))

    monkeypatch.setattr(process.database, "save_jd_result", fake_save)
    monkeypatch.setattr(process.database, "fail_jd_queue_row", fake_fail)
    return calls


def _stub_unfinished(monkeypatch, rows):
    monkeypatch.setattr(
        process.database, "get_unfinished_jd_jobs", lambda job_ids: rows
    )


def test_resolve_pending_only_marks_unclaimable(monkeypatch, captured_results):
    _stub_unfinished(monkeypatch, [("1", "pending"), ("2", "pending")])

    resolved = process._resolve_unfinished_jd_jobs(["1", "2"])

    assert resolved is True
    assert captured_results == [
        ("1", "unclaimable_pending_job"),
        ("2", "unclaimable_pending_job"),
    ]


def test_resolve_processing_only_marks_stale(monkeypatch, captured_results):
    _stub_unfinished(monkeypatch, [("7", "processing")])

    resolved = process._resolve_unfinished_jd_jobs(["7"])

    assert resolved is True
    assert captured_results == [("7", "stale_processing_job")]


def test_resolve_mixed_leftovers_uses_state_specific_errors(
    monkeypatch, captured_results
):
    _stub_unfinished(
        monkeypatch,
        [("1", "pending"), ("2", "processing"), ("3", "pending")],
    )

    resolved = process._resolve_unfinished_jd_jobs(["1", "2", "3"])

    assert resolved is True
    assert set(captured_results) == {
        ("1", "unclaimable_pending_job"),
        ("3", "unclaimable_pending_job"),
        ("2", "stale_processing_job"),
    }


def test_resolve_no_unfinished_rows_is_noop(monkeypatch, captured_results):
    _stub_unfinished(monkeypatch, [])

    resolved = process._resolve_unfinished_jd_jobs(["1"])

    assert resolved is False
    assert captured_results == []


# --- prune_unclaimable_jd_queue_rows ---


class DummyCursor:
    def __init__(self, rowcount=0):
        self.rowcount = rowcount
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))


class DummyConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return self._cursor


def test_prune_unclaimable_returns_rowcount(monkeypatch):
    cur = DummyCursor(rowcount=4)
    monkeypatch.setattr(database, "init_db", lambda: None)
    monkeypatch.setattr(database, "_connect", lambda: DummyConn(cur))

    pruned = database.prune_unclaimable_jd_queue_rows()

    assert pruned == 4
    sql = cur.executed[0][0]
    assert "no_active_profile_unclaimable" in sql
    assert "pending" in sql
    assert "processing" in sql
    # must NOT filter by mode — so no is_test_profile clause
    assert "is_test_profile" not in sql


def test_prune_unclaimable_mode_agnostic(monkeypatch):
    """Prune should produce identical SQL regardless of test mode."""
    results = []
    for test_mode in (False, True):
        cur = DummyCursor(rowcount=0)
        monkeypatch.setattr(database, "init_db", lambda: None)
        monkeypatch.setattr(database, "_connect", lambda: DummyConn(cur))
        monkeypatch.setattr(database, "is_test_mode_enabled", lambda tm=test_mode: tm)
        database.prune_unclaimable_jd_queue_rows()
        results.append(cur.executed[0][0])

    assert results[0] == results[1]


def test_fail_jd_queue_row_only_updates_queue(monkeypatch):
    cur = DummyCursor()
    monkeypatch.setattr(database, "init_db", lambda: None)
    monkeypatch.setattr(database, "_connect", lambda: DummyConn(cur))

    database.fail_jd_queue_row("job-123", "stale_processing_job")

    assert len(cur.executed) == 1
    sql, params = cur.executed[0]
    assert "jd_queue" in sql
    assert "jobs" not in sql  # must NOT touch the jobs table
    assert params == ("stale_processing_job", "job-123")
