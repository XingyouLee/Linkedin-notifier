"""Fixture-backed contract tests for notifier integration."""

from __future__ import annotations

import json
from pathlib import Path

from app.routers import linkedin_notifier as linkedin_notifier_router


def _resolve_fixtures_dir() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        candidate = parent / "docs" / "contracts" / "fixtures"
        if (candidate / "launch-token.json").exists():
            return candidate
    raise FileNotFoundError("Could not locate docs/contracts/fixtures for notifier contract tests.")


FIXTURES_DIR = _resolve_fixtures_dir()


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


class _CaptureCursor:
    def __init__(self, row: dict[str, object]):
        self._row = row
        self.sql: str | None = None
        self.params: tuple[object, ...] | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover - stub
        return False

    def execute(self, sql: str, params: tuple[object, ...]):
        self.sql = sql
        self.params = params

    def fetchone(self) -> dict[str, object]:
        return self._row


class _CaptureConnection:
    def __init__(self, cursor: _CaptureCursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover - stub
        return False

    def cursor(self):
        return self._cursor


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.split())


def test_resume_matcher_verifies_notifier_launch_token_fixture(monkeypatch):
    fixture = _load_fixture("launch-token.json")
    monkeypatch.setenv("MATERIALS_LINK_SECRET", fixture["secret"])

    payload = linkedin_notifier_router._verify_launch_token(fixture["token"])

    assert payload == fixture["payload"]


def test_fetch_launch_record_query_matches_contract_fixture(monkeypatch):
    fixture = _load_fixture("launch-query-shape.json")
    row = {field: f"value-for-{field}" for field in fixture["required_result_fields"]}
    cursor = _CaptureCursor(row)

    monkeypatch.setattr(
        linkedin_notifier_router,
        "_require_notifier_setting",
        lambda name: "postgresql://contracts.test/db",
    )
    monkeypatch.setattr(
        linkedin_notifier_router,
        "connect",
        lambda *args, **kwargs: _CaptureConnection(cursor),
    )

    result = linkedin_notifier_router._fetch_launch_record(
        profile_id=fixture["binds"]["profile_id"],
        job_id=fixture["binds"]["job_id"],
    )

    assert result == row
    assert cursor.params == (
        fixture["binds"]["profile_id"],
        fixture["binds"]["job_id"],
    )

    normalized_sql = _normalize_sql(cursor.sql or "")
    for fragment in (
        *fixture["selected_columns"],
        *fixture["tables"],
        *fixture["where_clauses"],
    ):
        assert fragment in normalized_sql
