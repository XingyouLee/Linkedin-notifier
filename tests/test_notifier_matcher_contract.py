import json
from pathlib import Path

import pytest

from dags import materials_launch

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "docs" / "contracts" / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def test_materials_launch_token_matches_contract_fixture():
    fixture = _load_fixture("launch-token.json")

    token = materials_launch.build_materials_launch_token(
        profile_id=fixture["payload"]["profile_id"],
        job_id=fixture["payload"]["job_id"],
        secret=fixture["secret"],
        expires_at=fixture["payload"]["exp"],
    )

    assert token == fixture["token"]
    assert materials_launch.verify_materials_launch_token(
        token=token,
        secret=fixture["secret"],
    ) == fixture["payload"]


def test_materials_launch_url_matches_contract_fixture(monkeypatch: pytest.MonkeyPatch):
    fixture = _load_fixture("launch-token.json")
    monkeypatch.setattr(materials_launch.time, "time", lambda: fixture["issued_at"])

    launch_url = materials_launch.build_materials_launch_url(
        base_url=fixture["base_url"],
        profile_id=fixture["payload"]["profile_id"],
        job_id=fixture["payload"]["job_id"],
        secret=fixture["secret"],
        ttl_seconds=fixture["ttl_seconds"],
    )

    assert launch_url == fixture["launch_url"]
