from pathlib import Path

import pytest

from dags import database
from dags import runtime_utils


def test_load_env_keeps_injected_jobs_db_url(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("JOBS_DB_URL=postgresql://from-file/db\n", encoding="utf-8")
    monkeypatch.setenv("JOBS_DB_URL", "postgresql://from-env/db")

    runtime_utils.load_env(env_candidates=[str(env_file)])

    assert database.get_db_url() == "postgresql://from-env/db"


def test_get_db_url_requires_jobs_db_url_in_cloud_mode(monkeypatch):
    monkeypatch.delenv("JOBS_DB_URL", raising=False)
    monkeypatch.setenv("CLOUD_DEPLOYMENT", "1")
    monkeypatch.delenv("JOBS_DB_HOST", raising=False)
    monkeypatch.delenv("JOBS_DB_PORT", raising=False)
    monkeypatch.delenv("JOBS_DB_USER", raising=False)
    monkeypatch.delenv("JOBS_DB_PASSWORD", raising=False)
    monkeypatch.delenv("JOBS_DB_NAME", raising=False)

    with pytest.raises(ValueError, match="JOBS_DB_URL"):
        database.get_db_url()


def test_get_db_url_requires_jobs_db_url_even_outside_cloud_mode(monkeypatch):
    monkeypatch.delenv("JOBS_DB_URL", raising=False)
    monkeypatch.delenv("CLOUD_DEPLOYMENT", raising=False)

    with pytest.raises(ValueError, match="JOBS_DB_URL"):
        database.get_db_url()


def test_get_db_url_ignores_split_jobs_db_variables(monkeypatch):
    monkeypatch.delenv("JOBS_DB_URL", raising=False)
    monkeypatch.delenv("CLOUD_DEPLOYMENT", raising=False)
    monkeypatch.setenv("JOBS_DB_HOST", "legacy-host")
    monkeypatch.setenv("JOBS_DB_PORT", "6543")
    monkeypatch.setenv("JOBS_DB_USER", "legacy-user")
    monkeypatch.setenv("JOBS_DB_PASSWORD", "legacy-password")
    monkeypatch.setenv("JOBS_DB_NAME", "legacy-db")

    with pytest.raises(ValueError, match="JOBS_DB_URL"):
        database.get_db_url()


def test_profile_config_path_error_is_explicit(monkeypatch, tmp_path):
    missing = tmp_path / "missing-profiles.json"
    monkeypatch.setenv("PROFILE_CONFIG_PATH", str(missing))

    with pytest.raises(FileNotFoundError, match="PROFILE_CONFIG_PATH"):
        database._load_profile_configs_from_file(missing)


def test_sync_profiles_fails_for_missing_explicit_profile_config(monkeypatch, tmp_path):
    missing = tmp_path / "missing-profiles.json"
    monkeypatch.setenv("PROFILE_CONFIG_PATH", str(missing))

    with pytest.raises(FileNotFoundError, match="PROFILE_CONFIG_PATH"):
        database._sync_profiles_from_source(None)
