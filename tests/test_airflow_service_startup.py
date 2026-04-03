import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
START_SCRIPT = REPO_ROOT / "scripts" / "start_airflow_service.sh"


def _run_start_script(*, extra_env=None):
    env = os.environ.copy()
    env.update(extra_env or {})
    return subprocess.run(
        ["bash", str(START_SCRIPT)],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
    )


def test_requires_airflow_metadata_database_url():
    result = _run_start_script(
        extra_env={
            "AIRFLOW_START_DRY_RUN": "1",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "levi:admin",
            "JOBS_DB_URL": "postgresql://jobs-user:jobs-pass@db:5432/jobsdb",
        }
    )

    assert result.returncode != 0
    assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" in result.stderr


def test_requires_business_database_url():
    result = _run_start_script(
        extra_env={
            "AIRFLOW_START_DRY_RUN": "1",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://airflow-user:airflow-pass@db:5432/airflow",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "levi:admin",
        }
    )

    assert result.returncode != 0
    assert "JOBS_DB_URL" in result.stderr


def test_requires_non_default_simple_auth_users_when_using_simple_auth_manager():
    result = _run_start_script(
        extra_env={
            "AIRFLOW_START_DRY_RUN": "1",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://airflow-user:airflow-pass@db:5432/airflow",
            "JOBS_DB_URL": "postgresql://jobs-user:jobs-pass@db:5432/jobsdb",
        }
    )

    assert result.returncode != 0
    assert "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS" in result.stderr


def test_all_in_one_dry_run_prints_expected_commands():
    result = _run_start_script(
        extra_env={
            "AIRFLOW_START_DRY_RUN": "1",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://airflow-user:airflow-pass@db:5432/airflow",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "levi:admin",
            "JOBS_DB_URL": "postgresql://jobs-user:jobs-pass@db:5432/jobsdb",
            "PORT": "19090",
        }
    )

    assert result.returncode == 0
    assert "AIRFLOW_SERVICE_ROLE=all-in-one" in result.stdout
    assert "AIRFLOW__CORE__AUTH_MANAGER=" in result.stdout
    assert "airflow db migrate" in result.stdout
    assert "airflow scheduler" in result.stdout
    assert "airflow dag-processor" in result.stdout
    assert "airflow api-server -H 0.0.0.0 -p 19090 --proxy-headers" in result.stdout


def test_api_server_role_uses_default_port():
    result = _run_start_script(
        extra_env={
            "AIRFLOW_START_DRY_RUN": "1",
            "AIRFLOW_SERVICE_ROLE": "api-server",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://airflow-user:airflow-pass@db:5432/airflow",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "levi:admin",
            "JOBS_DB_URL": "postgresql://jobs-user:jobs-pass@db:5432/jobsdb",
        }
    )

    assert result.returncode == 0
    assert "AIRFLOW_SERVICE_ROLE=api-server" in result.stdout
    assert "airflow api-server -H 0.0.0.0 -p 8080 --proxy-headers" in result.stdout


def test_rejects_unknown_service_role():
    result = _run_start_script(
        extra_env={
            "AIRFLOW_START_DRY_RUN": "1",
            "AIRFLOW_SERVICE_ROLE": "mystery-role",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://airflow-user:airflow-pass@db:5432/airflow",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "levi:admin",
            "JOBS_DB_URL": "postgresql://jobs-user:jobs-pass@db:5432/jobsdb",
        }
    )

    assert result.returncode != 0
    assert "Unsupported AIRFLOW_SERVICE_ROLE" in result.stderr
