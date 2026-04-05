from pathlib import Path


def test_requirements_drop_legacy_playwright_and_jobspy_dependencies():
    requirements = (Path(__file__).resolve().parents[1] / "requirements.txt").read_text()

    assert "python-jobspy" not in requirements
    assert "playwright" not in requirements


def test_legacy_playwright_worker_file_is_removed():
    worker_path = Path(__file__).resolve().parents[1] / "dags" / "jd_playwright_worker.py"
    fallback_script = Path(__file__).resolve().parents[1] / "scripts" / "jd_playwright_worker.py"

    assert not worker_path.exists()
    assert fallback_script.exists()
