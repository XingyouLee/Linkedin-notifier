from pathlib import Path


def test_dockerfile_makes_playwright_browser_install_opt_in():
    dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
    content = dockerfile.read_text()

    assert "ARG INSTALL_PLAYWRIGHT_CHROMIUM=0" in content
    assert 'if [ "$INSTALL_PLAYWRIGHT_CHROMIUM" = "1" ]; then' in content
    assert "python -m playwright install chromium" in content
