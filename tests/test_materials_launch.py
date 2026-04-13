import time

import pytest

from dags import materials_launch


def test_build_and_verify_materials_launch_token_round_trip():
    token = materials_launch.build_materials_launch_token(
        profile_id=11,
        job_id="li-42",
        secret="super-secret",
        expires_at=int(time.time()) + 60,
    )

    payload = materials_launch.verify_materials_launch_token(
        token=token,
        secret="super-secret",
    )

    assert payload["profile_id"] == 11
    assert payload["job_id"] == "li-42"


def test_verify_materials_launch_token_rejects_expired():
    token = materials_launch.build_materials_launch_token(
        profile_id=11,
        job_id="li-42",
        secret="super-secret",
        expires_at=int(time.time()) - 1,
    )

    with pytest.raises(ValueError, match="expired"):
        materials_launch.verify_materials_launch_token(
            token=token,
            secret="super-secret",
        )


def test_build_materials_launch_url_uses_launch_route():
    url = materials_launch.build_materials_launch_url(
        base_url="https://materials.example.com/",
        profile_id=11,
        job_id="li-42",
        secret="super-secret",
        ttl_seconds=60,
    )

    assert url.startswith("https://materials.example.com/launch?token=")
