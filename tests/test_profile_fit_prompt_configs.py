from __future__ import annotations

import json
from pathlib import Path


PROFILES_PATH = Path(__file__).resolve().parents[1] / "include" / "user_info" / "profiles.json"


def _load_profiles() -> dict[str, dict]:
    profiles = json.loads(PROFILES_PATH.read_text(encoding="utf-8"))
    return {profile["display_name"]: profile for profile in profiles}


def test_xingyou_profile_prefers_pipeline_work_over_bi_reporting_roles():
    profile = _load_profiles()["Xingyou Li"]

    summary = profile["candidate_summary"]
    prompt = profile["fit_prompt"]

    assert "Adjacent BI/analyst roles are only credible" in summary["summary"]
    assert "Pure dashboard/reporting ownership" in summary["obvious_gaps"]
    assert "dashboarding, stakeholder reporting, campaign/eCommerce insights, or BI tooling support" in prompt
    assert "Pure dashboarding, business-insights, marketing analytics, or stakeholder-reporting roles should usually be Weak Fit" in prompt



def test_george_profile_penalizes_generic_new_grad_and_fullstack_posts():
    profile = _load_profiles()["George Gu"]

    summary = profile["candidate_summary"]
    prompt = profile["fit_prompt"]

    assert "Generic software or full-stack roles are only credible when backend/data work clearly dominates" in summary["summary"]
    assert "Software Engineer (backend/data-focused)" in summary["target_roles"]
    assert "Broad generalist full-stack ownership" in summary["obvious_gaps"]
    assert "Software Engineer 1, Graduate Software Engineer, or New Grad Software Engineer" in prompt
    assert "default to Weak Fit unless backend/data scope is explicit and central" in prompt
    assert "broad rotation/team-placement language should default to Weak Fit" in prompt


def test_test_mode_profile_routes_to_xingyou_channel_with_exact_scan_limits():
    profiles = _load_profiles()

    xingyou_profile = profiles["Xingyou Li"]
    test_profiles = [
        profile
        for profile in profiles.values()
        if profile.get("test_mode_only") or profile.get("is_test_profile")
    ]

    assert len(test_profiles) == 1

    test_profile = test_profiles[0]
    assert test_profile["discord_channel_id"] == xingyou_profile["discord_channel_id"]
    assert test_profile.get("bootstrap_existing_jobs") is False

    search_configs = test_profile["search_configs"]
    assert len(search_configs) == 1

    search_config = search_configs[0]
    assert search_config["results_per_term"] == 20
    assert len(search_config["terms"]) == 2
