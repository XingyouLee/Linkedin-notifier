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
