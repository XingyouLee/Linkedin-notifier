import json

from dags import fitting_notifier


def _candidate_summary(
    *,
    candidate_years=1.5,
    candidate_seniority="junior",
):
    return {
        "summary": "Junior backend and data candidate with early-career production experience.",
        "target_roles": ["Data Engineer", "Backend Engineer"],
        "candidate_years": candidate_years,
        "candidate_seniority": candidate_seniority,
        "core_skills": ["Python", "SQL"],
        "obvious_gaps": [],
        "language_signals": {
            "dutch_level": "basic",
            "english_level": "fluent",
            "notes": "English strong, Dutch limited.",
        },
    }


def test_load_candidate_summary_config_parses_serialized_profile_config():
    loaded = fitting_notifier._load_candidate_summary_config(
        json.dumps(_candidate_summary(candidate_years=2, candidate_seniority="junior"))
    )

    assert loaded["candidate_years"] == 2
    assert loaded["candidate_seniority"] == "junior"
    assert loaded["summary"].startswith("Junior backend")


def test_normalize_candidate_summary_requires_summary_text():
    try:
        fitting_notifier._normalize_candidate_summary(
            {
                "candidate_years": 2,
                "candidate_seniority": "junior",
                "target_roles": ["Data Engineer"],
                "core_skills": ["Python"],
                "obvious_gaps": [],
                "language_signals": {},
            }
        )
    except ValueError as error:
        assert str(error) == "candidate_summary_missing_summary"
    else:  # pragma: no cover - defensive branch
        raise AssertionError("Expected candidate summary contract validation error")


def test_apply_fit_caps_caps_gap_of_three_and_sets_experience_blocker():
    parsed = fitting_notifier._apply_fit_caps(
        {
            "fit_score": 92,
            "decision": "Strong Fit",
            "exp_requirement": "Minimum 5 years of experience",
            "experience_check": {
                "required_years": 5,
            },
            "language_check": {"language_blocker": False},
        },
        job_title="Data Engineer",
        jd_text="Minimum 5 years of experience with data pipelines and SQL.",
        candidate_summary=_candidate_summary(candidate_years=2, candidate_seniority="junior"),
    )

    assert parsed["fit_score"] == 45
    assert parsed["decision"] == "Weak Fit"
    assert parsed["experience_check"]["gap_years"] == 3
    assert parsed["experience_check"]["experience_blocker"] is True


def test_apply_fit_caps_blocks_high_seniority_title_mismatch():
    parsed = fitting_notifier._apply_fit_caps(
        {
            "fit_score": 88,
            "decision": "Strong Fit",
            "exp_requirement": "not specified",
            "experience_check": {},
            "language_check": {"language_blocker": False},
        },
        job_title="Senior Data Engineer",
        jd_text="Build resilient ETL pipelines.",
        candidate_summary=_candidate_summary(candidate_years=2, candidate_seniority="junior"),
    )

    assert parsed["fit_score"] == 40
    assert parsed["decision"] == "Weak Fit"
    assert parsed["experience_check"]["seniority_required"] == "senior"
    assert parsed["experience_check"]["experience_blocker"] is True


def test_apply_fit_caps_caps_large_gap_to_not_recommended():
    parsed = fitting_notifier._apply_fit_caps(
        {
            "fit_score": 90,
            "decision": "Strong Fit",
            "exp_requirement": "8+ years of experience",
            "experience_check": {
                "required_years": 8,
            },
            "language_check": {"language_blocker": False},
        },
        job_title="Data Platform Engineer",
        jd_text="Looking for 8+ years building production-grade data platforms.",
        candidate_summary=_candidate_summary(candidate_years=2, candidate_seniority="junior"),
    )

    assert parsed["fit_score"] == 25
    assert parsed["decision"] == "Not Recommended"
    assert parsed["experience_check"]["experience_blocker"] is True


def test_has_experience_blocker_is_backward_compatible_with_legacy_match_json():
    legacy_match = json.dumps(
        {
            "fit_score": 72,
            "decision": "Moderate Fit",
            "summary": "Legacy row without the new fields.",
        }
    )

    assert fitting_notifier._has_experience_blocker(legacy_match) is False
