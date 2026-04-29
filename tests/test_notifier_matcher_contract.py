import pytest

from dags import database


def _profile(
    *,
    profile_key="default",
    display_name="Default",
    resume_path="resume/candidate.md",
    resume_text="# Candidate",
    is_active=True,
):
    return {
        "profile_key": profile_key,
        "display_name": display_name,
        "resume_path": resume_path,
        "resume_text": resume_text,
        "is_active": is_active,
        "search_configs": [
            {
                "name": "default",
                "location": "Netherlands",
                "distance": 25,
                "hours_old": 24,
                "results_per_term": 10,
                "terms": ["data engineer"],
                "is_active": True,
            }
        ],
    }


def test_collect_matcher_cutover_validation_errors_flags_non_text_and_empty_profiles():
    errors = database._collect_matcher_cutover_validation_errors(
        [
            _profile(
                profile_key="pdf-profile",
                display_name="PDF Profile",
                resume_path="resume/candidate.pdf",
                resume_text="",
            ),
            _profile(
                profile_key="missing-text",
                display_name="Missing Text",
                resume_path="resume/candidate.md",
                resume_text="",
            ),
        ]
    )

    assert len(errors) == 3
    assert any("pdf-profile (PDF Profile)" in error and ".pdf" in error for error in errors)
    assert any("pdf-profile (PDF Profile)" in error and "resume_text is empty" in error for error in errors)
    assert any("missing-text (Missing Text)" in error and "resume_text is empty" in error for error in errors)


def test_collect_matcher_cutover_validation_errors_allows_db_only_resume():
    profile = _profile(resume_path=None, resume_text=None)

    errors = database._collect_matcher_cutover_validation_errors([profile])

    assert errors == []


def test_collect_matcher_cutover_validation_errors_ignores_inactive_profiles():
    errors = database._collect_matcher_cutover_validation_errors(
        [
            _profile(
                profile_key="inactive-pdf",
                display_name="Inactive PDF",
                resume_path="resume/candidate.pdf",
                resume_text="",
                is_active=False,
            )
        ]
    )

    assert errors == []


def test_validate_matcher_cutover_profiles_raises_clear_operator_error():
    with pytest.raises(ValueError) as exc_info:
        database._validate_matcher_cutover_profiles(
            [
                _profile(
                    profile_key="bad-profile",
                    display_name="Bad Profile",
                    resume_path="resume/candidate.docx",
                    resume_text="",
                )
            ]
        )

    detail = str(exc_info.value)
    assert "sync_profiles_from_source(force=True)" in detail
    assert "bad-profile (Bad Profile)" in detail
    assert ".docx" in detail


def test_sync_profiles_from_source_force_runs_cutover_validation_before_sync(monkeypatch, tmp_path):
    config_path = tmp_path / "profiles.json"
    config_path.write_text("[]", encoding="utf-8")

    captured = {"validated": None}

    monkeypatch.setattr(database, "_resolve_profiles_config_path", lambda: config_path)
    monkeypatch.setattr(database, "_compute_profile_source_signature", lambda path: ("test", 1, 1))
    monkeypatch.setattr(database, "_PROFILE_SOURCE_SIGNATURE", None)
    monkeypatch.setattr(
        database,
        "_load_profile_configs_from_file",
        lambda path: [_profile(profile_key="ready", display_name="Ready")],
    )
    monkeypatch.setattr(
        database,
        "_coerce_profile_configs",
        lambda profiles: profiles,
    )
    monkeypatch.setattr(
        database,
        "_validate_matcher_cutover_profiles",
        lambda profiles: captured.update({"validated": list(profiles)}),
    )
    monkeypatch.setattr(database, "_sync_profile_configs", lambda *args, **kwargs: 1)
    monkeypatch.setattr(database, "_deactivate_missing_profiles", lambda *args, **kwargs: None)
    monkeypatch.setattr(database, "_bootstrap_flagged_profiles", lambda *args, **kwargs: None)

    result = database._sync_profiles_from_source(cursor=object(), force=True)

    assert result == 1
    assert captured["validated"] == [_profile(profile_key="ready", display_name="Ready")]
