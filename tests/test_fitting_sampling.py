"""Tests for P0.1 multi-sampling consensus scoring."""
import json
from pathlib import Path

import pytest
import requests

from dags import fitting_notifier, database


def _candidate_summary(**overrides):
    base = {
        "summary": "Backend/data candidate.",
        "target_roles": ["Data Engineer"],
        "candidate_years": 3,
        "candidate_seniority": "mid",
        "core_skills": ["Python", "SQL"],
        "obvious_gaps": [],
        "language_signals": {
            "dutch_level": "basic",
            "english_level": "fluent",
            "notes": "",
        },
    }
    base.update(overrides)
    return base


def _capped_sample(fit_score=80, decision="Strong Fit", language_blocker=False, experience_blocker=False):
    """Return a dict shaped like _apply_fit_caps output."""
    return {
        "fit_score": fit_score,
        "decision": decision,
        "exp_requirement": "not specified",
        "candidate_summary": _candidate_summary(),
        "language_check": {
            "dutch_required": False,
            "language_blocker": language_blocker,
            "impact": "",
        },
        "experience_check": {
            "required_years": None,
            "candidate_years": 3,
            "gap_years": None,
            "seniority_required": "unknown",
            "candidate_seniority": "mid",
            "experience_blocker": experience_blocker,
            "reason": "No material experience blocker detected.",
            "severity": "none",
        },
        "skills_match": {"strong_matches": [], "partial_matches": [], "missing_critical_skills": []},
        "risk_factors": [],
        "summary": "",
    }


TITLE = "Data Engineer"
JD = "Build pipelines."


# --- _aggregate_fit_samples ---

def test_aggregate_min_score():
    samples = [_capped_sample(90), _capped_sample(88), _capped_sample(20)]
    result = fitting_notifier._aggregate_fit_samples(
        samples, job_title=TITLE, jd_text=JD, candidate_summary=_candidate_summary()
    )
    assert result["fit_score"] == 20


def test_aggregate_min_decision_rank():
    samples = [
        _capped_sample(90, "Strong Fit"),
        _capped_sample(88, "Strong Fit"),
        _capped_sample(70, "Not Recommended"),
    ]
    result = fitting_notifier._aggregate_fit_samples(
        samples, job_title=TITLE, jd_text=JD, candidate_summary=_candidate_summary()
    )
    assert result["decision"] == "Not Recommended"


def test_aggregate_conservative_regression():
    """[Strong/Strong/Not Recommended] → Not Recommended."""
    samples = [
        _capped_sample(90, "Strong Fit"),
        _capped_sample(85, "Strong Fit"),
        _capped_sample(30, "Not Recommended"),
    ]
    result = fitting_notifier._aggregate_fit_samples(
        samples, job_title=TITLE, jd_text=JD, candidate_summary=_candidate_summary()
    )
    assert result["decision"] == "Not Recommended"
    assert result["fit_score"] == 30


def test_aggregate_score_spread():
    samples = [_capped_sample(90), _capped_sample(88), _capped_sample(20)]
    result = fitting_notifier._aggregate_fit_samples(
        samples, job_title=TITLE, jd_text=JD, candidate_summary=_candidate_summary()
    )
    assert result["fit_score_spread"] == 70  # 90 - 20
    assert result["fit_sample_count"] == 3
    assert result["fit_sample_scores"] == [90, 88, 20]


def test_aggregate_blocker_or_language():
    """Any sample with language_blocker=True → aggregate language_blocker=True."""
    samples = [
        _capped_sample(80, "Strong Fit", language_blocker=False),
        _capped_sample(75, "Moderate Fit", language_blocker=True),
        _capped_sample(70, "Moderate Fit", language_blocker=False),
    ]
    result = fitting_notifier._aggregate_fit_samples(
        samples, job_title=TITLE, jd_text=JD, candidate_summary=_candidate_summary()
    )
    assert result["language_check"]["language_blocker"] is True
    assert result["fit_score"] <= 40  # caps applied after blocker OR


def test_aggregate_blocker_or_experience():
    """Any sample with experience_blocker=True → aggregate experience_blocker=True."""
    samples = [
        _capped_sample(80, "Strong Fit", experience_blocker=False),
        _capped_sample(75, "Moderate Fit", experience_blocker=True),
    ]
    result = fitting_notifier._aggregate_fit_samples(
        samples, job_title=TITLE, jd_text=JD, candidate_summary=_candidate_summary()
    )
    assert result["experience_check"]["experience_blocker"] is True


# --- _sample_llm_matches sample_count=1 bypass ---

def test_sample_llm_matches_single_sample_returns_spread_zero(monkeypatch):
    """When sample_count=1, result has spread metadata but no aggregation."""
    raw = {
        "fit_score": 82,
        "decision": "Strong Fit",
        "exp_requirement": "not specified",
        "language_check": {"dutch_required": False, "language_blocker": False, "impact": ""},
        "experience_check": {},
        "skills_match": {"strong_matches": [], "partial_matches": [], "missing_critical_skills": []},
        "risk_factors": [],
        "summary": "",
    }

    def fake_fallback(*, endpoints, model_name, prompt, return_metadata=False, temperature=None):
        if return_metadata:
            return dict(raw), "test-model"
        return dict(raw)

    monkeypatch.setattr(fitting_notifier, "_request_llm_json_with_fallback", fake_fallback)

    result = fitting_notifier._sample_llm_matches(
        endpoints=[{"request_url": "http://x", "api_key": "k", "name": "ep1"}],
        model_name="test-model",
        prompt="test",
        sample_count=1,
        temperature=0.4,
        job_title=TITLE,
        jd_text=JD,
        candidate_summary=_candidate_summary(),
    )
    assert result["fit_score_spread"] == 0
    assert result["fit_sample_count"] == 1
    assert result["fit_sample_scores"] == [result["fit_score"]]
    assert result["fit_sample_models"] == ["test-model"]


def test_sample_llm_matches_records_partial_sample_errors(monkeypatch):
    calls = {"count": 0}

    def fake_fallback(*, endpoints, model_name, prompt, return_metadata=False, temperature=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError("response_invalid_decision")
        parsed = {
            "fit_score": 72,
            "decision": "Moderate Fit",
            "exp_requirement": "not specified",
            "language_check": {"dutch_required": False, "language_blocker": False, "impact": ""},
            "experience_check": {},
            "skills_match": {"strong_matches": [], "partial_matches": [], "missing_critical_skills": []},
            "risk_factors": [],
            "summary": "",
        }
        return parsed, "test-model"

    monkeypatch.setattr(fitting_notifier, "_request_llm_json_with_fallback", fake_fallback)

    result = fitting_notifier._sample_llm_matches(
        endpoints=[{"request_url": "http://x", "api_key": "k", "name": "ep1"}],
        model_name="test-model",
        prompt="test",
        sample_count=2,
        temperature=0.4,
        job_title=TITLE,
        jd_text=JD,
        candidate_summary=_candidate_summary(),
    )

    assert result["fit_sample_count"] == 1
    assert result["fit_sample_errors"] == ["response_invalid_decision"]
    assert result["fit_sample_models"] == ["test-model"]


# --- temperature payload inclusion/omission ---

def test_request_llm_json_includes_temperature_when_positive(monkeypatch):
    captured = {}

    class FakeResponse:
        def raise_for_status(self): pass
        def json(self): return {"output_text": '{"ok": true}'}

    def fake_post(url, headers, json, timeout):
        captured["json"] = json
        return FakeResponse()

    monkeypatch.setattr(requests, "post", fake_post)
    fitting_notifier._request_llm_json(
        request_url="http://x", api_key="k", model_name="m", prompt="p", temperature=0.4
    )
    assert "temperature" in captured["json"]
    assert captured["json"]["temperature"] == 0.4


def test_request_llm_json_omits_temperature_when_none(monkeypatch):
    captured = {}

    class FakeResponse:
        def raise_for_status(self): pass
        def json(self): return {"output_text": '{"ok": true}'}

    def fake_post(url, headers, json, timeout):
        captured["json"] = json
        return FakeResponse()

    monkeypatch.setattr(requests, "post", fake_post)
    fitting_notifier._request_llm_json(
        request_url="http://x", api_key="k", model_name="m", prompt="p"
    )
    assert "temperature" not in captured["json"]


def test_request_llm_json_omits_temperature_when_zero(monkeypatch):
    captured = {}

    class FakeResponse:
        def raise_for_status(self): pass
        def json(self): return {"output_text": '{"ok": true}'}

    def fake_post(url, headers, json, timeout):
        captured["json"] = json
        return FakeResponse()

    monkeypatch.setattr(requests, "post", fake_post)
    fitting_notifier._request_llm_json(
        request_url="http://x", api_key="k", model_name="m", prompt="p", temperature=0.0
    )
    assert "temperature" not in captured["json"]


def test_fitting_sample_temperature_accepts_decimal_env(monkeypatch):
    monkeypatch.setenv("FITTING_SAMPLE_TEMPERATURE", "0.25")

    assert fitting_notifier._fitting_sample_temperature() == 0.25


def test_fitting_sample_temperature_defaults_when_non_positive(monkeypatch):
    monkeypatch.setenv("FITTING_SAMPLE_TEMPERATURE", "0")

    assert fitting_notifier._fitting_sample_temperature() == 0.4


def test_fitting_sample_mode_defaults_to_single_endpoint(monkeypatch):
    monkeypatch.delenv("FITTING_SAMPLE_MODE", raising=False)

    assert fitting_notifier._fitting_sample_mode() == "single_endpoint"


def test_fitting_sample_mode_rejects_multi_endpoint_first_pass(monkeypatch):
    monkeypatch.setenv("FITTING_SAMPLE_MODE", "multi_endpoint")

    with pytest.raises(ValueError, match="unsupported_fitting_sample_mode"):
        fitting_notifier._fitting_sample_mode()


# --- all-transient failures preserve TRANSIENT_API:: prefix ---

def test_sample_llm_matches_all_transient_raises_transient_runtime_error(monkeypatch):
    def fake_fallback(*, endpoints, model_name, prompt, return_metadata=False, temperature=None):
        raise RuntimeError("TRANSIENT_API::timeout on all endpoints")

    monkeypatch.setattr(fitting_notifier, "_request_llm_json_with_fallback", fake_fallback)
    monkeypatch.setattr(fitting_notifier.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="TRANSIENT_API::"):
        fitting_notifier._sample_llm_matches(
            endpoints=[{"request_url": "http://x", "api_key": "k", "name": "ep1"}],
            model_name="test-model",
            prompt="test",
            sample_count=3,
            temperature=0.4,
            job_title=TITLE,
            jd_text=JD,
            candidate_summary=_candidate_summary(),
        )


def test_sample_llm_matches_all_invalid_raises_value_error_not_fatal(monkeypatch):
    def fake_fallback(*, endpoints, model_name, prompt, return_metadata=False, temperature=None):
        raise ValueError("response_invalid_decision")

    monkeypatch.setattr(fitting_notifier, "_request_llm_json_with_fallback", fake_fallback)

    with pytest.raises(ValueError, match="response_invalid_decision"):
        fitting_notifier._sample_llm_matches(
            endpoints=[{"request_url": "http://x", "api_key": "k", "name": "ep1"}],
            model_name="test-model",
            prompt="test",
            sample_count=3,
            temperature=0.4,
            job_title=TITLE,
            jd_text=JD,
            candidate_summary=_candidate_summary(),
        )


def test_sample_llm_matches_invalid_json_wrapped_by_fallback_is_job_error(monkeypatch):
    def fake_request_llm_json(*, request_url, api_key, model_name, prompt, temperature=None):
        raise ValueError("response_invalid_json: bad json")

    monkeypatch.setattr(fitting_notifier, "_request_llm_json", fake_request_llm_json)

    with pytest.raises(ValueError, match="response_invalid_json"):
        fitting_notifier._sample_llm_matches(
            endpoints=[{"request_url": "http://x", "api_key": "k", "name": "ep1"}],
            model_name="test-model",
            prompt="test",
            sample_count=3,
            temperature=0.4,
            job_title=TITLE,
            jd_text=JD,
            candidate_summary=_candidate_summary(),
        )


def test_sample_llm_matches_mixed_validation_and_transient_prefers_job_error(monkeypatch):
    calls = {"count": 0}

    def fake_fallback(*, endpoints, model_name, prompt, return_metadata=False, temperature=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError("response_invalid_decision")
        raise RuntimeError("TRANSIENT_API::timeout on all endpoints")

    monkeypatch.setattr(fitting_notifier, "_request_llm_json_with_fallback", fake_fallback)
    monkeypatch.setattr(fitting_notifier.time, "sleep", lambda _: None)

    with pytest.raises(ValueError, match="response_invalid_decision"):
        fitting_notifier._sample_llm_matches(
            endpoints=[{"request_url": "http://x", "api_key": "k", "name": "ep1"}],
            model_name="test-model",
            prompt="test",
            sample_count=2,
            temperature=0.4,
            job_title=TITLE,
            jd_text=JD,
            candidate_summary=_candidate_summary(),
        )


def test_sample_llm_matches_mixed_validation_and_fatal_preserves_fatal(monkeypatch):
    fatal_message = (
        "FATAL_API::endpoint=a model_name=m error=response_invalid_json: bad"
        " | endpoint=b model_name=m status=400 error=bad request"
    )

    def fake_fallback(*, endpoints, model_name, prompt, return_metadata=False, temperature=None):
        raise RuntimeError(fatal_message)

    monkeypatch.setattr(fitting_notifier, "_request_llm_json_with_fallback", fake_fallback)

    with pytest.raises(RuntimeError, match="^FATAL_API::"):
        fitting_notifier._sample_llm_matches(
            endpoints=[{"request_url": "http://x", "api_key": "k", "name": "ep1"}],
            model_name="test-model",
            prompt="test",
            sample_count=2,
            temperature=0.4,
            job_title=TITLE,
            jd_text=JD,
            candidate_summary=_candidate_summary(),
        )


def test_sample_llm_matches_success_and_fatal_preserves_fatal(monkeypatch):
    calls = {"count": 0}
    success = {
        "fit_score": 72,
        "decision": "Moderate Fit",
        "exp_requirement": "not specified",
        "language_check": {"dutch_required": False, "language_blocker": False, "impact": ""},
        "experience_check": {},
        "skills_match": {"strong_matches": [], "partial_matches": [], "missing_critical_skills": []},
        "risk_factors": [],
        "summary": "",
    }

    def fake_fallback(*, endpoints, model_name, prompt, return_metadata=False, temperature=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return dict(success), "test-model"
        raise RuntimeError("FATAL_API::endpoint=b model_name=m status=400 error=bad request")

    monkeypatch.setattr(fitting_notifier, "_request_llm_json_with_fallback", fake_fallback)

    with pytest.raises(RuntimeError, match="^FATAL_API::"):
        fitting_notifier._sample_llm_matches(
            endpoints=[{"request_url": "http://x", "api_key": "k", "name": "ep1"}],
            model_name="test-model",
            prompt="test",
            sample_count=2,
            temperature=0.4,
            job_title=TITLE,
            jd_text=JD,
            candidate_summary=_candidate_summary(),
        )


def test_process_single_item_only_samples_when_count_gt_one():
    source = Path("dags/fitting_notifier.py").read_text(encoding="utf-8")
    start = source.index('sample_count = runtime_int("FITTING_SAMPLE_COUNT"')
    end = source.index("parsed = None", start)
    sample_gate = source[start:end]

    assert "if sample_count > 1:" in sample_gate
    assert "_sample_llm_matches(" in sample_gate
    assert "_fitting_sample_temperature()" in sample_gate


# --- DB: _extract_fit_fields with new fields ---

def test_extract_fit_fields_returns_spread_and_sample_count():
    llm_match = json.dumps({
        "fit_score": 75,
        "decision": "Moderate Fit",
        "fit_score_spread": 30,
        "fit_sample_count": 3,
    })
    score, decision, spread, count = database._extract_fit_fields(llm_match)
    assert score == 75
    assert decision == "Moderate Fit"
    assert spread == 30
    assert count == 3


def test_extract_fit_fields_returns_none_spread_when_absent():
    llm_match = json.dumps({"fit_score": 82, "decision": "Strong Fit"})
    score, decision, spread, count = database._extract_fit_fields(llm_match)
    assert score == 82
    assert spread is None
    assert count is None


def test_extract_fit_fields_returns_none_on_null_match():
    score, decision, spread, count = database._extract_fit_fields(None)
    assert score is None
    assert decision is None
    assert spread is None
    assert count is None


# --- DB migration idempotence (static structural check) ---

def test_init_db_migration_contains_fit_score_spread_alter():
    import inspect
    source = inspect.getsource(database.init_db)
    assert "fit_score_spread" in source
    assert "fit_sample_count" in source
    assert "ADD COLUMN IF NOT EXISTS" in source
