import json
import time

import pytest
import requests

from dags import fitting_notifier
from dags import materials_launch


def _candidate_summary(**overrides):
    summary = {
        "summary": "Backend/data candidate with Python and SQL focus.",
        "target_roles": ["Data Engineer", "Backend Engineer"],
        "candidate_years": 3,
        "candidate_seniority": "mid",
        "core_skills": ["Python", "SQL", "PostgreSQL"],
        "obvious_gaps": ["Deep people management"],
        "language_signals": {
            "dutch_level": "basic",
            "english_level": "fluent",
            "notes": "English fluent, Dutch still limited.",
        },
    }
    summary.update(overrides)
    return summary


def _match_payload(**overrides):
    payload = {
        "fit_score": 82,
        "decision": "Strong Fit",
        "exp_requirement": "not specified",
        "language_check": {
            "dutch_required": False,
            "language_blocker": False,
            "impact": "No language blocker.",
        },
        "experience_check": {},
        "skills_match": {
            "strong_matches": ["Python"],
            "partial_matches": ["AWS"],
            "missing_critical_skills": [],
        },
        "risk_factors": [],
        "summary": "Strong technical overlap.",
    }
    payload.update(overrides)
    return payload


def test_build_fit_prompt_includes_candidate_summary_and_preserves_custom_prompt():
    prompt = fitting_notifier._build_fit_prompt(
        "Data Engineer",
        "Build pipelines and APIs.",
        "Resume content here.",
        _candidate_summary(),
        prompt_text="Return JSON only. Candidate Summary: <<<{{candidate_summary}}>>>",
    )

    assert "{{candidate_summary}}" not in prompt
    assert '"candidate_years": 3' in prompt
    assert prompt.startswith("Return JSON only.")


def test_build_fit_prompt_default_prompt_contains_strict_exp_requirement_instruction():
    prompt = fitting_notifier._build_fit_prompt(
        "Python Developer",
        "Looking for 5+ years building backend systems.",
        "Resume content here.",
        _candidate_summary(),
    )

    assert "Prefer a false negative over a false positive" in prompt
    assert "exp_requirement must be a single plain-English line" in prompt


def test_load_candidate_summary_config_parses_json_string():
    loaded = fitting_notifier._load_candidate_summary_config(
        json.dumps(_candidate_summary(candidate_years=2.5, candidate_seniority="junior"))
    )

    assert loaded["candidate_years"] == 2.5
    assert loaded["candidate_seniority"] == "junior"
    assert loaded["summary"]


def test_apply_fit_caps_downgrades_large_experience_gap():
    capped = fitting_notifier._apply_fit_caps(
        _match_payload(
            experience_check={"required_years": 7},
            exp_requirement="7+ years of experience required",
        ),
        job_title="Data Engineer",
        jd_text="Looking for 7+ years of experience in data engineering.",
        candidate_summary=_candidate_summary(candidate_years=1, candidate_seniority="junior"),
    )

    assert capped["fit_score"] == 25
    assert capped["decision"] == "Not Recommended"
    assert capped["experience_check"]["experience_blocker"] is True
    assert capped["experience_check"]["gap_years"] == 6


def test_apply_fit_caps_blocks_senior_title_even_without_exp_years():
    capped = fitting_notifier._apply_fit_caps(
        _match_payload(
            fit_score=76,
            decision="Moderate Fit",
            experience_check={},
        ),
        job_title="Senior Data Engineer",
        jd_text="Own critical pipelines and mentor teammates.",
        candidate_summary=_candidate_summary(candidate_years=3, candidate_seniority="mid"),
    )

    assert capped["fit_score"] == 40
    assert capped["decision"] == "Weak Fit"
    assert capped["experience_check"]["experience_blocker"] is True
    assert capped["experience_check"]["seniority_required"] == "senior"


def test_apply_fit_caps_keeps_reasonable_match_without_blocker():
    capped = fitting_notifier._apply_fit_caps(
        _match_payload(
            fit_score=74,
            decision="Moderate Fit",
            experience_check={"required_years": 3},
            exp_requirement="3+ years preferred",
        ),
        job_title="Data Engineer",
        jd_text="Python and SQL role.",
        candidate_summary=_candidate_summary(candidate_years=6, candidate_seniority="senior"),
    )

    assert capped["fit_score"] == 74
    assert capped["decision"] == "Moderate Fit"
    assert capped["experience_check"]["experience_blocker"] is False
    assert capped["candidate_summary"]["candidate_seniority"] == "senior"


def test_filter_notification_jobs_suppresses_experience_blocker_only():
    allowed_job = {
        "id": "1",
        "llm_match": json.dumps(
            {
                "experience_check": {"experience_blocker": False},
            }
        ),
    }
    blocked_job = {
        "id": "2",
        "llm_match": json.dumps(
            {
                "experience_check": {"experience_blocker": True},
            }
        ),
    }
    legacy_job = {
        "id": "3",
        "llm_match": json.dumps(
            {
                "fit_score": 66,
                "decision": "Moderate Fit",
            }
        ),
    }

    filtered = fitting_notifier._filter_notification_jobs(
        [allowed_job, blocked_job, legacy_job]
    )

    assert [job["id"] for job in filtered] == ["1", "3"]


def test_filter_notification_jobs_in_test_mode_allows_successful_fit_even_with_experience_blocker(
    monkeypatch,
):
    monkeypatch.setenv("LINKEDIN_TEST_MODE", "true")

    allowed_job = {
        "id": "1",
        "llm_match": json.dumps(
            {
                "fit_score": 41,
                "decision": "Weak Fit",
                "experience_check": {"experience_blocker": True},
            }
        ),
        "llm_match_error": None,
    }
    failed_job = {
        "id": "2",
        "llm_match": json.dumps(
            {
                "fit_score": 66,
                "decision": "Moderate Fit",
            }
        ),
        "llm_match_error": "llm_timeout",
    }
    invalid_payload_job = {
        "id": "3",
        "llm_match": "not-json",
        "llm_match_error": None,
    }

    filtered = fitting_notifier._filter_notification_jobs(
        [allowed_job, failed_job, invalid_payload_job]
    )

    assert [job["id"] for job in filtered] == ["1"]


def test_sort_notification_jobs_orders_by_fit_score_desc():
    jobs = [
        {"id": "b", "fit_score": 61, "profile_id": 2},
        {"id": "c", "fit_score": None, "profile_id": 1},
        {"id": "a", "fit_score": 88, "profile_id": 1},
        {"id": "d", "fit_score": 88, "profile_id": 3},
    ]

    sorted_jobs = fitting_notifier._sort_notification_jobs(jobs)

    assert [job["id"] for job in sorted_jobs] == ["a", "d", "b", "c"]


def test_format_exp_requirement_for_discord_handles_dict_like_payload():
    formatted = fitting_notifier._format_exp_requirement_for_discord(
        "{'required_years': 8, 'seniority_required': 'senior', 'notes': 'Data platform leadership'}"
    )

    assert "required years: 8" in formatted
    assert "seniority required: senior" in formatted
    assert "notes: Data platform leadership" in formatted


def test_normalize_exp_requirement_text_flattens_dict_like_payload_to_plain_text():
    normalized = fitting_notifier._normalize_exp_requirement_text(
        "{'title_signal': 'Python Developer with data/risk emphasis', 'jd_years_specified': '8-10 years', 'jd_seniority_specified': 'not applicable'}"
    )

    assert normalized == (
        "title signal: Python Developer with data/risk emphasis; "
        "jd years specified: 8-10 years; "
        "jd seniority specified: not applicable"
    )


def test_request_llm_json_with_fallback_skips_endpoint_with_missing_output(
    monkeypatch,
):
    calls = []

    def fake_request_llm_json(*, request_url, api_key, model_name, prompt):
        calls.append((request_url, model_name, prompt))
        if request_url == "https://empty.example/v1/responses":
            raise ValueError("response_missing_output_text")
        return {"fit_score": 77, "decision": "Moderate Fit"}

    monkeypatch.setattr(fitting_notifier, "_request_llm_json", fake_request_llm_json)

    parsed = fitting_notifier._request_llm_json_with_fallback(
        endpoints=[
            {
                "name": "empty-proxy",
                "request_url": "https://empty.example/v1/responses",
                "api_key": "key-1",
            },
            {
                "name": "working-proxy",
                "request_url": "https://working.example/v1/responses",
                "api_key": "key-2",
            },
        ],
        model_name="gpt-5.4",
        prompt="Return JSON only",
    )

    assert parsed == {"fit_score": 77, "decision": "Moderate Fit"}
    assert calls == [
        ("https://empty.example/v1/responses", "gpt-5.4", "Return JSON only"),
        ("https://working.example/v1/responses", "gpt-5.4", "Return JSON only"),
    ]


def test_parse_llm_endpoints_from_env_preserves_per_endpoint_model_override(monkeypatch):
    monkeypatch.setenv(
        "LLM_ENDPOINTS_JSON",
        json.dumps(
            [
                {
                    "name": "nc",
                    "request_url": "https://nowcoding.ai/v1/responses",
                    "api_key": "nc-key",
                },
                {
                    "name": "yuan",
                    "request_url": "https://us.mcxhm.cn/v1/responses",
                    "api_key": "yuan-key",
                    "model": "glm-5.1",
                },
            ]
        ),
    )

    endpoints = fitting_notifier._parse_llm_endpoints_from_env()

    assert endpoints == [
        {
            "name": "nc",
            "request_url": "https://nowcoding.ai/v1/responses",
            "api_key": "nc-key",
        },
        {
            "name": "yuan",
            "request_url": "https://us.mcxhm.cn/v1/responses",
            "api_key": "yuan-key",
            "model": "glm-5.1",
        },
    ]


def test_parse_llm_endpoints_from_env_rejects_api_key_env_indirection(monkeypatch):
    monkeypatch.setenv(
        "LLM_ENDPOINTS_JSON",
        json.dumps(
            [
                {
                    "name": "nc",
                    "request_url": "https://nowcoding.ai/v1/responses",
                    "api_key_env": "NC_API_KEY",
                }
            ]
        ),
    )
    monkeypatch.setenv("NC_API_KEY", "nc-key")

    with pytest.raises(ValueError, match="requires api_key"):
        fitting_notifier._parse_llm_endpoints_from_env()


def test_parse_llm_endpoints_from_env_ignores_legacy_single_endpoint_env(monkeypatch):
    monkeypatch.delenv("LLM_ENDPOINTS_JSON", raising=False)
    monkeypatch.setenv("FITTING_REQUEST_URL", "https://legacy.example/v1/responses")
    monkeypatch.setenv("LLM_API_KEY", "legacy-key")
    monkeypatch.setenv("GMN_API_KEY", "legacy-gmn-key")

    assert fitting_notifier._parse_llm_endpoints_from_env() == []


def test_request_llm_json_with_fallback_uses_endpoint_model_override(monkeypatch):
    calls = []

    def fake_request_llm_json(*, request_url, api_key, model_name, prompt):
        calls.append((request_url, model_name, prompt))
        if request_url == "https://nowcoding.ai/v1/responses":
            raise requests.Timeout("nc timeout")
        return {"fit_score": 77, "decision": "Moderate Fit"}

    monkeypatch.setattr(fitting_notifier, "_request_llm_json", fake_request_llm_json)

    parsed = fitting_notifier._request_llm_json_with_fallback(
        endpoints=[
            {
                "name": "nc",
                "request_url": "https://nowcoding.ai/v1/responses",
                "api_key": "key-1",
            },
            {
                "name": "yuan",
                "request_url": "https://us.mcxhm.cn/v1/responses",
                "api_key": "key-2",
                "model": "glm-5.1",
            },
        ],
        model_name="gpt-5.4",
        prompt="Return JSON only",
    )

    assert parsed == {"fit_score": 77, "decision": "Moderate Fit"}
    assert calls == [
        ("https://nowcoding.ai/v1/responses", "gpt-5.4", "Return JSON only"),
        ("https://us.mcxhm.cn/v1/responses", "glm-5.1", "Return JSON only"),
    ]


def test_request_llm_json_with_fallback_treats_missing_output_as_transient_when_all_endpoints_fail(
    monkeypatch,
):
    def fake_request_llm_json(*, request_url, api_key, model_name, prompt):
        raise ValueError("response_missing_output_text")

    monkeypatch.setattr(fitting_notifier, "_request_llm_json", fake_request_llm_json)

    with pytest.raises(RuntimeError, match="^TRANSIENT_API::") as error:
        fitting_notifier._request_llm_json_with_fallback(
            endpoints=[
                {
                    "name": "empty-proxy",
                    "request_url": "https://empty.example/v1/responses",
                    "api_key": "key-1",
                }
            ],
            model_name="gpt-5.4",
            prompt="Return JSON only",
        )

    assert "endpoint=empty-proxy model_name=gpt-5.4 error=response_missing_output_text" in str(
        error.value
    )


def test_request_llm_json_with_fallback_starts_from_first_endpoint_every_call(monkeypatch):
    calls = []

    def fake_request_llm_json(*, request_url, api_key, model_name, prompt):
        calls.append(request_url)
        return {"fit_score": 77, "decision": "Moderate Fit"}

    monkeypatch.setattr(fitting_notifier, "_request_llm_json", fake_request_llm_json)

    endpoints = [
        {
            "name": "proxy-a",
            "request_url": "https://a.example/v1/responses",
            "api_key": "key-a",
        },
        {
            "name": "proxy-b",
            "request_url": "https://b.example/v1/responses",
            "api_key": "key-b",
        },
    ]

    fitting_notifier._request_llm_json_with_fallback(
        endpoints=endpoints,
        model_name="gpt-5.4",
        prompt="Return JSON only",
    )
    fitting_notifier._request_llm_json_with_fallback(
        endpoints=endpoints,
        model_name="gpt-5.4",
        prompt="Return JSON only",
    )

    assert calls == [
        "https://a.example/v1/responses",
        "https://a.example/v1/responses",
    ]


def test_execute_prepared_fitting_items_stops_submitting_after_fatal_api():
    started_job_ids = []
    handled_events = []

    def process_single_item(prepared):
        job_id = prepared["item"]["job_id"]
        started_job_ids.append(job_id)
        if job_id == "fatal":
            raise RuntimeError("FATAL_API::endpoint outage")
        time.sleep(0.02)
        return prepared["item"], fitting_notifier._build_job_match_result(
            prepared["item"]["profile_id"],
            job_id,
            llm_match=json.dumps({"fit_score": 77, "decision": "Moderate Fit"}),
            model_name="gpt-5.4",
        )

    fatal_events = fitting_notifier._execute_prepared_fitting_items(
        [
            {"item": {"profile_id": 1, "job_id": "ok-1"}},
            {"item": {"profile_id": 1, "job_id": "fatal"}},
            {"item": {"profile_id": 1, "job_id": "ok-2"}},
            {"item": {"profile_id": 1, "job_id": "ok-3"}},
        ],
        concurrency=2,
        process_single_item=process_single_item,
        default_model_name_fn=lambda prepared: "gpt-5.4",
        handle_completed_event=handled_events.append,
    )

    assert len(started_job_ids) == 2
    assert set(started_job_ids) == {"ok-1", "fatal"}
    assert len(fatal_events) == 1
    assert fatal_events[0]["prepared"]["item"]["job_id"] == "fatal"
    assert any(
        event["kind"] == "job_result"
        and event["job_result"]["job_id"] == "ok-1"
        for event in handled_events
    )


def test_request_llm_json_uses_plain_string_input_payload(monkeypatch):
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"output_text": '{"ok": true}'}

    def fake_post(url, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(requests, "post", fake_post)

    parsed = fitting_notifier._request_llm_json(
        request_url="https://example.com/v1/responses",
        api_key="test-key",
        model_name="gpt-5.4",
        prompt="Return only valid JSON: {\"ok\": true}",
    )

    assert parsed == {"ok": True}
    assert captured["json"] == {
        "model": "gpt-5.4",
        "input": 'Return only valid JSON: {"ok": true}',
    }


def test_log_job_match_result_includes_model_name_for_success(capsys):
    fitting_notifier._log_job_match_result(
        {
            "profile_id": 12,
            "job_id": "job-1",
            "model_name": "gpt-5.4",
            "llm_match": json.dumps(
                {
                    "fit_score": 88,
                    "decision": "Strong Fit",
                }
            ),
            "llm_match_error": None,
        }
    )

    captured = capsys.readouterr()
    assert "status=ok" in captured.out
    assert "model_name=gpt-5.4" in captured.out


def test_log_job_match_result_includes_model_name_for_error(capsys):
    fitting_notifier._log_job_match_result(
        {
            "profile_id": 12,
            "job_id": "job-1",
            "model_name": "gpt-5.4-mini",
            "llm_match": None,
            "llm_match_error": "invalid_json_response",
        }
    )

    captured = capsys.readouterr()
    assert "status=error" in captured.out
    assert "model_name=gpt-5.4-mini" in captured.out



def test_build_discord_notification_summary_message_reports_zero_results():
    message = fitting_notifier._build_discord_notification_summary_message(
        {
            "profile_id": 7,
            "display_name": "George Gu",
            "eligible": 0,
            "sent": 0,
            "failed": 0,
            "time": "2026-04-24 12:00:00",
        }
    )

    assert "Fitting Notification Summary" in message
    assert "Profile: George Gu" in message
    assert "Eligible: 0" in message
    assert "Sent: 0" in message
    assert "Failed: 0" in message


def test_send_zero_result_notification_summaries_sends_per_active_profile(monkeypatch):
    import pandas as pd

    monkeypatch.setattr(
        fitting_notifier.database,
        "get_active_notification_profiles",
        lambda: pd.DataFrame(
            [
                {
                    "profile_id": 1,
                    "profile_key": "george",
                    "display_name": "George Gu",
                    "discord_channel_id": "chan-1",
                    "discord_webhook_url": None,
                },
                {
                    "profile_id": 2,
                    "profile_key": "xingyou",
                    "display_name": "Xingyou Li",
                    "discord_channel_id": None,
                    "discord_webhook_url": "https://discord.example/webhook",
                },
            ]
        ),
    )
    sent_messages = []

    def fake_send(content, *, channel_id=None, webhook_url=None):
        sent_messages.append(
            {"content": content, "channel_id": channel_id, "webhook_url": webhook_url}
        )
        return True, None

    monkeypatch.setattr(fitting_notifier, "_send_discord_message", fake_send)

    sent_count = fitting_notifier._send_zero_result_notification_summaries()

    assert sent_count == 2
    assert [message["channel_id"] for message in sent_messages] == ["chan-1", None]
    assert sent_messages[1]["webhook_url"] == "https://discord.example/webhook"
    assert all("Eligible: 0" in message["content"] for message in sent_messages)
    assert "Profile: George Gu" in sent_messages[0]["content"]
    assert "Profile: Xingyou Li" in sent_messages[1]["content"]

def test_build_discord_job_match_message_includes_materials_launch_url(monkeypatch):
    monkeypatch.setenv("RESUME_MATCHER_BASE_URL", "https://materials.example.com")
    monkeypatch.setenv("MATERIALS_LINK_SECRET", "test-secret")

    message = fitting_notifier._build_discord_job_match_message(
        {
            "profile_id": 7,
            "display_name": "George Gu",
            "id": "li-123",
            "title": "Data Engineer",
            "company": "Example Co",
            "fit_score": 88,
            "fit_decision": "Strong Fit",
            "job_url": "https://linkedin.example/job/li-123",
            "llm_match": json.dumps({"exp_requirement": "3+ years preferred"}),
        }
    )

    assert message is not None
    assert "Materials: https://materials.example.com/launch?token=" in message
    token = message.split("Materials: ", 1)[1].strip()
    parsed = materials_launch.verify_materials_launch_token(
        token=token.split("token=", 1)[1],
        secret="test-secret",
    )
    assert parsed["profile_id"] == 7
    assert parsed["job_id"] == "li-123"


def test_build_discord_job_match_message_omits_materials_url_without_config(monkeypatch):
    monkeypatch.delenv("RESUME_MATCHER_BASE_URL", raising=False)
    monkeypatch.delenv("MATERIALS_LINK_SECRET", raising=False)

    message = fitting_notifier._build_discord_job_match_message(
        {
            "profile_id": 7,
            "display_name": "George Gu",
            "id": "li-123",
            "title": "Data Engineer",
            "company": "Example Co",
            "fit_score": 88,
            "fit_decision": "Strong Fit",
            "job_url": "https://linkedin.example/job/li-123",
        }
    )

    assert message is not None
    assert "Materials:" not in message
