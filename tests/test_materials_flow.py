import datetime
import importlib
import json

import pytest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover - optional test dependency in some environments
    TestClient = None

from dags import materials_generation
from dags import materials_links
from dags import materials_prompts
from dags import materials_rendering


def _require_testclient():
    if TestClient is None:
        pytest.skip("fastapi not installed")


def test_build_and_verify_materials_token_round_trip(monkeypatch):
    monkeypatch.setenv("MATERIALS_LINK_SECRET", "test-secret")
    expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)

    token = materials_links.build_materials_token(
        profile_id=7,
        job_id="job-123",
        expires_at=expires_at,
    )
    payload = materials_links.verify_materials_token(token)

    assert payload["profile_id"] == 7
    assert payload["job_id"] == "job-123"
    assert payload["purpose"] == "materials_generate"


def test_verify_materials_token_rejects_tampering(monkeypatch):
    monkeypatch.setenv("MATERIALS_LINK_SECRET", "test-secret")
    expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
    token = materials_links.build_materials_token(
        profile_id=7,
        job_id="job-123",
        expires_at=expires_at,
    )
    payload_b64, signature_b64 = token.split(".", 1)
    tampered_payload = json.loads(materials_links._urlsafe_b64decode(payload_b64).decode("utf-8"))
    tampered_payload["job_id"] = "job-999"
    tampered_token = (
        materials_links._urlsafe_b64encode(
            json.dumps(tampered_payload, separators=(",", ":")).encode("utf-8")
        )
        + "."
        + signature_b64
    )

    with pytest.raises(ValueError, match="invalid_materials_token_signature"):
        materials_links.verify_materials_token(tampered_token)


def test_build_resume_prompt_includes_one_page_submission_ready_guidance():
    prompt = materials_prompts.build_resume_prompt(
        job_title="Data Engineer",
        company="Acme",
        job_description="We need a data engineer.",
        extracted_inventory={"experiences": []},
        alignment_plan={"selected_evidence_ids": []},
    )

    assert "one-page" in prompt
    assert "2-3 strong lines" in prompt
    assert "highest-value evidence" in prompt
    assert "skills should stay concise" in prompt
    assert "trim sections and entries aggressively" in prompt
    assert "usually 1-2 bullets per entry" in prompt


def test_build_resume_prompt_preserves_source_experience_boundaries():
    prompt = materials_prompts.build_resume_prompt(
        job_title="Data Engineer",
        company="Acme",
        job_description="We need a data engineer.",
        extracted_inventory={"experiences": []},
        alignment_plan={"selected_evidence_ids": []},
    )

    lowered = prompt.lower()
    assert "do not split one source experience into multiple entries" in lowered
    assert "do not repeat identical role/company/dates headers" in lowered


def test_build_resume_prompt_requires_relevance_ordering_before_compaction():
    prompt = materials_prompts.build_resume_prompt(
        job_title="Data Engineer",
        company="Acme",
        job_description="We need a data engineer.",
        extracted_inventory={"experiences": [], "projects": []},
        alignment_plan={"selected_evidence_ids": []},
    )

    lowered = prompt.lower()
    assert "order entries by target-role relevance and strength" in lowered
    assert "put must-keep evidence first instead of keeping source order" in lowered


def test_build_cover_letter_prompt_includes_resume_context_and_dutch_market_guidance():
    prompt = materials_prompts.build_cover_letter_prompt(
        job_title="Data Engineer",
        company="Acme",
        job_description="We are looking for a data engineer to build ETL pipelines.",
        extracted_inventory={"experiences": []},
        alignment_plan={"selected_evidence_ids": []},
        resume_text="Built reliable ETL pipelines in Python and Spark.",
        generated_resume={"headline": "Data Engineer", "summary_lines": [], "sections": [], "skills": ["Python"]},
    )

    assert "Original candidate resume" in prompt
    assert "Dutch-market" in prompt
    assert "direct, grounded, and specific" in prompt
    assert "avoid exaggerated enthusiasm" in prompt
    assert "not an ATS summary or audit report" in prompt
    assert "Built reliable ETL pipelines in Python and Spark." in prompt
    assert "Job description" in prompt
    assert "build ETL pipelines" in prompt
    assert "Generated resume" in prompt
    assert "align the letter's narrative with the generated resume" in prompt.lower()
    assert "closing must contain only the sign-off" in prompt


def test_build_cover_letter_prompt_requires_specific_company_motivation():
    prompt = materials_prompts.build_cover_letter_prompt(
        job_title="Data Engineer",
        company="Acme",
        job_description="We are looking for a data engineer to build ETL pipelines.",
        extracted_inventory={"experiences": []},
        alignment_plan={"selected_evidence_ids": []},
        resume_text="Built reliable ETL pipelines in Python and Spark.",
        generated_resume={"headline": "Data Engineer", "summary_lines": [], "sections": [], "skills": ["Python"]},
    )

    lowered = prompt.lower()
    assert "specific motivation for this company and role" in lowered
    assert "avoid generic motivation" in lowered




def test_validate_generated_document_requires_resume_sections():
    with pytest.raises(ValueError, match="invalid_resume_payload_missing_sections"):
        materials_generation._validate_generated_document(
            stage_name="resume",
            payload={
                "headline": "Data Engineer",
                "summary_lines": [],
                "skills": [],
                "warnings": [],
            },
        )


def test_validate_generated_document_rejects_non_string_cover_letter_paragraph():
    with pytest.raises(ValueError, match="invalid_cover_letter_payload_paragraphs"):
        materials_generation._validate_generated_document(
            stage_name="cover_letter",
            payload={
                "subject": "Application",
                "greeting": "Dear team",
                "paragraphs": [123],
                "closing": "Thanks",
                "warnings": [],
            },
        )


def test_validate_generated_document_accepts_string_cover_letter_paragraphs():
    payload = materials_generation._validate_generated_document(
        stage_name="cover_letter",
        payload={
            "subject": "Application",
            "greeting": "Dear team",
            "paragraphs": ["Paragraph one", "Paragraph two"],
            "closing": "Thanks",
            "warnings": [],
        },
    )

    assert payload["paragraphs"] == ["Paragraph one", "Paragraph two"]


def test_generate_materials_for_profile_job_saves_stabilized_resume_json(monkeypatch):
    saved_artifacts = {}

    monkeypatch.setattr(
        materials_generation.database,
        "get_material_generation_context",
        lambda profile_id, job_id: {
            "title": "Data Engineering Intern",
            "company": "Metyis",
            "description": "Build data pipelines and support analytics.",
            "display_name": "Xingyou Li",
            "profile_key": "xingyou",
            "resume_path": "/tmp/resume.md",
            "resume_text": None,
            "candidate_summary_config": json.dumps({"target_role": "Data Engineer"}),
            "model_name": "gpt-5.4",
        },
    )
    monkeypatch.setattr(
        materials_generation,
        "load_resume_text",
        lambda **kwargs: ("Original resume text", None),
    )
    monkeypatch.setattr(
        materials_generation.llm_runtime,
        "parse_llm_endpoints_from_env",
        lambda: [{"name": "test", "request_url": "https://example.com", "api_key": "key"}],
    )
    monkeypatch.setattr(
        materials_generation.database,
        "create_material_generation",
        lambda **kwargs: 41,
    )
    monkeypatch.setattr(
        materials_generation.database,
        "update_material_generation_status",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        materials_generation.database,
        "save_material_artifact",
        lambda generation_id, artifact_type, mime_type, content_text: saved_artifacts.setdefault(
            artifact_type,
            content_text,
        ),
    )
    monkeypatch.setattr(
        materials_rendering,
        "render_resume_markdown",
        lambda resume, *, profile_name: "resume-md",
    )
    monkeypatch.setattr(
        materials_rendering,
        "render_cover_letter_markdown",
        lambda cover_letter, *, profile_name, company: "cover-letter-md",
    )
    monkeypatch.setattr(
        materials_rendering,
        "render_resume_document_html",
        lambda resume, *, profile_name: "resume-html",
    )
    monkeypatch.setattr(
        materials_rendering,
        "render_cover_letter_document_html",
        lambda cover_letter, *, profile_name, company: "cover-letter-html",
    )
    monkeypatch.setattr(
        materials_rendering,
        "render_pdf_data_url_from_html",
        lambda document_html: "data:application/pdf;base64,JVBERi0=",
    )

    staged_payloads = iter(
        [
            (
                {
                    "candidate_profile": {},
                    "experiences": [],
                    "projects": [],
                    "education": [],
                    "constraints": [],
                },
                "gpt-5.4",
            ),
            (
                {
                    "target_role": "Data Engineering Intern",
                    "must_cover": [],
                    "gaps": [],
                    "selected_evidence_ids": [],
                    "banned_claims": [],
                    "tone": "concise",
                    "keywords": [],
                },
                "gpt-5.4",
            ),
            (
                {
                    "headline": "Data Engineer",
                    "summary_lines": ["One", "Two", "Three"],
                    "skills": ["Python", "SQL", "Airflow"],
                    "sections": [
                        {
                            "title": "Experience",
                            "entries": [
                                {
                                    "header_fields": {
                                        "role": "Data Engineer Intern",
                                        "company": "NIO Netherlands B.V.",
                                        "dates": "Sep 2023 - May 2024",
                                    },
                                    "bullets": ["Optimized SQL queries.", "Maintained pipelines."],
                                },
                                {
                                    "header_fields": {
                                        "role": "Data Engineer Intern",
                                        "company": "NIO Netherlands B.V.",
                                        "dates": "Sep 2023 - May 2024",
                                    },
                                    "bullets": ["Designed data models.", "Tracked holiday indicators."],
                                },
                            ],
                        }
                    ],
                },
                "gpt-5.4",
            ),
            (
                {
                    "subject": "Application for Data Engineering Intern",
                    "greeting": "Dear Hiring Team,",
                    "paragraphs": ["Motivation", "Evidence", "Close"],
                    "closing": "Kind regards,\nXingyou Li",
                },
                "gpt-5.4",
            ),
        ]
    )
    monkeypatch.setattr(
        materials_generation,
        "_run_json_stage",
        lambda **kwargs: next(staged_payloads),
    )

    result = materials_generation.generate_materials_for_profile_job(
        profile_id=1,
        job_id="job-123",
    )

    saved_resume = json.loads(saved_artifacts["resume_json"])
    experience_entries = saved_resume["sections"][0]["entries"]
    assert result["generation_id"] == 41
    assert len(experience_entries) == 1
    assert experience_entries[0]["bullets"] == [
        "Optimized SQL queries.",
        "Maintained pipelines.",
        "Designed data models.",
        "Tracked holiday indicators.",
    ]


def test_render_resume_document_html_uses_compact_one_page_layout_and_trimming():
    html = materials_rendering.render_resume_document_html(
        {
            "headline": "Data Engineer",
            "summary_lines": ["A", "B", "C", "D"],
            "skills": ["Python", "SQL", "Airflow", "dbt", "Spark", "AWS", "Docker", "Kafka", "Pandas"],
            "sections": [
                {
                    "title": "Experience",
                    "entries": [
                        {
                            "header_fields": {"role": "Role 1", "company": "Acme", "dates": "2024"},
                            "bullets": ["B1", "B2", "B3"],
                        },
                        {
                            "header_fields": {"role": "Role 2", "company": "Beta", "dates": "2023"},
                            "bullets": ["B4", "B5", "B6"],
                        },
                    ],
                },
                {
                    "title": "Projects",
                    "entries": [
                        {
                            "header_fields": {"role": "Proj 1", "company": "", "dates": "2022"},
                            "bullets": ["P1", "P2", "P3"],
                        }
                    ],
                },
            ],
        },
        profile_name="Levi",
    )

    assert "resume-page" in html
    assert "skills-list" in html
    assert "section-title" in html
    assert "<p>A</p>" in html
    assert "<p>D</p>" not in html
    assert "Kafka" in html
    assert "Pandas" not in html
    assert "B1" in html
    assert "B2" in html
    assert "B3" not in html
    assert "P3" not in html


def test_render_resume_document_html_omits_warnings_and_renders_entries():
    html = materials_rendering.render_resume_document_html(
        {
            "headline": "Data Engineer",
            "summary_lines": ["Summary line"],
            "skills": ["Python", "SQL"],
            "warnings": ["Should not appear"],
            "sections": [
                {
                    "title": "Experience",
                    "entries": [
                        {
                            "header_fields": {
                                "role": "Data Engineer Intern",
                                "company": "NIO Netherlands B.V.",
                                "dates": "2023-09 - 2024-05",
                            },
                            "bullets": ["Optimized SQL queries."],
                        }
                    ],
                }
            ],
        },
        profile_name="Xingyou Li",
    )

    assert "Should not appear" not in html
    assert "Data Engineer Intern" in html
    assert "NIO Netherlands B.V." in html
    assert "Optimized SQL queries." in html


def test_render_cover_letter_document_html_uses_polished_document_classes():
    html = materials_rendering.render_cover_letter_document_html(
        {
            "subject": "Application",
            "greeting": "Dear team",
            "paragraphs": ["Paragraph one"],
            "closing": "Thanks\nLevi",
        },
        profile_name="Levi",
        company="Acme",
    )

    assert "letter-page" in html
    assert "letter-header" in html
    assert "letter-body" in html


    html = materials_rendering.render_cover_letter_document_html(
        {
            "subject": "Application",
            "greeting": "Dear team",
            "paragraphs": ["Paragraph one"],
            "closing": "Thanks\nLevi",
            "warnings": ["Should not appear"],
        },
        profile_name="Levi",
        company="Acme",
    )

    assert "Should not appear" not in html
    assert "Paragraph one" in html
    assert "Thanks" in html


def test_render_resume_markdown_omits_warnings_section():
    markdown = materials_rendering.render_resume_markdown(
        {
            "headline": "Data Engineer",
            "summary_lines": ["Summary line"],
            "skills": ["Python"],
            "warnings": ["Should not appear"],
            "sections": [],
        },
        profile_name="Xingyou Li",
    )

    assert "Should not appear" not in markdown
    assert "## Notes" not in markdown


def test_render_resume_markdown_accepts_flat_entry_shapes():
    markdown = materials_rendering.render_resume_markdown(
        {
            "headline": "Data Engineer",
            "summary_lines": [],
            "skills": [],
            "warnings": [],
            "sections": [
                {
                    "title": "Experience",
                    "items": [
                        {
                            "title": "Data Engineer Intern",
                            "employer": "NIO Netherlands B.V.",
                            "start_date": "2023-09",
                            "end_date": "2024-05",
                            "highlights": [
                                "Optimized SQL queries across multiple data pipelines.",
                                "Maintained pipeline triggers and dependencies.",
                            ],
                        }
                    ],
                },
                {
                    "title": "Education",
                    "items": [
                        {
                            "degree": "M.S. Computer Science",
                            "institution": "University of Amsterdam",
                            "end_date": "2024-08",
                            "details": ["Big Data Engineering specialization."],
                        }
                    ],
                },
            ],
        },
        profile_name="Xingyou Li",
    )

    assert "### Data Engineer Intern — NIO Netherlands B.V." in markdown
    assert "2023-09 - 2024-05" in markdown
    assert "Optimized SQL queries across multiple data pipelines." in markdown
    assert "### M.S. Computer Science — University of Amsterdam" in markdown
    assert "Big Data Engineering specialization." in markdown


def test_render_cover_letter_markdown_omits_warnings_section():
    markdown = materials_rendering.render_cover_letter_markdown(
        {
            "subject": "Application",
            "greeting": "Dear team",
            "paragraphs": ["Paragraph one"],
            "closing": "Thanks",
            "warnings": ["Should not appear"],
        },
        profile_name="Levi",
        company="Acme",
    )

    assert "Should not appear" not in markdown
    assert "Notes:" not in markdown


def test_render_cover_letter_markdown_accepts_string_paragraphs():
    markdown = materials_rendering.render_cover_letter_markdown(
        {
            "subject": "Application",
            "greeting": "Dear team",
            "paragraphs": ["Paragraph one", "Paragraph two"],
            "closing": "Thanks",
            "warnings": [],
        },
        profile_name="Levi",
        company="Acme",
    )

    assert "Paragraph one" in markdown
    assert "Paragraph two" in markdown


def test_render_html_from_markdown_preserves_generated_html():
    html = materials_rendering.render_html_from_markdown(
        "# Resume\n\n<script>alert(1)</script>\n\nSafe paragraph."
    )

    assert "<script>alert(1)</script>" not in html
    assert "<h1>Resume</h1>" in html
    assert "<p>Safe paragraph.</p>" in html


def test_stabilize_resume_merges_duplicate_experience_headers_before_rendering():
    stabilized = materials_rendering.stabilize_resume_payload(
        {
            "headline": "Data Engineer",
            "summary_lines": ["One", "Two", "Three"],
            "skills": ["Python", "SQL", "Airflow"],
            "sections": [
                {
                    "title": "Experience",
                    "entries": [
                        {
                            "header_fields": {
                                "role": "Data Engineer Intern",
                                "company": "NIO Netherlands B.V.",
                                "dates": "Sep 2023 - May 2024",
                            },
                            "bullets": ["Optimized SQL queries.", "Maintained 100+ pipelines."],
                        },
                        {
                            "header_fields": {
                                "role": "Data Engineer Intern",
                                "company": "NIO Netherlands B.V.",
                                "dates": "Sep 2023 - May 2024",
                            },
                            "bullets": ["Designed data models.", "Tracked holiday indicators."],
                        },
                    ],
                }
            ],
        }
    )

    experience_entries = stabilized["sections"][0]["entries"]
    assert len(experience_entries) == 1
    assert experience_entries[0]["header_fields"]["role"] == "Data Engineer Intern"
    assert experience_entries[0]["bullets"] == [
        "Optimized SQL queries.",
        "Maintained 100+ pipelines.",
        "Designed data models.",
        "Tracked holiday indicators.",
    ]


def test_render_resume_document_html_merges_duplicate_headers_and_keeps_three_bullets_for_single_experience():
    html = materials_rendering.render_resume_document_html(
        {
            "headline": "Data Engineer",
            "summary_lines": ["One", "Two", "Three"],
            "skills": ["Python", "SQL", "Airflow"],
            "sections": [
                {
                    "title": "Experience",
                    "entries": [
                        {
                            "header_fields": {
                                "role": "Data Engineer Intern",
                                "company": "NIO Netherlands B.V.",
                                "dates": "Sep 2023 - May 2024",
                            },
                            "bullets": ["Optimized SQL queries.", "Maintained 100+ pipelines."],
                        },
                        {
                            "header_fields": {
                                "role": "Data Engineer Intern",
                                "company": "NIO Netherlands B.V.",
                                "dates": "Sep 2023 - May 2024",
                            },
                            "bullets": ["Designed data models.", "Tracked holiday indicators."],
                        },
                    ],
                }
            ],
        },
        profile_name="Xingyou Li",
    )

    assert html.count("Data Engineer Intern") == 1
    assert html.count("NIO Netherlands B.V.") == 1
    assert "Optimized SQL queries." in html
    assert "Maintained 100+ pipelines." in html
    assert "Designed data models." in html
    assert "Tracked holiday indicators." not in html


def test_materials_page_shows_failed_generation_state(monkeypatch):
    _require_testclient()
    webapp_main = importlib.import_module("webapp.main")
    client = TestClient(webapp_main.app)

    token_record = {"id": 11, "revoked_at": None}
    context = {
        "profile_id": 7,
        "job_id": "job-123",
        "title": "Data Engineer",
        "company": "Acme",
        "display_name": "Levi",
        "fit_decision": "Strong Fit",
        "fit_score": 88,
        "job_url": "https://example.com/jobs/123",
    }
    generation = {
        "id": 41,
        "status": "failed",
        "stage": "failed",
        "error_message_user": "Provider unavailable.",
        "model_name_used": "gpt-5.4",
    }

    monkeypatch.setattr(
        webapp_main.materials_links,
        "verify_materials_token",
        lambda token: {"profile_id": 7, "job_id": "job-123"},
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_material_access_token",
        lambda token: token_record,
    )
    monkeypatch.setattr(
        webapp_main.database,
        "touch_material_access_token",
        lambda token_id: None,
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_material_generation_context",
        lambda profile_id, job_id: context,
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_latest_material_generation",
        lambda profile_id, job_id: generation,
    )
    monkeypatch.setattr(webapp_main.database, "get_material_artifacts", lambda generation_id: [])
    monkeypatch.setattr(
        webapp_main.materials_generation,
        "generate_materials_for_profile_job",
        lambda **kwargs: None,
    )

    response = client.get("/materials", params={"token": "signed-token"})

    assert response.status_code == 200
    assert "Provider unavailable." in response.text
    assert "Failed" in response.text
    assert "No resume preview available yet." in response.text
    assert "No cover letter preview available yet." in response.text


def test_download_artifact_returns_404_when_generation_missing(monkeypatch):
    _require_testclient()
    webapp_main = importlib.import_module("webapp.main")
    client = TestClient(webapp_main.app)

    monkeypatch.setattr(
        webapp_main.materials_links,
        "verify_materials_token",
        lambda token: {"profile_id": 7, "job_id": "job-123"},
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_material_access_token",
        lambda token: {"id": 11, "revoked_at": None},
    )
    monkeypatch.setattr(
        webapp_main.database,
        "touch_material_access_token",
        lambda token_id: None,
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_latest_material_generation",
        lambda profile_id, job_id: None,
    )

    response = client.get("/materials/download/resume_md", params={"token": "signed-token"})

    assert response.status_code == 404
    assert response.json()["detail"] == "No generated materials are available for this link yet"


def test_render_pdf_data_url_from_html_returns_pdf_payload(monkeypatch):
    monkeypatch.setattr(
        materials_rendering,
        "render_pdf_bytes_from_html",
        lambda document_html: b"%PDF-mock",
    )

    pdf_data_url = materials_rendering.render_pdf_data_url_from_html("<html><body>Resume</body></html>")

    assert pdf_data_url.startswith("data:application/pdf;base64,")


def test_download_artifact_returns_pdf_bytes(monkeypatch):
    _require_testclient()
    webapp_main = importlib.import_module("webapp.main")
    client = TestClient(webapp_main.app)

    monkeypatch.setattr(
        webapp_main.materials_links,
        "verify_materials_token",
        lambda token: {"profile_id": 7, "job_id": "job-123"},
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_material_access_token",
        lambda token: {"id": 11, "revoked_at": None},
    )
    monkeypatch.setattr(
        webapp_main.database,
        "touch_material_access_token",
        lambda token_id: None,
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_latest_material_generation",
        lambda profile_id, job_id: {"id": 41},
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_material_artifacts",
        lambda generation_id: [
            {
                "artifact_type": "resume_pdf",
                "mime_type": "application/pdf",
                "content_text": materials_rendering.render_pdf_data_url_from_html("<html><body>Resume</body></html>"),
            }
        ],
    )

    response = client.get("/materials/download/resume_pdf", params={"token": "signed-token"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert response.headers["content-disposition"] == 'attachment; filename="resume_pdf.pdf"'
    assert response.content.startswith(b"%PDF-")
