import datetime
import importlib
import json
import subprocess

import pytest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover - optional test dependency in some environments
    TestClient = None

from dags import materials_generation
from dags import materials_links
from dags import materials_prompts
from dags import materials_rendering
from dags import resume_utils


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


def test_build_alignment_prompt_requires_exact_source_ids_for_prioritized_lists():
    prompt = materials_prompts.build_alignment_prompt(
        job_title="Data Engineer",
        job_description="We need Python and ETL experience.",
        extracted_inventory={
            "experiences": [{"evidence_id": "exp_nio_001"}],
            "projects": [{"evidence_id": "proj_covid_001"}],
        },
        candidate_summary={},
    )

    lowered = prompt.lower()
    assert "exact evidence_id values" in lowered
    assert "never invent shorthand ids" in lowered


def test_build_resume_prompt_requires_truthful_experience_boundaries_and_anti_fluff():
    prompt = materials_prompts.build_resume_prompt(
        job_title="Data Engineer",
        company="Acme",
        job_description="We need a data engineer.",
        extracted_inventory={"experiences": [], "projects": []},
        alignment_plan={"selected_evidence_ids": []},
    )

    lowered = prompt.lower()
    assert "experience entries must map 1:1 to actual employment/internship roles" in lowered
    assert "projects, coursework, thesis work, and certifications must stay out of the experience section" in lowered
    assert "do not invent sub-roles" in lowered
    assert "avoid generic filler phrases" in lowered
    assert "results-driven" in lowered
    assert "passionate" in lowered
    assert "copy the exact extracted evidence_id values verbatim" in lowered


def test_build_resume_prompt_requires_seniority_honesty_and_standard_titles():
    prompt = materials_prompts.build_resume_prompt(
        job_title="Data Engineer",
        company="Acme",
        job_description="We need a data engineer.",
        extracted_inventory={"experiences": []},
        alignment_plan={"selected_evidence_ids": []},
    )

    lowered = prompt.lower()
    assert "summary_lines should sound seniority-appropriate" in lowered
    assert "prioritize recruiter readability and credibility over keyword stuffing" in lowered
    assert "prefer standard recruiter/ats-readable titles" in lowered


def test_build_resume_prompt_requires_projects_for_early_career_profiles():
    prompt = materials_prompts.build_resume_prompt(
        job_title="Data Engineer",
        company="Acme",
        job_description="We need a data engineer.",
        extracted_inventory={"experiences": [], "projects": []},
        alignment_plan={"selected_evidence_ids": [], "prioritized_project_ids": ["proj_1", "proj_2", "proj_3"]},
    )

    lowered = prompt.lower()
    assert "do not collapse the resume to only work experience" in lowered
    assert "prefer keeping the strongest 2-3 projects" in lowered


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


def test_build_cover_letter_prompt_uses_resume_and_jd_as_primary_inputs():
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
    assert "the original resume text and job description are the primary writing inputs" in lowered
    assert "treat the alignment plan as a claim filter" in lowered
    assert "why this role makes sense now" in lowered
    assert "product, domain, team, problem, or mission clue" in lowered
    assert "replacing the company name would make the paragraph obviously weaker" in lowered
    assert "dream opportunity" in lowered




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


def test_load_resume_text_reads_markdown_resume_path(tmp_path):
    resume_path = tmp_path / "resume.md"
    resume_path.write_text("# Candidate\nPython\n", encoding="utf-8")

    resume_text, resume_error = resume_utils.load_resume_text(
        resume_path=str(resume_path),
    )

    assert resume_error is None
    assert resume_text == "# Candidate\nPython\n"


def test_load_resume_text_reads_pdf_resume_path_via_pdftotext(tmp_path, monkeypatch):
    resume_path = tmp_path / "resume.pdf"
    resume_path.write_bytes(b"%PDF-1.4\n%mock\n")

    monkeypatch.setattr(resume_utils, "PdfReader", None)
    monkeypatch.setattr(resume_utils.shutil, "which", lambda name: "/usr/bin/pdftotext")

    def fake_run(cmd, *, capture_output, text, check, timeout):
        assert cmd == [
            "/usr/bin/pdftotext",
            "-layout",
            "-nopgbrk",
            "-enc",
            "UTF-8",
            str(resume_path),
            "-",
        ]
        assert capture_output is True
        assert text is True
        assert check is True
        assert timeout == 30
        return subprocess.CompletedProcess(cmd, 0, "Xingyou Li\nPython\nSQL\n", "")

    monkeypatch.setattr(resume_utils.subprocess, "run", fake_run)

    resume_text, resume_error = resume_utils.load_resume_text(
        resume_path=str(resume_path),
    )

    assert resume_error is None
    assert "Xingyou Li" in resume_text
    assert "Python" in resume_text


def test_load_resume_text_does_not_fallback_when_explicit_pdf_resume_fails(
    tmp_path,
    monkeypatch,
):
    resume_path = tmp_path / "resume.pdf"
    resume_path.write_bytes(b"%PDF-1.4\n%mock\n")
    fallback_resume_path = tmp_path / "resume.md"
    fallback_resume_path.write_text("Fallback resume", encoding="utf-8")

    monkeypatch.setattr(resume_utils, "PdfReader", None)
    monkeypatch.setattr(resume_utils.shutil, "which", lambda name: None)
    monkeypatch.setattr(resume_utils, "__file__", str(tmp_path / "fake_resume_utils.py"))

    resume_text, resume_error = resume_utils.load_resume_text(
        resume_path=str(resume_path),
    )

    assert resume_text is None
    assert "pdf_extract_error" in resume_error
    assert "Fallback resume" not in (resume_error or "")


def test_load_resume_text_does_not_fallback_when_explicit_resume_path_fails(monkeypatch):
    explicit_path = "/tmp/explicit-resume.pdf"
    fallback_path = "/tmp/fallback-resume.md"

    monkeypatch.setattr(
        resume_utils,
        "_build_resume_candidates",
        lambda resume_path: [resume_utils.Path(explicit_path), resume_utils.Path(fallback_path)],
    )

    def fake_read(candidate_path):
        if str(candidate_path) == explicit_path:
            raise RuntimeError("pdf_extract_error")
        return "fallback resume text"

    monkeypatch.setattr(resume_utils, "_read_resume_path", fake_read)

    resume_text, resume_error = resume_utils.load_resume_text(
        resume_path=explicit_path,
    )

    assert resume_text is None
    assert "pdf_extract_error" in resume_error


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
        lambda resume, *, profile_name, candidate_profile=None: "resume-md",
    )
    monkeypatch.setattr(
        materials_rendering,
        "render_cover_letter_markdown",
        lambda cover_letter, *, profile_name, company: "cover-letter-md",
    )
    monkeypatch.setattr(
        materials_rendering,
        "render_resume_document_html",
        lambda resume, *, profile_name, candidate_profile=None: "resume-html",
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
                        "candidate_profile": {"location": "Amsterdam, NL"},
                        "experiences": [
                            {
                                "evidence_id": "exp_nio_001",
                                "employer": "NIO Netherlands B.V.",
                                "title": "Data Engineer Intern",
                                "start_date": "Sep 2023",
                                "end_date": "May 2024",
                                "highlights": [
                                    {"evidence_id": "exp_nio_001_b1"},
                                    {"evidence_id": "exp_nio_001_b2"},
                                    {"evidence_id": "exp_nio_001_b3"},
                                    {"evidence_id": "exp_nio_001_b4"},
                                ],
                            }
                        ],
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
                        "selected_evidence_ids": [
                            "exp_nio_001_b1",
                            "exp_nio_001_b2",
                            "exp_nio_001_b3",
                            "exp_nio_001_b4",
                        ],
                        "banned_claims": [],
                        "tone": "concise",
                        "keywords": [],
                        "prioritized_experience_ids": ["exp_nio_001"],
                    },
                    "gpt-5.4",
                ),
                    (
                        {
                            "headline": "Data Engineer",
                            "summary_lines": [
                                "Maintained 100+ data pipelines and supported reliable ETL operations.",
                                "Built SQL-driven data models for after-sales analysis and business reporting.",
                                "Hands-on experience with Python, SQL, and Airflow in data engineering workflows.",
                            ],
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
                                    "bullets": [
                                        "Optimized SQL queries.",
                                        "Maintained pipelines.",
                                        "Designed data models.",
                                        "Tracked holiday indicators.",
                                    ],
                                    "source_meta": {
                                        "source_type": "experience",
                                        "source_id": "exp_nio_001",
                                        "source_evidence_ids": [
                                            "exp_nio_001_b1",
                                            "exp_nio_001_b2",
                                            "exp_nio_001_b3",
                                            "exp_nio_001_b4",
                                        ],
                                    },
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
                        "paragraphs": [
                            "I am applying for the Data Engineering Intern role.",
                            "I have relevant experience in pipelines and data models.",
                            "Metyis stands out because of its focus on data warehousing and live business cases.",
                        ],
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


def test_resume_semantic_errors_reject_split_source_experience_entries():
    errors = materials_generation._resume_semantic_errors(
        resume={
            "headline": "Data Engineer",
            "summary_lines": ["Maintained 100+ pipelines with SQL and data modeling support."],
            "skills": ["Python", "SQL"],
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
                            "bullets": ["Optimized SQL queries."],
                            "source_meta": {
                                "source_type": "experience",
                                "source_id": "exp_nio_001",
                                "source_evidence_ids": ["exp_nio_001_b1"],
                            },
                        },
                        {
                            "header_fields": {
                                "role": "Pipeline Operations Intern",
                                "company": "NIO Netherlands B.V.",
                                "dates": "Sep 2023 - May 2024",
                            },
                            "bullets": ["Maintained 100+ pipelines."],
                            "source_meta": {
                                "source_type": "experience",
                                "source_id": "exp_nio_001",
                                "source_evidence_ids": ["exp_nio_001_b4"],
                            },
                        },
                    ],
                }
            ],
        },
        extracted_inventory={
            "experiences": [
                {
                    "evidence_id": "exp_nio_001",
                    "employer": "NIO Netherlands B.V.",
                    "title": "Data Engineer Intern",
                    "start_date": "Sep 2023",
                    "end_date": "May 2024",
                    "highlights": [
                        {"evidence_id": "exp_nio_001_b1"},
                        {"evidence_id": "exp_nio_001_b4"},
                    ],
                }
            ],
            "projects": [],
        },
        alignment_plan={
            "selected_evidence_ids": ["exp_nio_001_b1", "exp_nio_001_b4"],
            "prioritized_experience_ids": ["exp_nio_001"],
        },
    )

    assert any(error.startswith("experience_source_split_across_multiple_entries") for error in errors)


def test_resume_semantic_errors_reject_invented_sub_role_even_with_valid_source_id():
    errors = materials_generation._resume_semantic_errors(
        resume={
            "headline": "Data Engineer",
            "summary_lines": ["Maintained 100+ pipelines with SQL and data modeling support."],
            "skills": ["Python", "SQL"],
            "sections": [
                {
                    "title": "Experience",
                    "entries": [
                        {
                            "header_fields": {
                                "role": "Pipeline Operations Intern",
                                "company": "NIO Netherlands B.V.",
                                "dates": "Sep 2023 - May 2024",
                            },
                            "bullets": ["Maintained 100+ pipelines."],
                            "source_meta": {
                                "source_type": "experience",
                                "source_id": "exp_nio_001",
                                "source_evidence_ids": ["exp_nio_001_b4"],
                            },
                        }
                    ],
                }
            ],
        },
        extracted_inventory={
            "experiences": [
                {
                    "evidence_id": "exp_nio_001",
                    "employer": "NIO Netherlands B.V.",
                    "title": "Data Engineer Intern",
                    "start_date": "Sep 2023",
                    "end_date": "May 2024",
                    "highlights": [
                        {"evidence_id": "exp_nio_001_b4"},
                    ],
                }
            ],
            "projects": [],
        },
        alignment_plan={
            "selected_evidence_ids": ["exp_nio_001_b4"],
            "prioritized_experience_ids": ["exp_nio_001"],
        },
    )

    assert "experience_entry_header_role_mismatch" in errors


def test_resume_semantic_errors_reject_header_company_and_dates_mismatch():
    errors = materials_generation._resume_semantic_errors(
        resume={
            "headline": "Data Engineer",
            "summary_lines": ["Maintained 100+ pipelines with SQL and data modeling support."],
            "skills": ["Python", "SQL"],
            "sections": [
                {
                    "title": "Experience",
                    "entries": [
                        {
                            "header_fields": {
                                "role": "Data Engineer Intern",
                                "company": "NIO Europe",
                                "dates": "2024",
                            },
                            "bullets": ["Maintained 100+ pipelines."],
                            "source_meta": {
                                "source_type": "experience",
                                "source_id": "exp_nio_001",
                                "source_evidence_ids": ["exp_nio_001_b4"],
                            },
                        }
                    ],
                }
            ],
        },
        extracted_inventory={
            "experiences": [
                {
                    "evidence_id": "exp_nio_001",
                    "employer": "NIO Netherlands B.V.",
                    "title": "Data Engineer Intern",
                    "start_date": "Sep 2023",
                    "end_date": "May 2024",
                    "highlights": [
                        {"evidence_id": "exp_nio_001_b4"},
                    ],
                }
            ],
            "projects": [],
        },
        alignment_plan={
            "selected_evidence_ids": ["exp_nio_001_b4"],
            "prioritized_experience_ids": ["exp_nio_001"],
        },
    )

    assert "experience_entry_header_company_mismatch" in errors
    assert "experience_entry_header_dates_mismatch" in errors


def test_resume_semantic_errors_reject_project_header_drift():
    errors = materials_generation._resume_semantic_errors(
        resume={
            "headline": "Data Engineer",
            "summary_lines": ["Built ETL pipelines in Airflow and Spark."],
            "skills": ["Python", "SQL"],
            "sections": [
                {
                    "title": "Projects",
                    "entries": [
                        {
                            "header_fields": {
                                "role": "Airflow Platform Automation Project",
                                "company": "",
                                "dates": "Jan - Feb 2026",
                            },
                            "bullets": ["Built a daily ELT pipeline in Airflow."],
                            "source_meta": {
                                "source_type": "project",
                                "source_id": "proj_stock_001",
                                "source_evidence_ids": ["proj_stock_001_b1"],
                            },
                        }
                    ],
                }
            ],
        },
        extracted_inventory={
            "experiences": [],
            "projects": [
                {
                    "evidence_id": "proj_stock_001",
                    "name": "Stock Market ELT Pipeline with Apache Airflow",
                    "start_date": "Jan",
                    "end_date": "Feb 2026",
                    "highlights": [
                        {"evidence_id": "proj_stock_001_b1"},
                    ],
                }
            ],
        },
        alignment_plan={
            "selected_evidence_ids": ["proj_stock_001_b1"],
        },
    )

    assert "project_entry_header_role_mismatch" in errors


def test_cover_letter_semantic_errors_require_company_specific_motivation_hook():
    errors = materials_generation._cover_letter_semantic_errors(
        cover_letter={
            "subject": "Application",
            "greeting": "Dear team",
            "paragraphs": [
                "I am applying for this role because it is a strong fit.",
                "I have relevant data engineering experience.",
                "I am excited about this opportunity and believe it is a perfect fit for my background.",
            ],
            "closing": "Kind regards,\nXingyou Li",
        },
        alignment_plan={
            "motivation_themes": [
                "Focus on data warehousing and distributed computing with PySpark",
                "Mentor-supported growth through live business cases",
            ]
        },
        company="Metyis",
    )

    assert "cover_letter_motivation_missing_company_reference" in errors
    assert any(error.startswith("cover_letter_uses_generic_phrase") for error in errors)
    assert "cover_letter_motivation_lacks_jd_specific_hook" in errors


def test_resume_semantic_errors_require_prioritized_projects_for_early_career_profile():
    errors = materials_generation._resume_semantic_errors(
        resume={
            "headline": "Junior Data Engineer",
            "summary_lines": ["Maintained 100+ pipelines and improved SQL performance for reporting workloads."],
            "skills": ["Python", "SQL"],
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
                            "bullets": ["Maintained 100+ pipelines."],
                            "source_meta": {
                                "source_type": "experience",
                                "source_id": "exp_nio_001",
                                "source_evidence_ids": ["exp_nio_001_b4"],
                            },
                        }
                    ],
                }
            ],
        },
        extracted_inventory={
            "experiences": [
                {
                    "evidence_id": "exp_nio_001",
                    "employer": "NIO Netherlands B.V.",
                    "title": "Data Engineer Intern",
                    "start_date": "Sep 2023",
                    "end_date": "May 2024",
                    "highlights": [{"evidence_id": "exp_nio_001_b4"}],
                }
            ],
            "projects": [
                {
                    "evidence_id": "proj_covid_001",
                    "name": "COVID-19 Data Pipeline with Azure Data Factory",
                    "date_range": "Jul - Aug 2025",
                    "highlights": [{"evidence_id": "proj_covid_001_b1"}],
                },
                {
                    "evidence_id": "proj_stock_001",
                    "name": "Stock Market ELT Pipeline with Apache Airflow",
                    "date_range": "Jan - Feb 2026",
                    "highlights": [{"evidence_id": "proj_stock_001_b1"}],
                },
                {
                    "evidence_id": "proj_f1_001",
                    "name": "Formula 1 Data Lake - Azure Databricks & Spark",
                    "date_range": "Jun - Jul 2025",
                    "highlights": [{"evidence_id": "proj_f1_001_b1"}],
                },
            ],
        },
        alignment_plan={
            "selected_evidence_ids": [
                "exp_nio_001_b4",
                "proj_covid_001_b1",
                "proj_stock_001_b1",
                "proj_f1_001_b1",
            ],
            "prioritized_experience_ids": ["exp_nio_001"],
            "prioritized_project_ids": ["proj_covid_001", "proj_stock_001", "proj_f1_001"],
        },
    )

    assert "missing_prioritized_project_entry" in errors


def test_merge_candidate_profile_backfills_contact_from_resume_text():
    merged = materials_generation._merge_candidate_profile(
        extracted_candidate_profile={"location": "Dordrecht, Netherlands"},
        resume_text=(
            "# Xingyou Li\n\n"
            "- Location: Dordrecht, Netherlands\n"
            "- Phone: +31 644 955 158\n"
            "- Email: xingyoulee@icloud.com\n"
            "- LinkedIn: linkedin.com/in/xingyou-li\n"
        ),
        profile_name="Xingyou Li",
    )

    assert merged["name"] == "Xingyou Li"
    assert merged["location"] == "Dordrecht, Netherlands"
    assert merged["contact"]["email"] == "xingyoulee@icloud.com"
    assert merged["contact"]["phone"] == "+31 644 955 158"
    assert merged["contact"]["linkedin"] == "linkedin.com/in/xingyou-li"


def test_run_validated_json_stage_retries_after_shape_validation_error(monkeypatch):
    payloads = iter(
        [
            ({"subject": "Application", "greeting": "Dear team", "paragraphs": ["One", "Two", "Three"]}, "gpt-5.4"),
            (
                {
                    "subject": "Application",
                    "greeting": "Dear team",
                    "paragraphs": ["One", "Two", "Three"],
                    "closing": "Kind regards,\nXingyou Li",
                },
                "gpt-5.4",
            ),
        ]
    )
    monkeypatch.setattr(materials_generation, "_run_json_stage", lambda **kwargs: next(payloads))

    payload, model_name = materials_generation._run_validated_json_stage(
        endpoints=[{"name": "test", "request_url": "https://example.com", "api_key": "key"}],
        model_name="gpt-5.4",
        prompt="Return cover letter JSON.",
        stage_name="cover_letter",
        start_index=0,
        semantic_validator=lambda parsed: [],
    )

    assert payload["closing"] == "Kind regards,\nXingyou Li"
    assert model_name == "gpt-5.4"


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
        candidate_profile={
            "location": "Amsterdam, NL",
            "contact": {"email": "levi@example.com", "phone": "+31 600 000 000", "linkedin": "provided"},
        },
    )

    assert "resume-page" in html
    assert "skills-list" in html
    assert "section-title" in html
    assert "Amsterdam, NL" in html
    assert "levi@example.com" in html
    assert "LinkedIn" not in html
    assert "<p>A</p>" in html
    assert "<p>D</p>" not in html
    assert "Kafka" in html
    assert "Pandas" not in html
    assert "B1" in html
    assert "B2" in html
    assert "B3" not in html
    assert "P3" not in html


def test_render_resume_document_html_keeps_three_projects_for_early_career_profiles():
    html = materials_rendering.render_resume_document_html(
        {
            "headline": "Data Engineer",
            "summary_lines": ["A", "B", "C"],
            "skills": ["Python", "SQL", "Airflow", "Spark"],
            "sections": [
                {
                    "title": "Experience",
                    "entries": [
                        {
                            "header_fields": {"role": "Role 1", "company": "Acme", "dates": "2024"},
                            "bullets": ["B1", "B2"],
                        }
                    ],
                },
                {
                    "title": "Projects",
                    "entries": [
                        {
                            "header_fields": {"role": "Proj 1", "company": "", "dates": "2023"},
                            "bullets": ["P1"],
                        },
                        {
                            "header_fields": {"role": "Proj 2", "company": "", "dates": "2022"},
                            "bullets": ["P2"],
                        },
                        {
                            "header_fields": {"role": "Proj 3", "company": "", "dates": "2021"},
                            "bullets": ["P3"],
                        },
                    ],
                },
            ],
        },
        profile_name="Levi",
    )

    assert "Proj 1" in html
    assert "Proj 2" in html
    assert "Proj 3" in html


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
        candidate_profile={"location": "Amsterdam, NL", "contact": {"email": "xingyou@example.com"}},
    )

    assert "Should not appear" not in markdown
    assert "## Notes" not in markdown
    assert "Amsterdam, NL · xingyou@example.com" in markdown


def test_render_resume_markdown_accepts_flat_entry_shapes():
    markdown = materials_rendering.render_resume_markdown(
        {
            "headline": "Data Engineer",
            "summary_lines": [],
            "skills": ["Python", "SQL", "Airflow", "dbt", "Spark", "AWS", "Docker", "Kafka", "Pandas"],
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

    assert "Pandas" not in markdown
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

    response = client.get("/materials", params={"token": "signed-token"})

    assert response.status_code == 200
    assert "Provider unavailable." in response.text
    assert "Failed" in response.text
    assert "Retry generation" in response.text
    assert "No resume preview available yet." in response.text
    assert "No cover letter preview available yet." in response.text


def test_materials_page_does_not_auto_start_generation(monkeypatch):
    _require_testclient()
    webapp_main = importlib.import_module("webapp.main")
    client = TestClient(webapp_main.app)
    queued_calls = []

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
        "get_material_generation_context",
        lambda profile_id, job_id: {
            "profile_id": 7,
            "job_id": "job-123",
            "title": "Data Engineer",
            "company": "Acme",
            "display_name": "Levi",
            "fit_decision": "Strong Fit",
            "fit_score": 88,
            "job_url": "https://example.com/jobs/123",
        },
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_latest_material_generation",
        lambda profile_id, job_id: None,
    )
    monkeypatch.setattr(webapp_main.database, "get_material_artifacts", lambda generation_id: [])
    monkeypatch.setattr(
        webapp_main.materials_generation,
        "generate_materials_for_profile_job",
        lambda **kwargs: queued_calls.append(kwargs),
    )

    response = client.get("/materials", params={"token": "signed-token"})

    assert response.status_code == 200
    assert "Materials have not been generated yet." in response.text
    assert "Generation starts only after you click this button." in response.text
    assert "Generate materials" in response.text
    assert queued_calls == []


def test_materials_generate_post_queues_generation_on_click(monkeypatch):
    _require_testclient()
    webapp_main = importlib.import_module("webapp.main")
    client = TestClient(webapp_main.app)
    queued_calls = []
    created_generations = []

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
        "get_material_generation_context",
        lambda profile_id, job_id: {
            "profile_id": 7,
            "job_id": "job-123",
            "title": "Data Engineer",
            "company": "Acme",
            "display_name": "Levi",
            "fit_decision": "Strong Fit",
            "fit_score": 88,
            "job_url": "https://example.com/jobs/123",
        },
    )
    monkeypatch.setattr(
        webapp_main.database,
        "get_latest_material_generation",
        lambda profile_id, job_id: None,
    )
    monkeypatch.setattr(
        webapp_main.database,
        "create_material_generation",
        lambda **kwargs: (created_generations.append(kwargs) or 41),
    )
    monkeypatch.setattr(
        webapp_main.materials_generation,
        "generate_materials_for_profile_job",
        lambda **kwargs: queued_calls.append(kwargs),
    )

    response = client.post("/materials/generate", params={"token": "signed-token"}, follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/materials?token=signed-token"
    assert created_generations == [
        {
            "profile_id": 7,
            "job_id": "job-123",
            "access_token_id": 11,
            "status": "pending",
            "stage": "queued",
        }
    ]
    assert queued_calls == [
        {
            "profile_id": 7,
            "job_id": "job-123",
            "access_token_id": 11,
            "generation_id": 41,
        }
    ]


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
