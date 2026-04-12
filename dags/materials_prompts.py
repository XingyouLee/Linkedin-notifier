from __future__ import annotations

import json
from textwrap import dedent

PROMPT_VERSION = "materials_v4"

EXTRACTION_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["candidate_profile", "experiences", "projects", "education", "constraints"],
}

ALIGNMENT_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["target_role", "must_cover", "gaps", "selected_evidence_ids", "banned_claims", "tone", "keywords"],
}

RESUME_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["headline", "summary_lines", "sections", "skills"],
}

COVER_LETTER_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["subject", "greeting", "paragraphs", "closing"],
}


def build_extraction_prompt(*, resume_text: str, candidate_summary: dict) -> str:
    return dedent(
        f"""
        Return valid JSON only. No markdown. No comments. No trailing commas.
        You are extracting a factual evidence inventory from a candidate resume.
        Only include claims grounded in the provided resume text.
        Do not invent employers, titles, dates, metrics, skills, certifications, or project outcomes.
        Keep extraction conservative.

        Output JSON keys:
        - candidate_profile
        - experiences
        - projects
        - education
        - constraints

        Each experience/project bullet should keep a short source snippet and a stable evidence_id.
        Candidate summary baseline: {json.dumps(candidate_summary, ensure_ascii=False)}
        Candidate resume: <<<{resume_text}>>>
        """
    ).strip()


def build_alignment_prompt(
    *,
    job_title: str,
    job_description: str,
    extracted_inventory: dict,
    candidate_summary: dict,
) -> str:
    return dedent(
        f"""
        Return valid JSON only. No markdown. No comments. No trailing commas.
        You are aligning a job description against a candidate evidence inventory.
        Select only evidence that is truly supported.
        Explicitly list missing requirements and banned claims.
        Do not create new evidence.
        Keep experience boundaries intact: one source role stays one source role.

        Output JSON keys:
        - target_role
        - must_cover
        - gaps
        - selected_evidence_ids
        - banned_claims
        - tone
        - keywords

        You may also include optional helper keys for downstream writing quality, such as:
        - prioritized_experience_ids
        - prioritized_project_ids
        - summary_themes
        - motivation_themes

        Alignment rules:
        - prefer the strongest 3-5 evidence items rather than broad keyword coverage
        - keep projects, coursework, and employment evidence clearly separated
        - do not use keyword matching to justify unsupported seniority or ownership
        - motivation_themes should capture concrete company, team, domain, or mission hooks from the JD when present
        - if you include prioritized_experience_ids or prioritized_project_ids, they must use the exact evidence_id values from Extracted inventory entries verbatim
        - never invent shorthand ids like exp_1 or proj_1; copy the actual extracted evidence_id strings exactly

        Candidate summary baseline: {json.dumps(candidate_summary, ensure_ascii=False)}
        Job title: <<<{job_title}>>>
        Job description: <<<{job_description}>>>
        Extracted inventory: {json.dumps(extracted_inventory, ensure_ascii=False)}
        """
    ).strip()


def build_resume_prompt(
    *,
    job_title: str,
    company: str,
    job_description: str,
    extracted_inventory: dict,
    alignment_plan: dict,
) -> str:
    return dedent(
        f"""
        Return valid JSON only. No markdown. No comments. No trailing commas.
        You are writing a tailored, submission-ready resume for a real job application.
        The output should read like a polished ATS-friendly resume, not an audit report.
        Use only approved evidence from the alignment plan and extracted inventory.
        Do not add new employers, dates, titles, metrics, degrees, certifications, skills, or claims.
        Do not include internal caveats, warnings, missing requirements, or negative screening notes in the final resume.
        Emphasize the strongest supported evidence for the target role and keep the tone confident, concrete, and concise.

        Output JSON keys:
        - headline
        - summary_lines
        - sections
        - skills

        Resume content rules:
        - target a polished, submission-ready one-page resume for this role unless the approved evidence clearly requires more space
        - prioritize recruiter readability and credibility over keyword stuffing
        - headline should look like a real candidate headline tailored to the target role
        - headline must describe the candidate as a professional profile, not as an applicant; do not use words like "candidate", "applicant", "seeking", or "looking for"
        - summary_lines should be 2-3 strong lines focused on relevant strengths, tools, and business impact
        - summary_lines should sound seniority-appropriate for the actual candidate; do not inflate an internship/early-career profile into a senior owner
        - at least one summary line should point to concrete supported evidence such as pipeline scale, data modeling work, ETL ownership, or another specific source-backed responsibility
        - sections should prioritize the highest-value evidence for the target role and avoid empty sections
        - prefer standard recruiter/ATS-readable titles such as Experience, Projects, Education, Certifications, and Skills
        - within each section, order entries by target-role relevance and strength, strongest first
        - assume lower-ranked entries may be trimmed for one-page fit, so put must-keep evidence first instead of keeping source order
        - if alignment_plan.prioritized_project_ids contains relevant project evidence, keep a Projects section with the strongest 1-2 projects unless the candidate already has enough stronger work experience to fill the page credibly
        - for early-career profiles with one internship or limited formal work history, do not collapse the resume to only work experience when strong prioritized projects exist
        - for early-career profiles with limited formal work history, prefer keeping the strongest 2-3 projects when they materially strengthen technical credibility for the role
        - skills should stay concise and limited to the most relevant supported tools, domains, and keywords
        - trim sections and entries aggressively so only the strongest supported content remains
        - usually 1-2 bullets per entry; add more only when the evidence is unusually strong and still supports a one-page outcome
        - prefer quality over quantity; include fewer stronger bullets rather than many weak ones
        - each bullet should sound like a real resume bullet, with action + scope/result when supported
        - when a metric is not supported, write concrete scope or responsibility instead of inventing numbers
        - bullets should show why the work mattered, not just repeat tool names
        - avoid generic filler phrases such as "results-driven", "passionate", "proven track record", "dynamic", or "fast-paced environment"
        - Experience entries must map 1:1 to actual employment/internship roles from extracted_inventory.experiences
        - Projects, coursework, thesis work, and certifications must stay out of the Experience section unless they were truly jobs
        - do not split one source experience into multiple entries to preserve extra bullets
        - do not invent sub-roles, alternate titles, or narrower pseudo-roles from one source experience just to match the JD
        - if a source experience has several strong bullets, keep them under one entry and trim lower-priority bullets instead of duplicating the header
        - do not repeat identical role/company/dates headers within a section
        - never mention banned claims, missing requirements, lack of evidence, or weaknesses
        - use alignment-plan helper themes if present, but they never override the source evidence boundaries

        Resume schema requirements:
        - sections must be a list of section objects
        - each section object must contain: title, entries
        - entries must be a list of entry objects
        - every entry object must use this shape:
          {{
            "header_fields": {{
              "role": "primary title, project name, degree, or certification name",
              "company": "employer, institution, issuer, or organization if applicable",
              "dates": "date range or single date string if applicable"
            }},
            "bullets": ["bullet 1", "bullet 2"],
            "source_meta": {{
              "source_type": "experience | project | education | certification | skill",
              "source_id": "stable id from extracted_inventory when applicable",
              "source_evidence_ids": ["stable evidence ids that support this entry"]
            }}
          }}
        - Experience entries must include source_meta with source_type="experience", a real source_id from extracted_inventory.experiences, and only supporting experience evidence ids
        - Project entries should include source_meta with source_type="project" and a real source_id from extracted_inventory.projects when applicable
        - Education and Certifications entries should include source_meta.source_type and source_id when they come from extracted_inventory
        - source_meta.source_id and source_meta.source_evidence_ids must copy the exact extracted evidence_id values verbatim; never invent shorthand ids, renumber them, or swap ids between entries
        - do not use alternate entry shapes like employer/title/highlights or institution/degree/date_range at the top level of an entry
        - use empty strings or empty bullet arrays when a field is unknown instead of inventing data
        - omit sections entirely if there is no strong supported content for them

        Example section entry:
        {{
          "title": "Experience",
          "entries": [
            {{
              "header_fields": {{
                "role": "Data Engineer Intern",
                "company": "NIO Netherlands B.V.",
                "dates": "2023-09 - 2024-05"
              }},
              "bullets": [
                "Optimized SQL queries across multiple data pipelines.",
                "Maintained pipeline triggers and dependencies."
              ],
              "source_meta": {{
                "source_type": "experience",
                "source_id": "exp_nio_001",
                "source_evidence_ids": ["exp_nio_001_b1", "exp_nio_001_b4"]
              }}
            }}
          ]
        }}

        Target company: <<<{company or 'Unknown company'}>>>
        Target role: <<<{job_title}>>>
        Job description: <<<{job_description}>>>
        Extracted inventory: {json.dumps(extracted_inventory, ensure_ascii=False)}
        Alignment plan: {json.dumps(alignment_plan, ensure_ascii=False)}
        """
    ).strip()


def build_cover_letter_prompt(
    *,
    job_title: str,
    company: str,
    job_description: str,
    extracted_inventory: dict,
    alignment_plan: dict,
    resume_text: str,
    generated_resume: dict,
) -> str:
    return dedent(
        f"""
        Return valid JSON only. No markdown. No comments. No trailing commas.
        You are writing a real, submission-ready cover letter for a specific job application.
        Assume the target role is in the Netherlands and write in a Dutch-market professional style: direct, grounded, and specific.
        Use the original resume text for tone, chronology, and context, but do not copy resume bullets verbatim.
        Use only evidence that is supported by the original resume and approved by the alignment plan.
        The letter should sound like a thoughtful application written by the candidate, not an ATS summary or audit report.
        Do not claim experience, achievements, years, leadership scope, or domain background that is not supported.
        Do not include internal warnings, missing requirements, or negative screening notes in the final letter.
        Keep the letter focused on fit, relevant experience, motivation, and why this candidate is a strong match now.

        Output JSON keys:
        - subject
        - greeting
        - paragraphs
        - closing

        Cover letter content rules:
        - write a concise, submission-ready letter in 3 short paragraphs only
        - the original resume text and job description are the primary writing inputs; treat the alignment plan as a claim filter, not the main writing voice
        - keep the tone practical, modestly confident, and internationally professional
        - avoid exaggerated enthusiasm, aggressive self-promotion, or overly salesy language
        - paragraph 1: open naturally with role fit and why this role makes sense now, not a mechanical list of degree + tools
        - paragraph 2: read as a narrative connecting 2-3 strongest experiences to the role, not a list of tools and metrics
        - paragraph 3: give specific motivation for this company and role, ideally naming one concrete product, domain, team, problem, or mission clue from the JD when available, then close with forward-looking interest in a concise way
        - align the letter's narrative with the generated resume — highlight the same strengths, do not contradict or introduce evidence the resume omitted
        - respond to specific requirements or themes from the job description where supported evidence exists
        - keep every paragraph compact, specific, and human-sounding
        - use the original resume only for grounding and context; do not copy bullet wording directly
        - avoid sounding like an ATS summary, capability inventory, or generic template
        - avoid generic motivation that could fit any employer; motivation should clearly connect to this company or role
        - motivation should sound specific enough that replacing the company name would make the paragraph obviously weaker
        - avoid generic phrases such as "I am passionate about", "results-driven", "dream opportunity", or "perfect fit"
        - keep seniority and ownership claims honest; do not exaggerate leadership or strategic scope for an early-career profile
        - no filler or repetition
        - paragraphs may be strings or objects with a text field
        - never mention missing qualifications, language gaps, lack of evidence, or other warnings
        - closing must contain only the sign-off (e.g. "Kind regards,\\nXingyou Li") — do not repeat the name or sign-off anywhere in the paragraphs

        Original candidate resume: <<<{resume_text}>>>

        Job description: <<<{job_description}>>>

        Generated resume (submitted alongside this letter): {json.dumps(generated_resume, ensure_ascii=False)}

        Target company: <<<{company or 'Hiring Team'}>>>
        Target role: <<<{job_title}>>>
        Extracted inventory: {json.dumps(extracted_inventory, ensure_ascii=False)}
        Alignment plan: {json.dumps(alignment_plan, ensure_ascii=False)}
        """
    ).strip()
