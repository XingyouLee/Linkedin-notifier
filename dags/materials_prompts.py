from __future__ import annotations

import json
from textwrap import dedent

PROMPT_VERSION = "materials_v2"

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

        Output JSON keys:
        - target_role
        - must_cover
        - gaps
        - selected_evidence_ids
        - banned_claims
        - tone
        - keywords

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
        - headline should look like a real candidate headline tailored to the target role
        - summary_lines should be 2-3 strong lines focused on relevant strengths, tools, and business impact
        - sections should prioritize the highest-value evidence for the target role and avoid empty sections
        - skills should stay concise and limited to the most relevant supported tools, domains, and keywords
        - trim sections and entries aggressively so only the strongest supported content remains
        - usually 1-2 bullets per entry; add more only when the evidence is unusually strong and still supports a one-page outcome
        - prefer quality over quantity; include fewer stronger bullets rather than many weak ones
        - each bullet should sound like a real resume bullet, with action + scope/result when supported
        - never mention banned claims, missing requirements, lack of evidence, or weaknesses

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
            "bullets": ["bullet 1", "bullet 2"]
          }}
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
              ]
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
        - keep the tone practical, modestly confident, and internationally professional
        - avoid exaggerated enthusiasm, aggressive self-promotion, or overly salesy language
        - paragraph 1: open naturally with role fit and motivation, not a mechanical list of degree + tools
        - paragraph 2: read as a narrative connecting 2-3 strongest experiences to the role, not a list of tools and metrics
        - paragraph 3: close with specific motivation and forward-looking interest in a concise way
        - align the letter's narrative with the generated resume — highlight the same strengths, do not contradict or introduce evidence the resume omitted
        - respond to specific requirements or themes from the job description where supported evidence exists
        - keep every paragraph compact, specific, and human-sounding
        - use the original resume only for grounding and context; do not copy bullet wording directly
        - avoid sounding like an ATS summary, capability inventory, or generic template
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

