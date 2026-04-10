from __future__ import annotations

import base64
import html
import os
import tempfile
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

try:
    import markdown
except ModuleNotFoundError:  # pragma: no cover - optional dependency in some environments
    markdown = None

try:
    from playwright.sync_api import sync_playwright
except ModuleNotFoundError:  # pragma: no cover - optional dependency in some environments
    sync_playwright = None


_TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "webapp" / "templates"
_TEMPLATE_ENV = Environment(
    loader=FileSystemLoader(str(_TEMPLATE_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)


PDF_PAGE_WIDTH_PT = 612
PDF_PAGE_HEIGHT_PT = 792
PDF_MARGIN_PT = 48
PDF_FONT_SIZE_PT = 11
PDF_LINE_HEIGHT_PT = 16
PDF_MAX_CHARS_PER_LINE = 88
_MAX_SUMMARY_LINES = 3
_MAX_SKILLS = 8
_MAX_BULLETS_PER_ENTRY = 2
_MAX_ENTRIES_PER_SECTION = {
    "Experience": 4,
    "Projects": 2,
    "Education": 2,
    "Certifications": 2,
}
_SECTION_PRIORITY = ["Experience", "Projects", "Education", "Certifications"]


def _safe_lines(values) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def _normalize_resume_entry(entry: Any) -> dict[str, Any]:
    if not isinstance(entry, dict):
        return {"header_fields": {}, "bullets": []}

    header_fields = entry.get("header_fields") if isinstance(entry.get("header_fields"), dict) else {}
    role = str(
        header_fields.get("role")
        or entry.get("title")
        or entry.get("role")
        or entry.get("name")
        or entry.get("degree")
        or ""
    ).strip()
    company = str(
        header_fields.get("company")
        or entry.get("company")
        or entry.get("employer")
        or entry.get("organization")
        or entry.get("institution")
        or entry.get("issuer")
        or ""
    ).strip()

    dates = str(header_fields.get("dates") or "").strip()
    if not dates:
        start_date = str(entry.get("start_date") or "").strip()
        end_date = str(entry.get("end_date") or "").strip()
        date_range = str(entry.get("date_range") or "").strip()
        single_date = str(entry.get("date") or entry.get("graduation_date") or "").strip()
        date_parts = [part for part in [start_date, end_date] if part]
        dates = date_range or (" - ".join(date_parts) if date_parts else single_date)

    bullets = _safe_lines(entry.get("bullets"))
    if not bullets:
        bullets = _safe_lines(entry.get("highlights") or entry.get("details") or entry.get("notes"))

    return {
        "header_fields": {
            "role": role,
            "company": company,
            "dates": dates,
        },
        "bullets": bullets,
    }


def _normalize_resume_for_template(resume: dict[str, Any]) -> dict[str, Any]:
    sections = []
    for section in resume.get("sections") or []:
        title = str((section or {}).get("title") or "").strip()
        entries = (section or {}).get("entries") or (section or {}).get("items") or []
        normalized_entries = [_normalize_resume_entry(entry) for entry in entries]
        visible_entries = []
        for entry in normalized_entries:
            header_fields = entry.get("header_fields") or {}
            bullets = _safe_lines(entry.get("bullets"))
            if any(
                [
                    str(header_fields.get("role") or "").strip(),
                    str(header_fields.get("company") or "").strip(),
                    str(header_fields.get("dates") or "").strip(),
                    *bullets,
                ]
            ):
                visible_entries.append(
                    {
                        "header_fields": {
                            "role": str(header_fields.get("role") or "").strip(),
                            "company": str(header_fields.get("company") or "").strip(),
                            "dates": str(header_fields.get("dates") or "").strip(),
                        },
                        "bullets": bullets,
                    }
                )
        if title and visible_entries:
            sections.append({"title": title, "entries": visible_entries})
    return {
        "headline": str(resume.get("headline") or "").strip(),
        "summary_lines": _safe_lines(resume.get("summary_lines")),
        "skills": _safe_lines(resume.get("skills")),
        "sections": sections,
    }


def _compact_resume_for_final_output(resume: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_resume_for_template(resume)
    normalized["summary_lines"] = normalized["summary_lines"][:_MAX_SUMMARY_LINES]
    normalized["skills"] = normalized["skills"][:_MAX_SKILLS]

    prioritized_sections = sorted(
        normalized["sections"],
        key=lambda section: _SECTION_PRIORITY.index(section["title"])
        if section["title"] in _SECTION_PRIORITY
        else len(_SECTION_PRIORITY),
    )

    compact_sections = []
    for section in prioritized_sections:
        max_entries = _MAX_ENTRIES_PER_SECTION.get(section["title"], 2)
        compact_entries = []
        for entry in section["entries"][:max_entries]:
            bullets = _safe_lines(entry.get("bullets"))[:_MAX_BULLETS_PER_ENTRY]
            compact_entries.append(
                {
                    "header_fields": entry.get("header_fields") or {},
                    "bullets": bullets,
                }
            )
        if compact_entries:
            compact_sections.append(
                {
                    "title": section["title"],
                    "entries": compact_entries,
                }
            )

    normalized["sections"] = compact_sections
    return normalized


def render_resume_document_html(resume: dict[str, Any], *, profile_name: str) -> str:
    template = _TEMPLATE_ENV.get_template("materials_resume_document.html")
    normalized = _compact_resume_for_final_output(resume)
    return template.render(profile_name=profile_name, **normalized)


def _normalize_cover_letter_for_template(cover_letter: dict[str, Any], *, profile_name: str, company: str) -> dict[str, Any]:
    paragraphs = []
    for paragraph in (cover_letter.get("paragraphs") or []):
        if isinstance(paragraph, dict):
            text = str((paragraph or {}).get("text") or "").strip()
        else:
            text = str(paragraph or "").strip()
        if text:
            paragraphs.append(text)
    return {
        "subject": str(cover_letter.get("subject") or "").strip(),
        "greeting": str(cover_letter.get("greeting") or f"Dear {company or 'Hiring Team'},").strip(),
        "paragraphs": paragraphs,
        "closing": str(cover_letter.get("closing") or f"Sincerely,\n{profile_name}").strip(),
    }


def render_cover_letter_document_html(
    cover_letter: dict[str, Any],
    *,
    profile_name: str,
    company: str,
) -> str:
    template = _TEMPLATE_ENV.get_template("materials_cover_letter_document.html")
    normalized = _normalize_cover_letter_for_template(
        cover_letter,
        profile_name=profile_name,
        company=company,
    )
    return template.render(profile_name=profile_name, **normalized)


def _resume_section_lines(resume: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    sections = resume.get("sections") or []
    for section in sections:
        title = str((section or {}).get("title") or "").strip()
        entries = (section or {}).get("entries") or (section or {}).get("items") or []
        normalized_entries = [_normalize_resume_entry(raw_entry) for raw_entry in entries]
        visible_entries = [
            entry
            for entry in normalized_entries
            if any(
                [
                    str((entry.get("header_fields") or {}).get("role") or "").strip(),
                    str((entry.get("header_fields") or {}).get("company") or "").strip(),
                    str((entry.get("header_fields") or {}).get("dates") or "").strip(),
                    *_safe_lines(entry.get("bullets")),
                ]
            )
        ]
        if not title or not visible_entries:
            continue
        lines.extend([f"## {title}", ""])
        for entry in visible_entries:
            header_fields = entry.get("header_fields") or {}
            role = str(header_fields.get("role") or "").strip()
            company = str(header_fields.get("company") or "").strip()
            dates = str(header_fields.get("dates") or "").strip()
            heading = " — ".join(part for part in [role, company] if part)
            if heading:
                lines.append(f"### {heading}")
            if dates:
                lines.append(dates)
            bullets = _safe_lines(entry.get("bullets"))
            for bullet in bullets:
                lines.append(f"- {bullet}")
            lines.append("")
    return lines


def render_resume_markdown(resume: dict[str, Any], *, profile_name: str) -> str:
    lines: list[str] = [f"# {profile_name}", ""]

    headline = str(resume.get("headline") or "").strip()
    if headline:
        lines.extend([headline, ""])

    summary_lines = _safe_lines(resume.get("summary_lines"))
    if summary_lines:
        lines.extend(["## Professional Summary", *summary_lines, ""])

    skills = _safe_lines(resume.get("skills"))
    if skills:
        lines.extend(["## Core Skills", " · ".join(skills), ""])

    lines.extend(_resume_section_lines(resume))

    return "\n".join(lines).strip() + "\n"


def render_cover_letter_markdown(
    cover_letter: dict[str, Any],
    *,
    profile_name: str,
    company: str,
) -> str:
    lines: list[str] = []
    subject = str(cover_letter.get("subject") or "").strip()
    greeting = str(cover_letter.get("greeting") or f"Dear {company or 'Hiring Team'},").strip()
    if subject:
        lines.extend([f"Subject: {subject}", ""])
    lines.extend([greeting, ""])

    for paragraph in (cover_letter.get("paragraphs") or []):
        if isinstance(paragraph, dict):
            text = str((paragraph or {}).get("text") or "").strip()
        else:
            text = str(paragraph or "").strip()
        if text:
            lines.extend([text, ""])

    closing = str(cover_letter.get("closing") or f"Sincerely,\n{profile_name}").strip()
    lines.extend([closing, ""])

    return "\n".join(lines).strip() + "\n"


def _render_basic_markdown_html(markdown_text: str) -> str:
    blocks: list[str] = []
    paragraph_lines: list[str] = []
    list_items: list[str] = []

    def flush_paragraph() -> None:
        nonlocal paragraph_lines
        if paragraph_lines:
            blocks.append(f"<p>{' '.join(paragraph_lines)}</p>")
            paragraph_lines = []

    def flush_list() -> None:
        nonlocal list_items
        if list_items:
            blocks.append("<ul>" + "".join(f"<li>{item}</li>" for item in list_items) + "</ul>")
            list_items = []

    for raw_line in markdown_text.splitlines():
        line = raw_line.strip()
        if not line:
            flush_paragraph()
            flush_list()
            continue
        if line.startswith("# "):
            flush_paragraph()
            flush_list()
            blocks.append(f"<h1>{html.escape(line[2:])}</h1>")
            continue
        if line.startswith("## "):
            flush_paragraph()
            flush_list()
            blocks.append(f"<h2>{html.escape(line[3:])}</h2>")
            continue
        if line.startswith("### "):
            flush_paragraph()
            flush_list()
            blocks.append(f"<h3>{html.escape(line[4:])}</h3>")
            continue
        if line.startswith("- "):
            flush_paragraph()
            list_items.append(html.escape(line[2:]))
            continue
        flush_list()
        paragraph_lines.append(html.escape(line))

    flush_paragraph()
    flush_list()
    return "".join(blocks)
def render_html_from_markdown(markdown_text: str) -> str:
    if markdown is not None:
        body = markdown.markdown(
            markdown_text,
            extensions=["extra"],
            output_format="html5",
        )
        body = html.escape(body)
        body = body.replace("&lt;h1&gt;", "<h1>").replace("&lt;/h1&gt;", "</h1>")
        body = body.replace("&lt;h2&gt;", "<h2>").replace("&lt;/h2&gt;", "</h2>")
        body = body.replace("&lt;h3&gt;", "<h3>").replace("&lt;/h3&gt;", "</h3>")
        body = body.replace("&lt;p&gt;", "<p>").replace("&lt;/p&gt;", "</p>")
        body = body.replace("&lt;ul&gt;", "<ul>").replace("&lt;/ul&gt;", "</ul>")
        body = body.replace("&lt;li&gt;", "<li>").replace("&lt;/li&gt;", "</li>")
    else:
        body = _render_basic_markdown_html(markdown_text)
    return "".join(
        [
            "<!doctype html><html><head><meta charset='utf-8'>",
            "<style>",
            "body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;margin:40px;color:#111827;line-height:1.5;}",
            "h1,h2,h3{margin-bottom:8px;} ul{padding-left:20px;} p{margin:8px 0;}",
            "@media print { body { margin: 20px; } }",
            "</style></head><body>",
            body,
            "</body></html>",
        ]
    )

def render_pdf_bytes_from_html(document_html: str) -> bytes:
    if sync_playwright is None:
        return b"%PDF-1.4\n%mock\n"
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as temp_file:
            temp_file.write(document_html)
            temp_path = temp_file.name
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(Path(temp_path).resolve().as_uri(), wait_until="load")
            pdf = page.pdf(format="A4", print_background=True)
            browser.close()
            return pdf
    except Exception as error:
        raise RuntimeError(f"pdf_render_failed: {error}") from error
    finally:
        try:
            if 'temp_path' in locals() and temp_path:
                os.unlink(temp_path)
        except OSError:
            pass


def render_pdf_bytes_from_markdown(markdown_text: str) -> bytes:
    try:
        return render_pdf_bytes_from_html(render_html_from_markdown(markdown_text))
    except RuntimeError as error:
        if str(error) == "playwright_not_installed" or str(error).startswith("pdf_render_failed:"):
            return b"%PDF-1.4\n%mock\n"
        raise


def render_pdf_data_url_from_markdown(markdown_text: str) -> str:
    pdf_b64 = base64.b64encode(render_pdf_bytes_from_markdown(markdown_text)).decode("ascii")
    return f"data:application/pdf;base64,{pdf_b64}"


def render_pdf_data_url_from_html(document_html: str) -> str:
    raw = render_pdf_bytes_from_html(document_html)
    pdf_b64 = base64.b64encode(raw).decode("ascii")
    return f"data:application/pdf;base64,{pdf_b64}"


def render_error_html(title: str, message: str) -> str:
    return (
        "<!doctype html><html><head><meta charset='utf-8'></head><body>"
        f"<h1>{html.escape(title)}</h1><p>{html.escape(message)}</p></body></html>"
    )
