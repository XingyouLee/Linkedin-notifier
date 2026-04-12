from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

try:  # pragma: no cover - optional dependency
    from pypdf import PdfReader
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    try:
        from PyPDF2 import PdfReader  # type: ignore[no-redef]
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        PdfReader = None  # type: ignore[assignment]


def _read_text_resume(candidate_path: Path) -> str:
    with candidate_path.open("r", encoding="utf-8") as file_handle:
        return file_handle.read()


def _extract_pdf_text_with_pypdf(candidate_path: Path) -> str:
    if PdfReader is None:
        raise RuntimeError("pdf_reader_unavailable")

    reader = PdfReader(str(candidate_path))
    text_chunks = [page.extract_text() or "" for page in reader.pages]
    combined = "\n".join(chunk.strip() for chunk in text_chunks if chunk and chunk.strip()).strip()
    if not combined:
        raise RuntimeError("pdf_reader_empty_text")
    return combined


def _extract_pdf_text_with_pdftotext(candidate_path: Path) -> str:
    pdftotext_binary = shutil.which("pdftotext")
    if not pdftotext_binary:
        raise RuntimeError("pdftotext_unavailable")

    completed = subprocess.run(
        [
            pdftotext_binary,
            "-layout",
            "-nopgbrk",
            "-enc",
            "UTF-8",
            str(candidate_path),
            "-",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    extracted_text = (completed.stdout or "").strip()
    if not extracted_text:
        raise RuntimeError("pdftotext_empty_text")
    return extracted_text


def _read_pdf_resume(candidate_path: Path) -> str:
    errors = []
    for extractor in (
        _extract_pdf_text_with_pypdf,
        _extract_pdf_text_with_pdftotext,
    ):
        try:
            return extractor(candidate_path)
        except Exception as error:
            errors.append(f"{extractor.__name__}:{error}")

    raise RuntimeError("pdf_extract_error: " + " | ".join(errors))


def _read_resume_path(candidate_path: Path) -> str:
    if candidate_path.suffix.lower() == ".pdf":
        return _read_pdf_resume(candidate_path)
    return _read_text_resume(candidate_path)


def _build_resume_candidates(resume_path: str | None) -> list[Path]:
    if resume_path:
        return [Path(os.path.abspath(os.path.expanduser(resume_path)))]

    return [
        Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "resume.md"))),
        Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "resume.md"))),
    ]


def load_resume_text(
    *,
    resume_path: str | None = None,
    resume_text: str | None = None,
):
    if resume_text:
        return str(resume_text), None

    resume_error = None
    explicit_resume_path = bool(resume_path)
    for candidate_path in _build_resume_candidates(resume_path):
        try:
            return _read_resume_path(candidate_path), None
        except Exception as error:
            resume_error = error
            if explicit_resume_path:
                break

    return None, f"resume_read_error: {resume_error}"
