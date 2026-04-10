from __future__ import annotations

import os


def load_resume_text(
    *,
    resume_path: str | None = None,
    resume_text: str | None = None,
):
    if resume_text:
        return str(resume_text), None

    resume_candidates = []
    if resume_path:
        resume_candidates.append(os.path.abspath(os.path.expanduser(resume_path)))
    resume_candidates.extend(
        [
            os.path.abspath(os.path.join(os.path.dirname(__file__), "resume.md")),
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "resume.md")),
        ]
    )
    resume_error = None
    for candidate_path in resume_candidates:
        try:
            with open(candidate_path, "r", encoding="utf-8") as file_handle:
                return file_handle.read(), None
        except Exception as error:
            resume_error = error

    return None, f"resume_read_error: {resume_error}"
