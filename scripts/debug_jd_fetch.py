#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dags.jd_api_worker import _fetch_job_posting_html, extract_description  # noqa: E402


def _context_snippet(text: str, pattern: str, radius: int = 300) -> str | None:
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return None
    start = max(0, match.start() - radius)
    end = min(len(text), match.end() + radius)
    return text[start:end]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Debug JD fetch/extraction for one LinkedIn job id."
    )
    parser.add_argument("job_id")
    args = parser.parse_args()

    session = requests.Session()
    html = _fetch_job_posting_html(session, args.job_id)
    description = extract_description(html)

    print(f"JOB_ID={args.job_id}")
    print(f"HTML_LENGTH={len(html)}")
    print(f"EXTRACTED_LENGTH={len(description) if description else 0}")
    print("EXTRACTED_DESCRIPTION_START")
    print(description or "None")
    print("EXTRACTED_DESCRIPTION_END")

    for keyword in [
        "Nederlands",
        "Nederlandse",
        "taal",
        "show-more-less-html__markup",
    ]:
        print(f"KEYWORD_PRESENT[{keyword}]={keyword.lower() in html.lower()}")

    for pattern in [r"Nederlands", r"Nederlandse", r"taal"]:
        snippet = _context_snippet(html, pattern)
        if snippet:
            print(f"CONTEXT_START[{pattern}]")
            print(snippet)
            print(f"CONTEXT_END[{pattern}]")


if __name__ == "__main__":
    main()
