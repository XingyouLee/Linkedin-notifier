from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser
from typing import Optional

import requests

from dags import database

JOB_POSTING_API_URL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
REQUEST_TIMEOUT_SEC = 45
MIN_DESCRIPTION_LENGTH = 80
FALLBACK_MIN_PAGE_TEXT_LENGTH = 200
FALLBACK_MAX_PAGE_TEXT_LENGTH = 5000
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
DESCRIPTION_CLASS_MARKERS = (
    "show-more-less-html__markup",
    "jobs-description__content",
    "job-details-module__content",
    "description",
)


def _is_description_container(attrs: dict[str, str]) -> bool:
    class_attr = attrs.get("class", "")
    if any(marker in class_attr for marker in DESCRIPTION_CLASS_MARKERS):
        return True

    if "data-test-job-description" in attrs:
        return True

    if attrs.get("data-testid") == "expandable-text-box":
        return True

    return False


class _DescriptionHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._tag_stack = []
        self._active_targets = []
        self.candidates = []

    def handle_starttag(self, tag, attrs):
        attrs_dict = {
            str(name).lower(): ("" if value is None else str(value))
            for name, value in attrs
        }
        self._tag_stack.append(tag)
        if _is_description_container(attrs_dict):
            self._active_targets.append({"depth": len(self._tag_stack), "parts": []})

    def handle_endtag(self, tag):
        current_depth = len(self._tag_stack)
        while (
            self._active_targets and self._active_targets[-1]["depth"] == current_depth
        ):
            target = self._active_targets.pop()
            text = _sanitize_html_text("".join(target["parts"]))
            if text:
                self.candidates.append(text)
        if self._tag_stack:
            self._tag_stack.pop()

    def handle_data(self, data):
        if not data:
            return
        for target in self._active_targets:
            target["parts"].append(data)


def _sanitize_html_text(html_text: str) -> str:
    plain_text = TAG_RE.sub(" ", unescape(html_text or ""))
    plain_text = WHITESPACE_RE.sub(" ", plain_text).strip()
    return plain_text


def extract_description(html_text: str) -> Optional[str]:
    parser = _DescriptionHTMLParser()
    parser.feed(html_text or "")
    candidates = []
    seen_candidates = set()
    for candidate in parser.candidates:
        normalized_candidate = _sanitize_html_text(candidate)
        if len(normalized_candidate) < MIN_DESCRIPTION_LENGTH:
            continue
        if normalized_candidate.lower() == "about the job":
            continue
        if normalized_candidate in seen_candidates:
            continue
        seen_candidates.add(normalized_candidate)
        candidates.append(normalized_candidate)

    if candidates:
        return max(candidates, key=len)

    page_text = _sanitize_html_text(html_text or "")
    if len(page_text) >= FALLBACK_MIN_PAGE_TEXT_LENGTH:
        return page_text[:FALLBACK_MAX_PAGE_TEXT_LENGTH]
    return None


def _fetch_job_posting_html(session: requests.Session, job_id: str) -> str:
    response = session.get(
        JOB_POSTING_API_URL.format(job_id=job_id),
        timeout=REQUEST_TIMEOUT_SEC,
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    return response.text


def run_once(limit: int = 5, job_ids=None) -> int:
    pending_df = database.claim_pending_jd_requests(limit=limit, job_ids=job_ids)
    items = (
        []
        if pending_df.empty
        else list(pending_df[["job_id", "job_url"]].itertuples(index=False, name=None))
    )
    if not items:
        print("No claimable JD requests")
        return 0

    processed = 0
    with requests.Session() as session:
        for job_id, _job_url in items:
            print(f"Fetching JD via API for {job_id}")
            try:
                html_text = _fetch_job_posting_html(session, str(job_id))
                description = extract_description(html_text)
                if description:
                    database.save_jd_result(str(job_id), description=description)
                else:
                    database.save_jd_result(
                        str(job_id), description_error="empty_description"
                    )
            except requests.HTTPError as error:
                status_code = (
                    error.response.status_code
                    if error.response is not None
                    else "unknown"
                )
                database.save_jd_result(
                    str(job_id),
                    description_error=f"job_posting_http_error status={status_code}",
                )
            except Exception as error:
                database.save_jd_result(str(job_id), description_error=str(error))
            processed += 1

    return processed


if __name__ == "__main__":
    count = run_once()
    print(f"Processed {count} queued jobs")
