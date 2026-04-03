from __future__ import annotations

from html import unescape
from html.parser import HTMLParser
from typing import Optional

import requests

from dags import database

JOB_POSTING_API_URL = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
REQUEST_TIMEOUT_SEC = 45
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)
DESCRIPTION_CLASSES = {"description__text", "description__text--rich"}
CRITERIA_CLASS = "description__job-criteria-list"


class _DescriptionHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._tag_stack: list[str] = []
        self._active_targets: list[dict[str, int | list[str] | str]] = []
        self.results: dict[str, list[str]] = {
            "description": [],
            "criteria": [],
        }

    def handle_starttag(self, tag, attrs):
        attrs_dict = {
            str(name).lower(): ("" if value is None else str(value))
            for name, value in attrs
        }
        self._tag_stack.append(tag)

        target_kind = self._target_kind(tag, attrs_dict)
        if target_kind:
            self._active_targets.append(
                {
                    "depth": len(self._tag_stack),
                    "parts": [],
                    "kind": target_kind,
                }
            )

    def handle_endtag(self, tag):
        current_depth = len(self._tag_stack)
        while (
            self._active_targets and self._active_targets[-1]["depth"] == current_depth
        ):
            target = self._active_targets.pop()
            text = _sanitize_html_text("".join(target["parts"]))
            if text:
                self.results[str(target["kind"])].append(text)
        if self._tag_stack:
            self._tag_stack.pop()

    def handle_data(self, data):
        if not data:
            return
        for target in self._active_targets:
            target["parts"].append(data)

    def flush_remaining_targets(self) -> None:
        while self._active_targets:
            target = self._active_targets.pop()
            text = _sanitize_html_text("".join(target["parts"]))
            if text:
                self.results[str(target["kind"])].append(text)

    def _target_kind(self, tag: str, attrs: dict[str, str]) -> str | None:
        class_names = {
            class_name.strip()
            for class_name in attrs.get("class", "").split()
            if class_name.strip()
        }

        if tag == "div" and DESCRIPTION_CLASSES.issubset(class_names):
            return "description"

        if tag in {"ul", "div"} and CRITERIA_CLASS in class_names:
            return "criteria"

        return None


def _sanitize_html_text(html_text: str) -> str:
    plain_text = unescape(html_text or "")
    plain_text = " ".join(plain_text.split())
    return plain_text.strip()


def extract_description(html_text: str) -> Optional[str]:
    parser = _DescriptionHTMLParser()
    parser.feed(html_text or "")
    parser.flush_remaining_targets()

    description_candidates = [
        _sanitize_html_text(candidate) for candidate in parser.results["description"]
    ]
    description_candidates = [candidate for candidate in description_candidates if candidate]

    criteria_candidates = [
        _sanitize_html_text(candidate) for candidate in parser.results["criteria"]
    ]
    criteria_candidates = [candidate for candidate in criteria_candidates if candidate]

    description = max(description_candidates, key=len) if description_candidates else None
    criteria = max(criteria_candidates, key=len) if criteria_candidates else None

    if description and criteria:
        return f"{description}\n\n{criteria}"
    if description:
        return description
    if criteria:
        return criteria
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
            print(f"Fetching JD via guest jobPosting for {job_id}")
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
