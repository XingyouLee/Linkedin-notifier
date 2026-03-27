#!/usr/bin/env python3

import argparse
import csv
import json
import math
import re
from html import unescape
from pathlib import Path
from types import SimpleNamespace
from typing import List

import pandas as pd
import requests

API_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
REQUEST_PAGE_SIZE = 10

POSTED_TO_HOURS = {
    "24h": 24,
    "3d": 72,
    "7d": 168,
    "30d": 720,
}

TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
JOB_URL_RE = re.compile(r'href="([^"]*?/jobs/view/[^"]+)"')
TITLE_RE = re.compile(r"<h3[^>]*>(.*?)</h3>", re.S)
COMPANY_RE = re.compile(r"<h4[^>]*>(.*?)</h4>", re.S)
JOB_ID_RE = re.compile(r"-(\d+)\?")
ITEM_RE = re.compile(r"<li>(.*?)</li>", re.S)

JOB_COLUMNS = ["id", "site", "job_url", "title", "company", "search_term"]


def clean_text(raw: str) -> str:
    if not raw:
        return ""
    text = unescape(raw)
    text = TAG_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def extract_job_id(job_url: str) -> str:
    if not job_url:
        return ""
    match = JOB_ID_RE.search(job_url)
    return match.group(1) if match else ""


def build_params(args, start: int):
    params = {
        "keywords": args.term,
        "distance": str(args.distance),
        "start": str(start),
    }
    if getattr(args, "location", None):
        params["location"] = str(args.location).strip()
    if args.hours_old > 0:
        params["f_TPR"] = f"r{args.hours_old * 3600}"
    return params


def parse_items(html_text: str):
    rows = []
    for item in ITEM_RE.findall(html_text):
        url_match = JOB_URL_RE.search(item)
        title_match = TITLE_RE.search(item)
        company_match = COMPANY_RE.search(item)

        if not url_match or not title_match or not company_match:
            continue

        job_url = unescape(url_match.group(1))
        job_id = extract_job_id(job_url)
        if not job_id:
            continue

        rows.append(
            {
                "id": job_id,
                "title": clean_text(title_match.group(1)),
                "company": clean_text(company_match.group(1)),
                "job_url": job_url,
            }
        )
    return rows


def fetch_page(session: requests.Session, args, start: int):
    params = build_params(args, start=start)
    response = session.get(
        API_URL,
        params=params,
        timeout=45,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            )
        },
    )
    response.raise_for_status()
    return parse_items(response.text)


def scrape_jobs(args):
    target = max(1, args.results_wanted)
    expected_pages = math.ceil(target / 25)

    all_rows = []
    seen = set()
    start = 0
    request_count = 0

    with requests.Session() as session:
        while len(all_rows) < target:
            page_rows = fetch_page(session, args, start=start)
            request_count += 1

            if not page_rows:
                print(f"request={request_count}, start={start}, fetched=0 -> stop")
                break

            added = 0
            for row in page_rows:
                if row["id"] in seen:
                    continue
                seen.add(row["id"])
                all_rows.append(row)
                added += 1
                if len(all_rows) >= target:
                    break

            logical_page = math.ceil(len(all_rows) / 25) if all_rows else 1
            print(
                f"request={request_count}, logical_page~{logical_page}/{expected_pages}, "
                f"start={start}, fetched={len(page_rows)}, added={added}, total={len(all_rows)}"
            )

            if added == 0:
                break

            start += REQUEST_PAGE_SIZE

    return all_rows


def _normalize_terms(terms) -> List[str]:
    if terms is None:
        return []
    if isinstance(terms, str):
        parts = [part.strip() for part in terms.split(",")]
    else:
        parts = [str(part).strip() for part in terms]
    return [part for part in parts if part]


def scrape_jobs_for_term(
    term: str,
    location: str = "Netherlands",
    distance: int = 25,
    hours_old: int = 168,
    results_wanted: int = 100,
):
    args = SimpleNamespace(
        term=term,
        location=location,
        distance=int(distance),
        hours_old=int(hours_old),
        results_wanted=max(1, int(results_wanted)),
    )
    rows = scrape_jobs(args)
    for row in rows:
        row["site"] = "linkedin"
        row["search_term"] = term
    return rows


def scrape_linkedin_public_jobs(
    terms,
    location="Netherlands",
    distance=25,
    hours_old=168,
    results_wanted=100,
):
    normalized_terms = _normalize_terms(terms)
    if not normalized_terms:
        return pd.DataFrame(columns=JOB_COLUMNS)

    all_rows = []
    for term in normalized_terms:
        term_rows = scrape_jobs_for_term(
            term=term,
            location=location,
            distance=distance,
            hours_old=hours_old,
            results_wanted=results_wanted,
        )
        all_rows.extend(term_rows)

    if not all_rows:
        return pd.DataFrame(columns=JOB_COLUMNS)

    jobs_df = pd.DataFrame(all_rows)
    for col in JOB_COLUMNS:
        if col not in jobs_df.columns:
            jobs_df[col] = None
    jobs_df["id"] = jobs_df["id"].fillna("").astype(str).str.strip()
    jobs_df = jobs_df[jobs_df["id"] != ""]
    jobs_df = jobs_df.drop_duplicates(subset=["id"], keep="first")
    return jobs_df[JOB_COLUMNS].reset_index(drop=True)


def write_output(rows, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix.lower() == ".csv":
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "title", "company", "job_url"])
            writer.writeheader()
            writer.writerows(rows)
        return

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def parse_args():
    parser = argparse.ArgumentParser(
        description="LinkedIn public jobs scraper using guest pagination API."
    )
    parser.add_argument(
        "--term", required=True, help="Job search term, e.g. 'Data Engineer'."
    )
    parser.add_argument(
        "--location",
        default="Netherlands",
        help="Location filter passed to the LinkedIn guest jobs API.",
    )
    parser.add_argument("--distance", type=int, default=25, help="Distance filter.")

    parser.add_argument(
        "--posted", choices=sorted(POSTED_TO_HOURS.keys()), help="Post date window."
    )
    parser.add_argument(
        "--hours-old", type=int, default=168, help="Lookback window in hours."
    )

    parser.add_argument(
        "--results-wanted",
        type=int,
        default=100,
        help="Total number of jobs to collect.",
    )
    parser.add_argument(
        "--output",
        default="output/linkedin_jobs.json",
        help="Output file path (.json or .csv).",
    )

    args = parser.parse_args()
    if args.posted:
        args.hours_old = POSTED_TO_HOURS[args.posted]

    return args


def main():
    args = parse_args()
    rows = scrape_jobs(args)
    write_output(rows, Path(args.output))
    print(f"done: collected={len(rows)}, output={args.output}")


if __name__ == "__main__":
    main()
