#!/usr/bin/env python3

import argparse
import asyncio
import csv
import json
import os
import re
from html import unescape
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlencode

import pandas as pd
from playwright.async_api import async_playwright

POSTED_TO_HOURS = {
    "24h": 24,
    "3d": 72,
    "7d": 168,
    "30d": 720,
}

JOB_COLUMNS = ["id", "site", "job_url", "title", "company", "search_term"]
PAGE_SIZE = 25
MIN_SCROLL_ROUNDS_PER_PAGE = 8
STALL_ROUNDS_LIMIT = 6
SHOW_MORE_SELECTORS = [
    "button.infinite-scroller__show-more-button",
    "button:has-text('See more jobs')",
    "button:has-text('More jobs')",
]
LOGIN_WALL_TEXT_RE = re.compile(
    r"(sign in to view more jobs)|(sign in to continue)|(log in to continue)",
    re.I,
)
WHITESPACE_RE = re.compile(r"\s+")


def clean_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = unescape(raw)
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def extract_job_id(job_url: Optional[str]) -> str:
    if not job_url:
        return ""
    for pattern in (
        r"/jobs/view/(?:[^/?]*-)?(\d+)",
        r"currentJobId=(\d+)",
        r"-(\d+)\?",
    ):
        match = re.search(pattern, job_url)
        if match:
            return match.group(1)
    return ""


def build_search_url(
    term: str,
    location: str,
    geo_id: str,
    distance: int,
    hours_old: int,
    start: int = 0,
) -> str:
    params = {
        "keywords": term,
    }
    try:
        distance_value = int(distance)
    except Exception:
        distance_value = 0
    if distance_value > 0:
        params["distance"] = str(distance_value)
    if location:
        params["location"] = location
    if geo_id:
        params["geoId"] = str(geo_id)
    if hours_old and int(hours_old) > 0:
        params["f_TPR"] = f"r{int(hours_old) * 3600}"
    if int(start) > 0:
        params["start"] = str(int(start))
    return f"https://www.linkedin.com/jobs/search/?{urlencode(params)}"


def _normalize_terms(terms) -> List[str]:
    if terms is None:
        return []
    if isinstance(terms, str):
        parts = [p.strip() for p in terms.split(",")]
    else:
        parts = [str(p).strip() for p in terms]
    return [p for p in parts if p]


async def _dismiss_popups(page):
    for selector in [
        "button:has-text('Reject')",
        "button:has-text('Accept')",
        "button[aria-label='Dismiss']",
        "button[aria-label='Dismiss sign-in prompt']",
    ]:
        button = page.locator(selector).first
        try:
            if await button.count() > 0 and await button.is_visible():
                await button.click(timeout=1500)
                await page.wait_for_timeout(250)
        except Exception:
            pass


async def _is_login_wall(page) -> bool:
    if any(token in page.url for token in ["/login", "/checkpoint", "/challenge"]):
        return True

    body_text = ""
    try:
        body_text = await page.evaluate(
            "() => (document.body && document.body.innerText ? document.body.innerText : '').slice(0, 3000)"
        )
    except Exception:
        pass
    if body_text and LOGIN_WALL_TEXT_RE.search(body_text):
        return True

    for selector in ["#username", "input[name='session_key']", "input[name='session_password']"]:
        try:
            if await page.locator(selector).count() > 0:
                return True
        except Exception:
            continue

    return False


async def _login(page, email: str, password: str):
    if not email or not password:
        raise RuntimeError("LinkedIn login is required but email/password are missing.")

    await page.goto("https://www.linkedin.com/login", timeout=60000)
    await page.wait_for_timeout(1200)

    username = page.locator("#username")
    pwd = page.locator("#password")
    if await username.count() == 0 or await pwd.count() == 0:
        raise RuntimeError("LinkedIn login form not found.")

    await username.fill(email)
    await pwd.fill(password)
    await page.locator("button[type='submit']").first.click()
    await page.wait_for_timeout(2500)

    if any(token in page.url for token in ["/checkpoint", "/challenge"]):
        raise RuntimeError("LinkedIn requires checkpoint verification. Complete it manually once, then retry.")

    if "login-submit" in page.url:
        raise RuntimeError("LinkedIn login failed. Check email/password.")


async def _collect_visible_jobs(page):
    return await page.evaluate(
        """
        () => {
          const rows = [];
          const seen = new Set();
          const primarySelectors = [
            ".jobs-search__results-list",
            ".jobs-search-results-list",
            ".jobs-search-results__list",
            ".jobs-search-results-list__list",
            ".scaffold-layout__list-container ul",
          ];

          let links = [];
          for (const selector of primarySelectors) {
            const containers = Array.from(document.querySelectorAll(selector));
            for (const container of containers) {
              const inContainer = Array.from(
                container.querySelectorAll("a[href*='/jobs/view/']")
              );
              if (inContainer.length > 0) {
                links = inContainer;
                break;
              }
            }
            if (links.length > 0) {
              break;
            }
          }

          if (links.length === 0) {
            links = Array.from(document.querySelectorAll("a[href*='/jobs/view/']"));
          }

          for (const link of links) {
            const href = link.href || link.getAttribute("href");
            if (!href || seen.has(href)) continue;
            seen.add(href);

            const card = link.closest(
              "li, div.base-card, div.job-search-card, div.jobs-search-results__list-item, div.scaffold-layout__list-item"
            );
            const titleEl = card && card.querySelector("h3, .base-search-card__title, .job-search-card__title");
            const companyEl = card && card.querySelector("h4, .base-search-card__subtitle, .job-search-card__subtitle, a.hidden-nested-link");

            rows.push({
              job_url: href,
              title: titleEl && titleEl.textContent ? titleEl.textContent.trim() : null,
              company: companyEl && companyEl.textContent ? companyEl.textContent.trim() : null,
            });
          }
          return rows;
        }
        """
    )


async def _scroll_one_round(page):
    await page.evaluate(
        """
        () => {
          const isScrollable = (el) => {
            if (!el) return false;
            const style = window.getComputedStyle(el);
            const overflowY = style.overflowY || "";
            const canScroll = overflowY.includes("auto") || overflowY.includes("scroll");
            return canScroll && el.scrollHeight > el.clientHeight + 20;
          };

          const cardLinks = Array.from(document.querySelectorAll("a[href*='/jobs/view/']"));
          const lastLink = cardLinks.length > 0 ? cardLinks[cardLinks.length - 1] : null;

          let target = null;
          let cursor = lastLink;
          while (cursor) {
            if (isScrollable(cursor)) {
              target = cursor;
              break;
            }
            cursor = cursor.parentElement;
          }

          if (!target) {
            const selectors = [
              ".scaffold-layout__list",
              ".scaffold-layout__list-container",
              ".jobs-search-results-list",
              ".jobs-search__results-list",
              ".jobs-search-results__list",
            ];
            for (const selector of selectors) {
              const node = document.querySelector(selector);
              if (isScrollable(node)) {
                target = node;
                break;
              }
            }
          }

          if (target) {
            const step = Math.max(500, Math.floor(target.clientHeight * 0.9));
            target.scrollTop = Math.min(target.scrollTop + step, target.scrollHeight);
          } else {
            window.scrollBy(0, 1600);
          }

          if (lastLink) {
            lastLink.scrollIntoView({ block: "end" });
          }
        }
        """
    )

    for selector in SHOW_MORE_SELECTORS:
        button = page.locator(selector).first
        try:
            if await button.count() > 0 and await button.is_visible():
                await button.click(timeout=1500)
                break
        except Exception:
            continue

    await page.wait_for_timeout(1800)


async def _open_search_page(page, url, email, password):
    await page.goto(url, timeout=60000)
    await page.wait_for_timeout(1800)
    await _dismiss_popups(page)

    if await _is_login_wall(page):
        if not email or not password:
            raise RuntimeError(
                "LinkedIn requires login for this query. Set LINKEDIN_EMAIL and LINKEDIN_PASSWORD."
            )
        await _login(page, email=email, password=password)
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(1800)
        await _dismiss_popups(page)


async def _scrape_term(
    page,
    term: str,
    location: str,
    geo_id: str,
    distance: int,
    hours_old: int,
    max_results: int,
    max_rounds: int,
    email: Optional[str],
    password: Optional[str],
):
    results = []
    seen_ids = set()
    max_page_hops = max(10, int(os.getenv("SCAN_MAX_PAGE_HOPS", str(max_results))))
    empty_pages = 0
    start = 0

    for page_idx in range(max_page_hops):
        page_url = build_search_url(
            term=term,
            location=location,
            geo_id=geo_id,
            distance=distance,
            hours_old=hours_old,
            start=start,
        )
        print(
            f"[SCAN_PAGE_URL_BEFORE_NAV] term={term} "
            f"page={page_idx + 1}/{max_page_hops} start={start} url={page_url}"
        )
        await _open_search_page(page, url=page_url, email=email, password=password)

        page_seen_ids = set()
        page_visible_ids = set()
        stalled_rounds = 0
        min_scroll_rounds = min(
            max_rounds,
            max(1, int(os.getenv("SCAN_MIN_SCROLL_ROUNDS_PER_PAGE", str(MIN_SCROLL_ROUNDS_PER_PAGE)))),
        )
        stall_round_limit = max(1, int(os.getenv("SCAN_STALL_ROUNDS_LIMIT", str(STALL_ROUNDS_LIMIT))))

        for round_idx in range(max_rounds):
            before_page = len(page_seen_ids)
            visible_rows = await _collect_visible_jobs(page)

            for row in visible_rows:
                job_url = row.get("job_url")
                job_id = extract_job_id(job_url)
                if not job_id:
                    continue

                page_visible_ids.add(job_id)

                if job_id in seen_ids:
                    continue

                seen_ids.add(job_id)
                page_seen_ids.add(job_id)
                results.append(
                    {
                        "id": job_id,
                        "site": "linkedin",
                        "job_url": job_url,
                        "title": clean_text(row.get("title")),
                        "company": clean_text(row.get("company")),
                        "search_term": term,
                    }
                )
                if len(results) >= max_results:
                    print(
                        f"[SCAN_PAGE_RESULT] term={term} page={page_idx + 1}/{max_page_hops} "
                        f"start={start} page_new={len(page_seen_ids)} total={len(results)}"
                    )
                    return results

            if len(page_seen_ids) >= PAGE_SIZE:
                break

            if len(page_seen_ids) == before_page:
                if (round_idx + 1) >= min_scroll_rounds:
                    stalled_rounds += 1
            else:
                stalled_rounds = 0

            if stalled_rounds >= stall_round_limit:
                break

            try:
                current_url = page.url
            except Exception:
                current_url = "<unknown>"
            print(
                f"[SCAN_PAGE_URL_BEFORE_SCROLL] term={term} "
                f"page={page_idx + 1}/{max_page_hops} start={start} "
                f"round={round_idx + 1}/{max_rounds} "
                f"visible={len(visible_rows)} page_new={len(page_seen_ids)} "
                f"url={current_url}"
            )
            await _scroll_one_round(page)

        page_new_count = len(page_seen_ids)
        page_visible_count = len(page_visible_ids)
        page_step = page_new_count if page_new_count > 0 else page_visible_count
        if page_step <= 0:
            page_step = 1

        print(
            f"[SCAN_PAGE_RESULT] term={term} page={page_idx + 1}/{max_page_hops} "
            f"start={start} page_new={page_new_count} page_visible={page_visible_count} "
            f"step={page_step} total={len(results)}"
        )

        if page_new_count == 0:
            empty_pages += 1
        else:
            empty_pages = 0

        if empty_pages >= 2:
            break

        start += page_step

        if len(results) >= max_results:
            break

    return results


async def _scrape_linkedin_public_jobs_async(
    terms,
    location,
    geo_id,
    distance,
    hours_old,
    max_results_per_term,
    max_scroll_rounds,
    email,
    password,
    headless,
):
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
            ],
        )
        page = await browser.new_page()
        if email and password:
            await _login(page, email=email, password=password)

        all_rows = []
        for term in terms:
            term_rows = await _scrape_term(
                page=page,
                term=term,
                location=location,
                geo_id=geo_id,
                distance=distance,
                hours_old=hours_old,
                max_results=max_results_per_term,
                max_rounds=max_scroll_rounds,
                email=email,
                password=password,
            )
            all_rows.extend(term_rows)
            await page.wait_for_timeout(900)

        await browser.close()
        return all_rows


def scrape_linkedin_public_jobs(
    terms,
    location="Netherlands",
    geo_id="102890719",
    distance=25,
    hours_old=168,
    max_results_per_term=25,
    max_scroll_rounds=18,
    email=None,
    password=None,
    headless=True,
) -> pd.DataFrame:
    normalized_terms = _normalize_terms(terms)
    if not normalized_terms:
        return pd.DataFrame(columns=JOB_COLUMNS)

    rows = asyncio.run(
        _scrape_linkedin_public_jobs_async(
            terms=normalized_terms,
            location=location,
            geo_id=geo_id,
            distance=distance,
            hours_old=hours_old,
            max_results_per_term=max_results_per_term,
            max_scroll_rounds=max_scroll_rounds,
            email=email,
            password=password,
            headless=headless,
        )
    )

    if not rows:
        return pd.DataFrame(columns=JOB_COLUMNS)

    jobs_df = pd.DataFrame(rows)
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
        with output_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=JOB_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        return

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(rows, file, ensure_ascii=False, indent=2)


def parse_args():
    parser = argparse.ArgumentParser(description="LinkedIn jobs scraper using Playwright scrolling.")
    parser.add_argument(
        "--term",
        action="append",
        help="Search term, can be repeated. Example: --term 'Data Engineer' --term 'Data Analyst'.",
    )
    parser.add_argument(
        "--terms",
        help="Comma-separated terms. Example: 'Data Engineer,Data Analyst'.",
    )
    parser.add_argument("--location", default="Netherlands", help="Location string for LinkedIn search.")
    parser.add_argument("--geo-id", default="102890719", help="LinkedIn geoId.")
    parser.add_argument("--distance", type=int, default=25, help="Distance filter.")
    parser.add_argument("--posted", choices=sorted(POSTED_TO_HOURS.keys()), help="Post date window.")
    parser.add_argument("--hours-old", type=int, default=168, help="Lookback window in hours.")
    parser.add_argument("--results-wanted", type=int, default=25, help="Max jobs per term.")
    parser.add_argument("--max-scroll-rounds", type=int, default=18, help="Maximum scroll rounds per term.")
    parser.add_argument("--output", default="output/linkedin_jobs.json", help="Output file path (.json or .csv).")
    parser.add_argument("--email", default=os.getenv("LINKEDIN_EMAIL"), help="LinkedIn account email.")
    parser.add_argument("--password", default=os.getenv("LINKEDIN_PASSWORD"), help="LinkedIn account password.")
    parser.add_argument("--headful", action="store_true", help="Run browser in headful mode.")

    args = parser.parse_args()
    if args.posted:
        args.hours_old = POSTED_TO_HOURS[args.posted]

    terms = []
    if args.term:
        terms.extend(args.term)
    if args.terms:
        terms.extend([term.strip() for term in args.terms.split(",") if term.strip()])
    args.terms = terms

    if not args.terms:
        parser.error("Please provide at least one term via --term or --terms.")

    return args


def main():
    args = parse_args()
    jobs_df = scrape_linkedin_public_jobs(
        terms=args.terms,
        location=args.location,
        geo_id=args.geo_id,
        distance=args.distance,
        hours_old=args.hours_old,
        max_results_per_term=max(1, int(args.results_wanted)),
        max_scroll_rounds=max(1, int(args.max_scroll_rounds)),
        email=args.email,
        password=args.password,
        headless=not args.headful,
    )
    rows = jobs_df.to_dict(orient="records")
    write_output(rows, Path(args.output))
    print(f"done: collected={len(rows)}, output={args.output}")


if __name__ == "__main__":
    main()
