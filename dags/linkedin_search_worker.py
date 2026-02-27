import asyncio
import re
from urllib.parse import urlencode

import pandas as pd
from playwright.async_api import async_playwright


SHOW_MORE_SELECTORS = [
    "button.infinite-scroller__show-more-button",
    "button:has-text('See more jobs')",
    "button:has-text('More jobs')",
]


def _build_search_url(term: str, location: str, geo_id: str, hours_old: int) -> str:
    params = {
        "keywords": term,
        "distance": "25",
    }
    if location:
        params["location"] = location
    if geo_id:
        params["geoId"] = str(geo_id)
    if hours_old and int(hours_old) > 0:
        params["f_TPR"] = f"r{int(hours_old) * 3600}"

    return f"https://www.linkedin.com/jobs/search/?{urlencode(params)}"


def _extract_job_id(job_url: str) -> str:
    if not job_url:
        return ""
    match = re.search(r"/jobs/view/(?:[^/?]*-)?(\d+)", job_url)
    if match:
        return match.group(1)
    match = re.search(r"currentJobId=(\d+)", job_url)
    return match.group(1) if match else ""


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
                await page.wait_for_timeout(300)
        except Exception:
            pass


async def _collect_visible_jobs(page):
    return await page.evaluate(
        """
        () => {
          const cards = Array.from(document.querySelectorAll('li'));
          const rows = [];
          for (const card of cards) {
            const link = card.querySelector("a[href*='/jobs/view/']");
            const title = card.querySelector('h3');
            const company = card.querySelector('h4');
            if (!link || !title || !company) continue;
            rows.push({
              job_url: link.href || null,
              title: title.textContent ? title.textContent.trim() : null,
              company: company.textContent ? company.textContent.trim() : null,
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
          const list = document.querySelector('.jobs-search__results-list');
          if (list && list.lastElementChild) {
            list.lastElementChild.scrollIntoView({ block: 'end' });
          }
          window.scrollBy(0, 1600);
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

    await page.wait_for_timeout(1200)


async def _scrape_term(page, term, location, geo_id, hours_old, max_results, max_rounds):
    url = _build_search_url(term=term, location=location, geo_id=geo_id, hours_old=hours_old)
    await page.goto(url, timeout=60000)
    await page.wait_for_timeout(1800)
    await _dismiss_popups(page)

    seen_ids = set()
    results = []
    stalled_rounds = 0

    for _ in range(max_rounds):
        before = len(seen_ids)
        visible_jobs = await _collect_visible_jobs(page)

        for row in visible_jobs:
            job_id = _extract_job_id(row.get("job_url"))
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)
            results.append(
                {
                    "id": job_id,
                    "site": "linkedin",
                    "job_url": row.get("job_url"),
                    "title": row.get("title"),
                    "company": row.get("company"),
                    "search_term": term,
                }
            )
            if len(results) >= max_results:
                return results

        if len(seen_ids) == before:
            stalled_rounds += 1
        else:
            stalled_rounds = 0

        if stalled_rounds >= 3:
            break

        await _scroll_one_round(page)

    return results


async def _scrape_linkedin_jobs_async(
    terms,
    location,
    geo_id,
    hours_old,
    max_results_per_term,
    max_scroll_rounds,
):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
            ],
        )
        page = await browser.new_page()

        all_rows = []
        for term in terms:
            rows = await _scrape_term(
                page=page,
                term=term,
                location=location,
                geo_id=geo_id,
                hours_old=hours_old,
                max_results=max_results_per_term,
                max_rounds=max_scroll_rounds,
            )
            all_rows.extend(rows)

        await browser.close()

    return all_rows


def scrape_linkedin_jobs(
    terms,
    location,
    geo_id,
    hours_old,
    max_results_per_term=25,
    max_scroll_rounds=18,
):
    if not terms:
        return pd.DataFrame(columns=["id", "site", "job_url", "title", "company", "search_term"])

    rows = asyncio.run(
        _scrape_linkedin_jobs_async(
            terms=terms,
            location=location,
            geo_id=geo_id,
            hours_old=hours_old,
            max_results_per_term=max_results_per_term,
            max_scroll_rounds=max_scroll_rounds,
        )
    )
    return pd.DataFrame(rows)
