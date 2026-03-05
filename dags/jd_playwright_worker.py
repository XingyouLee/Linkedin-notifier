import asyncio
import re
from typing import Optional

from playwright.async_api import async_playwright

from dags import database

SHOW_MORE_TIMEOUT_MS = 2000
NAVIGATION_TIMEOUT_MS = 60000
POST_NAVIGATION_WAIT_MS = 1500
POST_EXPAND_WAIT_MS = 500
MIN_DESCRIPTION_LENGTH = 80
FALLBACK_MIN_PAGE_TEXT_LENGTH = 200
FALLBACK_MAX_PAGE_TEXT_LENGTH = 5000
BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--no-zygote",
]

SHOW_MORE_SELECTORS = [
    "button[data-tracking-control-name='public_jobs_show-more-html-btn']",
    "button.jobs-description__footer-button",
    "button[aria-label*='Show more']",
    "button:has-text('Show more')",
]

DESCRIPTION_SELECTORS = [
    ".show-more-less-html__markup",
    ".jobs-description__content",
    "[data-test-job-description]",
    "div.job-details-module__content",
    "[data-testid='expandable-text-box']",
    "section:has(h2:has-text('About the job')) [tabindex='-1']",
    "section:has(h2:has-text('About the job')) p",
]


def _sanitize_html_text(html_text: str) -> str:
    plain_text = re.sub(r"<[^>]+>", " ", html_text)
    plain_text = re.sub(r"\s+", " ", plain_text).strip()
    return plain_text


async def extract_description(page) -> Optional[str]:
    for sel in SHOW_MORE_SELECTORS:
        btn = page.locator(sel).first
        if await btn.count() > 0:
            try:
                if await btn.is_visible():
                    await btn.click(timeout=SHOW_MORE_TIMEOUT_MS)
                    await page.wait_for_timeout(POST_EXPAND_WAIT_MS)
            except Exception:
                pass

    for sel in DESCRIPTION_SELECTORS:
        loc = page.locator(sel).first
        if await loc.count() > 0:
            text = (await loc.inner_text() or "").strip()
            if len(text) > MIN_DESCRIPTION_LENGTH and text.lower() != "about the job":
                return text

    article = page.locator("article").first
    if await article.count() > 0:
        text = (await article.inner_text() or "").strip()
        if text.lower().startswith("about the job"):
            text = text[len("about the job"):].strip()
        if len(text) > MIN_DESCRIPTION_LENGTH:
            return text

    page_text = _sanitize_html_text(await page.content())
    if len(page_text) > FALLBACK_MIN_PAGE_TEXT_LENGTH:
        return page_text[:FALLBACK_MAX_PAGE_TEXT_LENGTH]

    return None


async def run_once(limit: int = 5) -> int:
    pending_df = database.get_pending_jd_requests(limit=limit)
    items = (
        []
        if pending_df.empty
        else list(pending_df[["job_id", "job_url"]].itertuples(index=False, name=None))
    )
    if not items:
        print("No pending JD requests")
        return 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=BROWSER_ARGS)
        page = await browser.new_page()

        processed = 0
        for job_id, job_url in items:
            print(f"Scraping {job_id}: {job_url}")
            try:
                await page.goto(job_url, timeout=NAVIGATION_TIMEOUT_MS)
                await page.wait_for_timeout(POST_NAVIGATION_WAIT_MS)
                description = await extract_description(page)
                if description:
                    database.save_jd_result(job_id, description=description)
                else:
                    database.save_jd_result(job_id, description_error="empty_description")
            except Exception as e:
                database.save_jd_result(job_id, description_error=str(e))
            processed += 1

        await browser.close()

    return processed


if __name__ == "__main__":
    count = asyncio.run(run_once())
    print(f"Processed {count} queued jobs")
