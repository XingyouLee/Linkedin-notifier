import asyncio
import re

from playwright.async_api import async_playwright

from dags import database

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


async def extract_description(page):
    for sel in SHOW_MORE_SELECTORS:
        btn = page.locator(sel).first
        if await btn.count() > 0:
            try:
                if await btn.is_visible():
                    await btn.click(timeout=2000)
                    await page.wait_for_timeout(500)
            except Exception:
                pass

    for sel in DESCRIPTION_SELECTORS:
        loc = page.locator(sel).first
        if await loc.count() > 0:
            text = (await loc.inner_text() or "").strip()
            if len(text) > 80 and text.lower() != "about the job":
                return text

    article = page.locator("article").first
    if await article.count() > 0:
        text = (await article.inner_text() or "").strip()
        if text.lower().startswith("about the job"):
            text = text[len("about the job"):].strip()
        if len(text) > 80:
            return text

    page_text = (await page.content())
    page_text = re.sub(r"<[^>]+>", " ", page_text)
    page_text = re.sub(r"\s+", " ", page_text).strip()
    if len(page_text) > 200:
        return page_text[:5000]

    return None


async def run_once(limit=5):
    pending_df = database.get_pending_jd_requests(limit=limit)
    items = [] if pending_df.empty else list(
        pending_df[["job_id", "job_url"]].itertuples(index=False, name=None)
    )
    if not items:
        print("No pending JD requests")
        return 0

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

        processed = 0
        for job_id, job_url in items:
            print(f"Scraping {job_id}: {job_url}")
            try:
                await page.goto(job_url, timeout=60000)
                await page.wait_for_timeout(1500)
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
