import asyncio
import os
import re
import sqlite3

from playwright.async_api import async_playwright

DB_PATH = "/usr/local/airflow/include/jobs.db"

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


def get_pending(limit=5):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT job_id, job_url
        FROM jd_queue
        WHERE status='pending'
        ORDER BY updated_at ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def save_result(job_id, description=None, error=None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if description:
        cur.execute("UPDATE jobs SET description=?, description_error=NULL WHERE id=?", (description, job_id))
        cur.execute(
            "UPDATE jd_queue SET status='done', attempts=attempts+1, updated_at=CURRENT_TIMESTAMP, error=NULL WHERE job_id=?",
            (job_id,),
        )
    else:
        cur.execute("UPDATE jobs SET description=NULL, description_error=? WHERE id=?", (error, job_id))
        cur.execute(
            "UPDATE jd_queue SET status='failed', attempts=attempts+1, updated_at=CURRENT_TIMESTAMP, error=? WHERE job_id=?",
            (error, job_id),
        )

    conn.commit()
    conn.close()


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
    items = get_pending(limit=limit)
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
                    save_result(job_id, description=description)
                else:
                    save_result(job_id, error="empty_description")
            except Exception as e:
                save_result(job_id, error=str(e))
            processed += 1

        await browser.close()

    return processed


if __name__ == "__main__":
    count = asyncio.run(run_once())
    print(f"Processed {count} queued jobs")
