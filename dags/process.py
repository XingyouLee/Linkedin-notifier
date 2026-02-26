from airflow.decorators import dag, task

from datetime import datetime
import os
import re
import pandas as pd

import database


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['linkedin_notifier'],
)
def linkedin_notifier():

    @task
    def scan_and_save_jobs(
        search_term="Data Engineer",
        location="Netherlands",
        geo_id="102890719",
        hours_old=72,
        results_wanted=10,
    ):
        """Scrape LinkedIn public job search via Playwright (no login required)."""
        import asyncio
        import random
        from playwright.async_api import async_playwright

        async def _scrape():
            seconds = hours_old * 3600
            all_jobs = []
            page_num = 0

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
                page = await browser.new_page(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    )
                )

                while len(all_jobs) < results_wanted:
                    start = page_num * 25
                    url = (
                        f"https://www.linkedin.com/jobs/search/"
                        f"?keywords={search_term.replace(' ', '%20')}"
                        f"&geoId={geo_id}"
                        f"&location={location.replace(' ', '%20')}"
                        f"&f_TPR=r{seconds}"
                        f"&sortBy=DD"
                        f"&start={start}"
                    )
                    print(f"Fetching page {page_num}: {url}")

                    await page.goto(url, timeout=60000)
                    await page.wait_for_timeout(2000 + random.randint(500, 1500))

                    # Scroll down to trigger lazy-loaded cards
                    for _ in range(3):
                        await page.evaluate("window.scrollBy(0, 800)")
                        await page.wait_for_timeout(500 + random.randint(200, 600))

                    # Extract job cards from the DOM
                    cards = await page.query_selector_all("ul.jobs-search__results-list > li")
                    if not cards:
                        cards = await page.query_selector_all("li div.base-card")

                    if not cards:
                        print(f"No cards found on page {page_num}, stopping.")
                        break

                    for card in cards:
                        link_el = await card.query_selector("a.base-card__full-link")
                        if not link_el:
                            link_el = await card.query_selector("a[href*='/jobs/view/']")
                        if not link_el:
                            continue

                        href = await link_el.get_attribute("href") or ""
                        job_url = href.split("?")[0].strip()

                        job_id_match = re.search(r"-(\d{8,})$", job_url) or re.search(r"/(\d{8,})", job_url)
                        job_id = job_id_match.group(1) if job_id_match else None
                        if not job_id:
                            continue

                        title_el = await card.query_selector("h3.base-search-card__title")
                        if not title_el:
                            title_el = await card.query_selector("span.sr-only")
                        title = (await title_el.inner_text()).strip() if title_el else None

                        company_el = await card.query_selector("h4.base-search-card__subtitle a")
                        if not company_el:
                            company_el = await card.query_selector("a.hidden-nested-link")
                        company = (await company_el.inner_text()).strip() if company_el else None

                        all_jobs.append({
                            "id": job_id,
                            "site": "linkedin",
                            "job_url": job_url,
                            "title": title,
                            "company": company,
                        })

                    print(f"Page {page_num}: found {len(cards)} cards, total so far: {len(all_jobs)}")
                    page_num += 1

                    if len(cards) < 25:
                        break

                    # Random delay between pages
                    await page.wait_for_timeout(1000 + random.randint(500, 2000))

                await browser.close()

            return all_jobs[:results_wanted]

        all_jobs = asyncio.run(_scrape())

        jobs_df = pd.DataFrame(all_jobs)
        print(f"Found {len(jobs_df)} jobs total")
        if not jobs_df.empty:
            print(jobs_df.head(10))

        database.save_jobs(jobs_df)
        return {"scanned": len(all_jobs)}

    @task
    def filter_jobs():
        df = database.get_latest_batch_jobs()
        print(df.head())

        blocked_companies = ["Elevation Group", "Capgemini", "Jobster", "Sogeti", "Info Support", "CGI Nederland", ""]
        df = df[~df['company'].isin(blocked_companies)]
        df = df[~df['title'].str.contains("Senior", na=False)]
        return df

    @task
    def df_to_records(df):
        if df is None:
            return []
        clean_df = df.astype(object).where(pd.notna(df), None)
        return clean_df.to_dict(orient="records")

    @task
    def enqueue_jd_requests(job_records):
        jobs_df = pd.DataFrame(job_records or [])
        queued = database.enqueue_jd_requests(jobs_df)
        print(f"Queued {queued} JD requests for in-DAG worker")
        return [j.get("id") for j in (job_records or []) if j.get("id")]

    @task(task_id="run_jd_worker")
    def run_jd_worker(job_ids, worker_batch_size=5, max_loops=20, idle_loop_limit=2):
        import asyncio
        import time
        from openclaw_jd_worker import run_once

        job_ids = job_ids or []
        if not job_ids:
            return {"processed": 0, "done": 0, "failed": 0, "pending": 0, "total": 0}

        worker_batch_size = int(os.getenv("JD_WORKER_BATCH_SIZE", str(worker_batch_size)))
        max_loops = int(os.getenv("JD_WORKER_MAX_LOOPS", str(max_loops)))
        idle_loop_limit = int(os.getenv("JD_WORKER_IDLE_LOOP_LIMIT", str(idle_loop_limit)))

        total = len(job_ids)
        total_processed = 0
        idle_loops = 0

        for _ in range(max_loops):
            done_count = database.count_jd_queue_status(job_ids, "done")
            failed_count = database.count_jd_queue_status(job_ids, "failed")
            pending_count = total - done_count - failed_count
            print(
                f"JD queue progress(before): done={done_count}, failed={failed_count}, "
                f"pending={pending_count}, total={total}"
            )

            if pending_count <= 0:
                break

            processed = asyncio.run(run_once(limit=worker_batch_size))
            total_processed += processed

            if processed == 0:
                idle_loops += 1
                if idle_loops >= idle_loop_limit:
                    break
                time.sleep(2)
                continue

            idle_loops = 0
            time.sleep(1)

        done_count = database.count_jd_queue_status(job_ids, "done")
        failed_count = database.count_jd_queue_status(job_ids, "failed")
        pending_count = total - done_count - failed_count
        result = {
            "processed": total_processed,
            "done": done_count,
            "failed": failed_count,
            "pending": pending_count,
            "total": total,
        }

        if pending_count > 0:
            raise RuntimeError(f"JD worker stopped with pending jobs: {result}")

        return result

    @task
    def fetch_jd_results(job_ids):
        job_ids = job_ids or []
        if not job_ids:
            return []

        jobs_df = database.get_jobs_by_ids(job_ids)
        import numpy as np

        jobs_df = jobs_df.replace({np.nan: None})
        return jobs_df.to_dict(orient="records")

    @task
    def enqueue_fitting_tasks(job_records):
        jobs_df = pd.DataFrame(job_records or [])
        if "description" in jobs_df.columns:
            jobs_df = jobs_df[jobs_df["description"].notna()]
        queued = database.enqueue_fitting_requests(jobs_df)
        print(f"Queued {queued} fitting requests")
        return {"queued": queued}

    # Pipeline wiring (all task calls at bottom)
    results_wanted = int(os.getenv("SCAN_RESULTS_WANTED", "10"))
    scan_meta = scan_and_save_jobs(results_wanted=results_wanted)

    filtered_jobs_df = filter_jobs()
    scan_meta >> filtered_jobs_df
    filtered_job_records = df_to_records(filtered_jobs_df)

    queued_job_ids = enqueue_jd_requests(filtered_job_records)
    jd_worker_meta = run_jd_worker(queued_job_ids)
    jobs_with_jd = fetch_jd_results(queued_job_ids)

    jd_worker_meta >> jobs_with_jd
    enqueue_fitting_tasks(jobs_with_jd)


linkedin_notifier()
