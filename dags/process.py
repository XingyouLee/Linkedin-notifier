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
        results_wanted=200,
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
        return df.to_dict(orient="records")

    @task
    def enqueue_jd_requests(job_records):
        jobs_df = pd.DataFrame(job_records or [])
        queued = database.enqueue_jd_requests(jobs_df)
        print(f"Queued {queued} JD requests for OpenClaw")
        return [j.get("id") for j in (job_records or []) if j.get("id")]

    @task
    def wait_for_jd_results(job_ids, timeout_seconds=600, poll_interval=15):
        import time

        job_ids = job_ids or []
        if not job_ids:
            return []

        start = time.time()
        while True:
            done_count = database.count_jd_queue_status(job_ids, "done")
            failed_count = database.count_jd_queue_status(job_ids, "failed")
            total = len(job_ids)
            print(f"JD queue progress: done={done_count}, failed={failed_count}, total={total}")

            if done_count + failed_count >= total:
                break

            if time.time() - start > timeout_seconds:
                raise TimeoutError("Timed out waiting for OpenClaw JD scraping results")

            time.sleep(poll_interval)

        jobs_df = database.get_jobs_by_ids(job_ids)
        import numpy as np
        jobs_df = jobs_df.replace({np.nan: None})
        return jobs_df.to_dict(orient="records")

    @task
    def match_jobs_with_resume_llm(job_records, batch_size=5):
        import json
        import requests
        from dotenv import load_dotenv

        env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
        load_dotenv(env_path)

        api_key = os.getenv("GMN_API_KEY")
        if not api_key:
            for job in job_records or []:
                job["llm_match"] = None
                job["llm_match_error"] = "missing_gmn_api_key"
            return pd.DataFrame(job_records or [])

        resume_candidates = [
            os.path.abspath(os.path.join(os.path.dirname(__file__), "resume.md")),
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "resume.md")),
        ]
        resume_text = None
        resume_error = None
        for resume_path in resume_candidates:
            try:
                with open(resume_path, "r", encoding="utf-8") as f:
                    resume_text = f.read()
                break
            except Exception as e:
                resume_error = e

        if not resume_text:
            for job in job_records or []:
                job["llm_match"] = None
                job["llm_match_error"] = f"resume_read_error: {resume_error}"
            return pd.DataFrame(job_records or [])

        jobs = job_records or []
        for i in range(0, len(jobs), batch_size):
            batch = jobs[i:i + batch_size]

            for j in batch:
                jd_text = j.get("description") or ""
                base_prompt = (
                    "If output is not valid JSON, regenerate until valid. "
                    "Do not include markdown. Do not include trailing commas. "
                    "You are a strict job-fit evaluator. Evaluate whether the candidate fits the job description. "
                    "CRITICAL RULES: "
                    "1. If the job explicitly requires Dutch (e.g., 'Dutch required', 'Fluent Dutch mandatory'): "
                    "- Set language_blocker = true "
                    "- Cap fit_score at 40 "
                    "2. If required experience >= 5 years AND candidate has < 2 years: "
                    "- Apply heavy penalty "
                    "3. Be strict and realistic. Do not be optimistic. "
                    "4. Prioritize: "
                    "- Required skills match "
                    "- Years of experience "
                    "- Language requirements "
                    "- Domain relevance "
                    "Return ONLY valid JSON. No explanations outside JSON. No markdown. No comments. "
                    "JSON structure: "
                    "{"
                    '"fit_score": 0-100, '
                    '"decision": "Strong Fit | Moderate Fit | Weak Fit | Not Recommended", '
                    '"language_check": {"dutch_required": true/false, "language_blocker": true/false, "impact": "short explanation"}, '
                    '"experience_check": {"required_years": number, "candidate_years": number, "gap_years": number, "severity": "none | minor | moderate | severe"}, '
                    '"skills_match": {"strong_matches": [], "partial_matches": [], "missing_critical_skills": []}, '
                    '"risk_factors": [], '
                    '"summary": "3-5 concise lines"'
                    "} "
                    "Job Description: <<<" + jd_text + ">>> "
                    "Candidate Resume: <<<" + resume_text + ">>>"
                )

                parsed = None
                last_error = None
                for attempt in range(3):
                    prompt = base_prompt
                    if attempt > 0:
                        prompt += " Previous output was invalid JSON. Return strictly valid JSON only."

                    payload = {
                        "model": "gpt-5.2",
                        "input": [
                            {
                                "type": "message",
                                "role": "user",
                                "content": [{"type": "input_text", "text": prompt}],
                            }
                        ],
                    }

                    try:
                        r = requests.post(
                            "https://gmn.chuangzuoli.com/v1/responses",
                            headers={
                                "Content-Type": "application/json",
                                "Authorization": f"Bearer {api_key}",
                            },
                            json=payload,
                            timeout=120,
                        )
                        r.raise_for_status()
                        response_json = r.json()
                        output_text = response_json.get("output_text")
                        if not output_text:
                            try:
                                output_text = response_json["output"][0]["content"][0]["text"]
                            except Exception:
                                output_text = json.dumps(response_json, ensure_ascii=False)

                        parsed = json.loads(output_text)
                        break
                    except Exception as e:
                        last_error = str(e)

                if parsed is not None:
                    j["llm_match"] = json.dumps(parsed, ensure_ascii=False)
                    j["llm_match_error"] = None
                else:
                    j["llm_match"] = None
                    j["llm_match_error"] = last_error or "invalid_json_response"

        return pd.DataFrame(jobs)

    @task
    def store_fitting_results(jobs_with_match_df):
        if jobs_with_match_df is None:
            return 0
        database.save_llm_matches(jobs_with_match_df)
        return len(jobs_with_match_df)

    @task
    def select_jobs_for_notification(limit=20):
        jobs_df = database.get_jobs_to_notify(limit=limit)
        print(f"Jobs eligible for notification: {len(jobs_df)}")
        return jobs_df.to_dict(orient="records")

    @task
    def notify_discord(jobs_to_notify):
        import requests
        from dotenv import load_dotenv

        env_candidates = [
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env")),
            os.path.abspath(os.path.join(os.path.dirname(__file__), ".env")),
            "/usr/local/airflow/.env",
            "/usr/local/airflow/dags/.env",
        ]
        for env_path in env_candidates:
            try:
                load_dotenv(env_path)
            except Exception:
                pass

        channel_id = "1476129860450779147"
        bot_token = os.getenv("DISCORD_BOT_TOKEN")
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

        sent = 0
        failed = 0

        for job in jobs_to_notify or []:
            job_id = job.get("id")
            title = job.get("title") or "Unknown title"
            company = job.get("company") or "Unknown company"
            fit_score = job.get("fit_score")
            fit_decision = job.get("fit_decision")
            job_url = job.get("job_url") or ""

            message = (
                f"🎯 Job Match\n"
                f"ID: {job_id}\n"
                f"Title: {title}\n"
                f"Company: {company}\n"
                f"Decision: {fit_decision}\n"
                f"Fit Score: {fit_score}\n"
                f"URL: {job_url}"
            )

            try:
                if bot_token:
                    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
                    headers = {
                        "Authorization": f"Bot {bot_token}",
                        "Content-Type": "application/json",
                    }
                    resp = requests.post(url, headers=headers, json={"content": message}, timeout=30)
                elif webhook_url:
                    resp = requests.post(webhook_url, json={"content": message}, timeout=30)
                else:
                    raise RuntimeError("Missing DISCORD_BOT_TOKEN or DISCORD_WEBHOOK_URL")

                resp.raise_for_status()
                database.mark_job_notified(job_id, status="sent")
                sent += 1
            except Exception as e:
                database.mark_job_notified(job_id, status="failed", error=str(e))
                failed += 1

        return {"sent": sent, "failed": failed}

    # Pipeline wiring (all task calls at bottom)
    scan_meta = scan_and_save_jobs()

    filtered_jobs_df = filter_jobs()
    scan_meta >> filtered_jobs_df
    filtered_job_records = df_to_records(filtered_jobs_df)

    queued_job_ids = enqueue_jd_requests(filtered_job_records)
    jobs_with_jd = wait_for_jd_results(queued_job_ids)

    jobs_with_llm_match_df = match_jobs_with_resume_llm(jobs_with_jd)
    store_result = store_fitting_results(jobs_with_llm_match_df)

    jobs_to_notify = select_jobs_for_notification()
    store_result >> jobs_to_notify
    notify_discord(jobs_to_notify)


linkedin_notifier()
