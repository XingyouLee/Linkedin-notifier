from airflow.decorators import dag, task

from datetime import datetime
import sys
import os
import pandas as pd



import database
from jobspy import scrape_jobs

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['linkedin_notifier'],
)
def linkedin_notifier():
    
    @task
    def scan_for_new_posts():
        jobs = scrape_jobs(
            site_name=["linkedin"], # "glassdoor", "bayt", "naukri", "bdjobs"
            search_term="Data engineer",
            location="Netherlands",
            results_wanted=100,
            hours_old=72,
            country_indeed='Netherlands',
        )
        print(f"Found {len(jobs)} jobs")
        print(jobs.head())
    
        import numpy as np
        jobs = jobs.replace({np.nan: None})
        return jobs.to_dict(orient="records")

    @task
    def add_to_db(jobs_records):
        # Convert back to dataframe
        jobs_df = pd.DataFrame(jobs_records)
        database.save_jobs(jobs_df)
    
    @task
    def filter_jobs():
        df = database.get_latest_batch_jobs()
        print(df.head())

        blocked_companies = ["Elevation Group", "Capgemini", "Jobster", "Sogeti", "Info Support", "CGI Nederland", ""]
        # Exclude jobs from blocked companies
        df = df[~df['company'].isin(blocked_companies)]
        # Filter out jobs that Senior in the title
        df = df[~df['title'].str.contains("Senior", na=False)]
        return df.head(3)
    
    @task
    def retrieve_job_descriptions(job_records):
        """Retrieve and attach job descriptions for all jobs using a single browser session."""
        import asyncio
        import os
        import sys

        # Ensure the vendored linkedin_scraper repo (a package directory) is importable in the container.
        # This matches `/Users/levi/Linkedin-notifier/scrape_job_description.py`.
        SCRAPER_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "linkedin_scraper"))
        if SCRAPER_REPO not in sys.path:
            sys.path.insert(0, SCRAPER_REPO)

        from linkedin_scraper import BrowserManager, login_with_credentials
        from linkedin_scraper.scrapers.job import JobScraper
        from dotenv import load_dotenv

        load_dotenv()
        email = os.getenv("LINKEDIN_EMAIL")
        password = os.getenv("LINKEDIN_PASSWORD")

        chromium_args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-zygote",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-site-isolation-trials",
        ]

        async def _extract_description_fallback(page):
            show_more_selectors = [
                "button[data-tracking-control-name='public_jobs_show-more-html-btn']",
                "button.jobs-description__footer-button",
                "button[aria-label*='Show more']",
                "button:has-text('Show more')",
            ]
            for sel in show_more_selectors:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    try:
                        if await btn.is_visible():
                            await btn.click(timeout=2000)
                            await page.wait_for_timeout(500)
                    except Exception:
                        pass

            for sel in [
                ".show-more-less-html__markup",
                ".jobs-description__content",
                "[data-test-job-description]",
                "div.job-details-module__content",
                "[data-testid='expandable-text-box']",
                "section:has(h2:has-text('About the job')) [tabindex='-1']",
                "section:has(h2:has-text('About the job')) p",
            ]:
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
            return None

        async def scrape_all(jobs):
            async with BrowserManager(headless=True, args=chromium_args) as browser:
                if email and password:
                    print(f"Logging in as {email}...")
                    try:
                        await login_with_credentials(browser.page, email=email, password=password)
                        print("✅ Login successful")
                    except Exception as e:
                        print(f"⚠️ Login failed, continuing anyway: {e}")
                else:
                    print("⚠️ No LINKEDIN_EMAIL/PASSWORD found in environment")

                job_scraper = JobScraper(browser.page)

                for job in jobs:
                    url = job.get("job_url")
                    if not url:
                        job["description"] = None
                        job["description_error"] = "missing_job_url"
                        continue

                    print(f"📄 Scraping: {url}")
                    try:
                        scraped_job = await job_scraper.scrape(url)
                        description = scraped_job.job_description
                        if not description or len(description.strip()) <= 20 or description.strip().lower() == "about the job":
                            description = await _extract_description_fallback(browser.page)

                        job["description"] = description
                        if not job["description"]:
                            job["description_error"] = "empty_description"
                    except Exception as e:
                        print(f"❌ Scraping failed for {url}: {e}")
                        job["description"] = None
                        job["description_error"] = str(e)

                    await asyncio.sleep(2)

            return jobs

        return asyncio.run(scrape_all(job_records))

    # jobs = scan_for_new_posts()
    
    # Task 1: Save ALL newly scraped jobs to DB
    # add_to_db(jobs)
    
    # Task 2: Filter the jobs we just added
    filtered_jobs_df = filter_jobs()
    # Convert DataFrame -> list[dict] inside a task
    @task
    def df_to_records(df):
        if df is None:
            return []
        return df.to_dict(orient="records")

    filtered_job_records = df_to_records(filtered_jobs_df)
    
    @task
    def match_jobs_with_resume_llm(job_records, batch_size=5):
        """Call LLM in batches and attach raw model output per job."""
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
                    '\"fit_score\": 0-100, '
                    '\"decision\": \"Strong Fit | Moderate Fit | Weak Fit | Not Recommended\", '
                    '\"language_check\": {\"dutch_required\": true/false, \"language_blocker\": true/false, \"impact\": \"short explanation\"}, '
                    '\"experience_check\": {\"required_years\": number, \"candidate_years\": number, \"gap_years\": number, \"severity\": \"none | minor | moderate | severe\"}, '
                    '\"skills_match\": {\"strong_matches\": [], \"partial_matches\": [], \"missing_critical_skills\": []}, '
                    '\"risk_factors\": [], '
                    '\"summary\": \"3-5 concise lines\"'
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

    # Task 3: Fetch JD for all matching jobs in a single batch
    # Airflow automatically builds the dependency: filter_jobs -> df_to_records -> retrieve_job_descriptions
    jobs_with_jd = retrieve_job_descriptions(filtered_job_records)

    @task
    def store_fitting_results(jobs_with_match_df):
        if jobs_with_match_df is None:
            return 0
        database.save_llm_matches(jobs_with_match_df)
        return len(jobs_with_match_df)

    # Task 4: LLM match in batches and return dataframe with a new llm_match column
    jobs_with_llm_match_df = match_jobs_with_resume_llm(jobs_with_jd)

    # Task 5: Persist fitting results to database
    store_fitting_results(jobs_with_llm_match_df)

linkedin_notifier()
