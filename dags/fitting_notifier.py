from airflow.decorators import dag, task

from datetime import datetime
import os
import pandas as pd

import database


@dag(
    start_date=datetime(2023, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=['linkedin_fitting_notifier'],
)
def linkedin_fitting_notifier():

    @task
    def claim_fitting_tasks(limit=10):
        env_limit = int(os.getenv("FITTING_CLAIM_LIMIT", str(limit)))
        return database.claim_pending_fitting_tasks(limit=env_limit)

    @task
    def fetch_jobs_for_fitting(queue_items):
        if not queue_items:
            return []

        job_ids = [item.get("job_id") for item in queue_items if item.get("job_id")]
        if not job_ids:
            return []

        jobs_df = database.get_jobs_by_ids(job_ids)
        if jobs_df is None or jobs_df.empty:
            return []

        jobs_df = jobs_df.astype(object).where(pd.notna(jobs_df), None)
        return jobs_df.to_dict(orient="records")

    @task
    def match_jobs_with_resume_llm(job_records, batch_size=5):
        if not job_records:
            return pd.DataFrame()

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
                if not jd_text.strip():
                    j["llm_match"] = None
                    j["llm_match_error"] = "missing_job_description"
                    continue

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

                    model_name = os.getenv("FITTING_MODEL_NAME", "gpt-5.2")
                    payload = {
                        "model": model_name,
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
        if jobs_with_match_df is None or jobs_with_match_df.empty:
            return 0
        database.save_llm_matches(jobs_with_match_df)
        return len(jobs_with_match_df)

    @task
    def finalize_fitting_queue(queue_items, jobs_with_match_df):
        if not queue_items:
            return {"done": 0, "failed": 0}

        max_attempts = int(os.getenv("FITTING_MAX_ATTEMPTS", "3"))

        results = {}
        if jobs_with_match_df is not None and not jobs_with_match_df.empty:
            for row in jobs_with_match_df[["id", "llm_match", "llm_match_error"]].itertuples(index=False, name=None):
                job_id, llm_match, llm_match_error = row
                results[job_id] = {
                    "llm_match": llm_match,
                    "llm_match_error": llm_match_error,
                }

        done = 0
        failed = 0
        for item in queue_items:
            job_id = item.get("job_id")
            attempts = int(item.get("attempts") or 0)
            result = results.get(job_id)
            if result and result.get("llm_match") and not result.get("llm_match_error"):
                database.mark_fitting_done(job_id)
                done += 1
            else:
                error = "missing_llm_match"
                if result and result.get("llm_match_error"):
                    error = result["llm_match_error"]
                retry = (attempts + 1) < max_attempts
                database.mark_fitting_failed(job_id, error=error, retry=retry)
                failed += 1

        return {"done": done, "failed": failed}

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
                "🎯 Job Match\n"
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

    queue_items = claim_fitting_tasks()
    job_records = fetch_jobs_for_fitting(queue_items)
    jobs_with_llm_match_df = match_jobs_with_resume_llm(job_records)
    store_result = store_fitting_results(jobs_with_llm_match_df)
    finalize_result = finalize_fitting_queue(queue_items, jobs_with_llm_match_df)

    store_result >> finalize_result

    jobs_to_notify = select_jobs_for_notification()
    store_result >> jobs_to_notify
    notify_discord(jobs_to_notify)


linkedin_fitting_notifier()
