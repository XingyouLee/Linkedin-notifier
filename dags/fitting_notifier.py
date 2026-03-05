from airflow.decorators import dag, task

from datetime import date, datetime
import os
import pandas as pd
from dotenv import load_dotenv
import time
from dags import database


def _load_env():
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

    if not os.getenv("GMN_API_KEY"):
        for env_path in env_candidates:
            try:
                load_dotenv(env_path, override=True)
            except Exception:
                pass


_load_env()


def _to_xcom_safe_value(value):
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    return value


def _df_to_xcom_records(df: pd.DataFrame):
    if df is None or df.empty:
        return []

    safe_df = df.astype(object).where(pd.notna(df), None).copy()
    for col in safe_df.columns:
        safe_df[col] = safe_df[col].map(_to_xcom_safe_value)
    return safe_df.to_dict(orient="records")


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=['linkedin_fitting_notifier'],
)
def linkedin_fitting_notifier():

    @task
    def claim_fitting_tasks():
        return database.claim_pending_fitting_tasks(limit=None)

    @task
    def fetch_jobs_for_fitting(queue_items):
        queue_items = queue_items or []
        print("Number of queue items: ", len(queue_items))
        if not queue_items:
            return []

        job_ids = [item.get("job_id") for item in queue_items if item.get("job_id")]
        if not job_ids:
            return []

        jobs_df = database.get_jobs_by_ids(job_ids)
        if jobs_df is None or jobs_df.empty:
            return []

        return _df_to_xcom_records(jobs_df)

    @task
    def match_jobs_with_resume_llm(job_records, batch_size=5):
        if not job_records:
            return pd.DataFrame()

        import json
        import requests
        _load_env()

        def _extract_output_text(response_json):
            output_text = response_json.get("output_text")
            if output_text:
                return output_text

            for item in (response_json.get("output") or []):
                if item.get("type") != "message":
                    continue
                for content in (item.get("content") or []):
                    text = content.get("text")
                    if text:
                        return text
            return None

        def _persist_one_result(job_item):
            job_id = job_item.get("id")
            if not job_id:
                return

            save_df = pd.DataFrame(
                [
                    {
                        "id": job_id,
                        "llm_match": job_item.get("llm_match"),
                        "llm_match_error": job_item.get("llm_match_error"),
                    }
                ]
            )
            database.save_llm_matches(save_df)

            llm_error = job_item.get("llm_match_error")
            if llm_error:
                print(f"llm_result job_id={job_id} status=error error={llm_error}")
                return

            fit_score = None
            decision = None
            try:
                parsed_match = json.loads(job_item.get("llm_match") or "{}")
                fit_score = parsed_match.get("fit_score")
                decision = parsed_match.get("decision")
            except Exception:
                pass
            print(
                f"llm_result job_id={job_id} status=ok "
                f"fit_score={fit_score} decision={decision}"
            )

        api_key = os.getenv("GMN_API_KEY")
        if not api_key:
            print("No LLM API key found. Skipping LLM matching.")
            for job in job_records or []:
                job["llm_match"] = None
                job["llm_match_error"] = "missing_llm_api_key"
                _persist_one_result(job)
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
                _persist_one_result(job)
            return pd.DataFrame(job_records or [])

        jobs = job_records or []
        for i in range(0, len(jobs), batch_size):
            batch = jobs[i:i + batch_size]

            for j in batch:
                try:
                    jd_text = j.get("description") or ""
                    if not jd_text.strip():
                        j["llm_match"] = None
                        j["llm_match_error"] = "missing_job_description"
                        _persist_one_result(j)
                        continue

                    base_prompt = (
                        "If output is not valid JSON, regenerate until valid. "
                        "Do not include markdown. Do not include trailing commas. "
                        "You are a strict job-fit evaluator, with years of experience in recruitment from the Netherlands. Evaluate whether the candidate fits the job description. The candidate is opening to multiple roles, like Data Engineer, Data Scientist, Data Analyst, Machine Learning Engineer, Python Developer, etc."
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
                        "5. Extract JD experience requirement clearly into exp_requirement. "
                        "If not explicitly stated, set exp_requirement to 'not specified'. "
                        "Return ONLY valid JSON. No explanations outside JSON. No markdown. No comments. "
                        "JSON structure: "
                        "{"
                        '"fit_score": 0-100, '
                        '"decision": "Strong Fit | Moderate Fit | Weak Fit | Not Recommended", '
                        '"exp_requirement": "one-line JD experience requirement", '
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
                        request_url = os.getenv("FITTING_REQUEST_URL")
                        try:
                            r = requests.post(
                                request_url,
                                headers={
                                    "Content-Type": "application/json",
                                    "Authorization": f"Bearer {api_key}",
                                },
                                json=payload,
                                timeout=120,
                            )
                            r.raise_for_status()
                            response_json = r.json()
                            output_text = _extract_output_text(response_json)
                            if not output_text:
                                raise ValueError("response_missing_output_text")

                            parsed = json.loads(output_text)
                            if not isinstance(parsed, dict):
                                raise ValueError("response_json_not_object")
                            if "fit_score" not in parsed or "decision" not in parsed:
                                raise ValueError("response_missing_fit_fields")
                            break
                        except Exception as e:
                            last_error = str(e)

                    if parsed is not None:
                        j["llm_match"] = json.dumps(parsed, ensure_ascii=False)
                        j["llm_match_error"] = None
                    else:
                        j["llm_match"] = None
                        j["llm_match_error"] = last_error or "invalid_json_response"
                except Exception as e:
                    j["llm_match"] = None
                    j["llm_match_error"] = f"unexpected_job_error: {e}"

                _persist_one_result(j)

        return pd.DataFrame(jobs)

    @task
    def store_fitting_results(jobs_with_match_df):
        if jobs_with_match_df is None or jobs_with_match_df.empty:
            return 0
        print("LLM results already persisted per response.")
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
        return _df_to_xcom_records(jobs_df)

    @task
    def notify_discord(jobs_to_notify):
        import json
        import requests
        from datetime import datetime

        _load_env()

        channel_id = "1476129860450779147"
        bot_token = os.getenv("DISCORD_BOT_TOKEN")
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

        def _send_discord_message(content: str):
            try:
                if bot_token:
                    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
                    headers = {
                        "Authorization": f"Bot {bot_token}",
                        "Content-Type": "application/json",
                    }
                    resp = requests.post(url, headers=headers, json={"content": content}, timeout=30)
                elif webhook_url:
                    resp = requests.post(webhook_url, json={"content": content}, timeout=30)
                else:
                    return False, "Missing DISCORD_BOT_TOKEN or DISCORD_WEBHOOK_URL"

                resp.raise_for_status()
                return True, None
            except Exception as e:
                return False, str(e)

        jobs_to_notify = jobs_to_notify or []
        eligible = len(jobs_to_notify)
        sent = 0
        failed = 0

        for job in jobs_to_notify:
            job_id = job.get("id")
            title = job.get("title") or "Unknown title"
            company = job.get("company") or "Unknown company"
            fit_score = job.get("fit_score")
            fit_decision = job.get("fit_decision")
            job_url = job.get("job_url") or ""
            exp_requirement = "not specified"

            llm_match = job.get("llm_match")
            if llm_match:
                try:
                    parsed = llm_match if isinstance(llm_match, dict) else json.loads(llm_match)
                    exp_requirement = parsed.get("exp_requirement") or "not specified"
                except Exception:
                    pass

            message = (
                "🎯 Job Match\n"
                f"ID: {job_id}\n"
                f"Title: {title}\n"
                f"Company: {company}\n"
                f"Decision: {fit_decision}\n"
                f"Fit Score: {fit_score}\n"
                f"Exp Requirement: {exp_requirement}\n"
                f"URL: {job_url}"
            )

            ok, error = _send_discord_message(message)
            if ok:
                database.mark_job_notified(job_id, status="sent")
                sent += 1
            else:
                database.mark_job_notified(job_id, status="failed", error=error)
                failed += 1
            time.sleep(1)

        summary_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        summary_message = (
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📌 Fitting Notification Summary\n"
            f"Time: {summary_time}\n"
            f"Eligible: {eligible}\n"
            f"Sent: {sent}\n"
            f"Failed: {failed}\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )

        summary_sent, summary_error = _send_discord_message(summary_message)
        if not summary_sent:
            print(f"Failed to send summary message: {summary_error}")

        return {
            "eligible": eligible,
            "sent": sent,
            "failed": failed,
            "summary_sent": summary_sent,
        }

    queue_items = claim_fitting_tasks()
    job_records = fetch_jobs_for_fitting(queue_items)
    jobs_with_llm_match_df = match_jobs_with_resume_llm(job_records)
    store_result = store_fitting_results(jobs_with_llm_match_df)
    finalize_result = finalize_fitting_queue(queue_items, jobs_with_llm_match_df)

    store_result >> finalize_result

    jobs_to_notify = select_jobs_for_notification()
    finalize_result >> jobs_to_notify
    notify_discord(jobs_to_notify)


linkedin_fitting_notifier()
