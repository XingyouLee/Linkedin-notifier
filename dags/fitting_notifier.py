from airflow.sdk import dag, task

import json
from datetime import datetime
import os
import pandas as pd
import requests
import time
from dags import database
from dags.runtime_utils import df_to_xcom_records, load_env


load_env(required_keys=["GMN_API_KEY"], override_if_missing=True)


def _build_match_task_result(
    jobs=None,
    *,
    api_error: bool = False,
    api_error_message: str | None = None,
):
    return {
        "jobs": jobs or [],
        "api_error": api_error,
        "api_error_message": api_error_message,
    }


def _unwrap_match_task_result(match_result):
    if not match_result:
        return [], False, None
    if isinstance(match_result, dict):
        return (
            match_result.get("jobs") or [],
            bool(match_result.get("api_error")),
            match_result.get("api_error_message"),
        )
    return match_result or [], False, None


def _build_job_match_result(
    job_id: str,
    *,
    llm_match: str | None = None,
    llm_match_error: str | None = None,
):
    return {
        "id": job_id,
        "llm_match": llm_match,
        "llm_match_error": llm_match_error,
    }


def _iter_job_ids(job_items):
    for item in job_items or []:
        if isinstance(item, dict):
            job_id = item.get("id") or item.get("job_id")
        else:
            job_id = item
        if job_id:
            yield str(job_id)


def _build_uniform_error_results(job_records, error_message: str):
    results = []
    for job_id in _iter_job_ids(job_records):
        results.append(_build_job_match_result(job_id, llm_match_error=error_message))
    return results


def _complete_results_after_api_error(job_records, partial_results, error_message: str):
    results = list(partial_results or [])
    seen_ids = {row.get("id") for row in results if row.get("id")}
    for job_id in _iter_job_ids(job_records):
        if job_id in seen_ids:
            continue
        results.append(_build_job_match_result(job_id, llm_match_error=error_message))
    return results


def _log_job_match_result(job_result):
    job_id = job_result.get("id")
    if not job_id:
        return

    llm_error = job_result.get("llm_match_error")
    if llm_error:
        print(f"llm_result job_id={job_id} status=error error={llm_error}")
        return

    fit_score = None
    decision = None
    try:
        parsed_match = json.loads(job_result.get("llm_match") or "{}")
        fit_score = parsed_match.get("fit_score")
        decision = parsed_match.get("decision")
    except Exception:
        pass

    print(
        f"llm_result job_id={job_id} status=ok "
        f"fit_score={fit_score} decision={decision}"
    )


def _extract_output_text(response_json):
    output_text = response_json.get("output_text")
    if output_text:
        return output_text

    for item in response_json.get("output") or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            text = content.get("text")
            if text:
                return text
    return None


def _load_resume_text():
    resume_candidates = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "resume.md")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "resume.md")),
    ]
    resume_error = None
    for resume_path in resume_candidates:
        try:
            with open(resume_path, "r", encoding="utf-8") as file_handle:
                return file_handle.read(), None
        except Exception as error:
            resume_error = error

    return None, f"resume_read_error: {resume_error}"


def _build_fit_prompt(jd_text: str, resume_text: str) -> str:
    return (
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
        "Job Description: <<<"
        + jd_text
        + ">>> Candidate Resume: <<<"
        + resume_text
        + ">>>"
    )


def _request_llm_match(*, request_url: str, api_key: str, model_name: str, prompt: str):
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
    response = requests.post(
        request_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        json=payload,
        timeout=120,
    )
    response.raise_for_status()

    response_json = response.json()
    output_text = _extract_output_text(response_json)
    if not output_text:
        raise ValueError("response_missing_output_text")

    parsed = json.loads(output_text)
    if not isinstance(parsed, dict):
        raise ValueError("response_json_not_object")
    if "fit_score" not in parsed or "decision" not in parsed:
        raise ValueError("response_missing_fit_fields")
    return parsed


def _send_discord_message(content: str):
    channel_id = os.getenv("DISCORD_CHANNEL_ID", "1476129860450779147")
    bot_token = os.getenv("DISCORD_BOT_TOKEN")
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    try:
        if bot_token:
            url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
            headers = {
                "Authorization": f"Bot {bot_token}",
                "Content-Type": "application/json",
            }
            response = requests.post(
                url, headers=headers, json={"content": content}, timeout=30
            )
        elif webhook_url:
            response = requests.post(webhook_url, json={"content": content}, timeout=30)
        else:
            return False, "Missing DISCORD_BOT_TOKEN or DISCORD_WEBHOOK_URL"

        response.raise_for_status()
        return True, None
    except Exception as error:
        return False, str(error)


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["linkedin_fitting_notifier"],
)
def linkedin_fitting_notifier():
    @task
    def claim_fitting_tasks():
        return database.claim_pending_fitting_tasks()

    @task
    def extract_job_ids_for_fitting(queue_items):
        queue_items = queue_items or []
        print("Number of queue items: ", len(queue_items))
        if not queue_items:
            return []

        return [str(item.get("job_id")) for item in queue_items if item.get("job_id")]

    @task
    def match_jobs_with_resume_llm(job_ids, batch_size=5):
        job_ids = [str(job_id) for job_id in (job_ids or []) if job_id]
        if not job_ids:
            return _build_match_task_result([])

        load_env(required_keys=["GMN_API_KEY"], override_if_missing=True)
        matched_jobs = []

        def _return_api_error(message: str, partial_results=None):
            error_message = str(message)
            print(f"LLM API error detected, stop early: {error_message}")
            return _build_match_task_result(
                _complete_results_after_api_error(
                    job_ids, partial_results, error_message
                ),
                api_error=True,
                api_error_message=error_message,
            )

        api_key = os.getenv("GMN_API_KEY")
        if not api_key:
            error_message = "missing_llm_api_key"
            error_results = _build_uniform_error_results(job_ids, error_message)
            for job_result in error_results:
                _log_job_match_result(job_result)
            return _return_api_error(error_message, error_results)

        request_url = os.getenv("FITTING_REQUEST_URL")
        if not request_url:
            error_message = "missing_fitting_request_url"
            error_results = _build_uniform_error_results(job_ids, error_message)
            for job_result in error_results:
                _log_job_match_result(job_result)
            return _return_api_error(error_message, error_results)

        resume_text, resume_error = _load_resume_text()

        if not resume_text:
            error_results = _build_uniform_error_results(
                job_ids,
                resume_error or "resume_read_error",
            )
            for job_result in error_results:
                _log_job_match_result(job_result)
            return _build_match_task_result(error_results)

        jobs_df = database.get_jobs_by_ids(job_ids)
        if jobs_df is None or jobs_df.empty:
            error_results = _build_uniform_error_results(job_ids, "missing_job_record")
            for job_result in error_results:
                _log_job_match_result(job_result)
            return _build_match_task_result(error_results)

        required_columns = ["id", "description"]
        for col in required_columns:
            if col not in jobs_df.columns:
                jobs_df[col] = None

        safe_jobs_df = (
            jobs_df[required_columns]
            .astype(object)
            .where(pd.notna(jobs_df[required_columns]), None)
        )
        jobs_by_id = {
            str(record["id"]): record
            for record in safe_jobs_df.to_dict(orient="records")
            if record.get("id")
        }

        model_name = os.getenv("FITTING_MODEL_NAME", "gpt-5.2")
        for i in range(0, len(job_ids), batch_size):
            batch_job_ids = job_ids[i : i + batch_size]

            for job_id in batch_job_ids:
                job_record = jobs_by_id.get(job_id)
                if not job_record:
                    job_result = _build_job_match_result(
                        job_id,
                        llm_match_error="missing_job_record",
                    )
                    matched_jobs.append(job_result)
                    _log_job_match_result(job_result)
                    continue

                try:
                    jd_text = job_record.get("description") or ""
                    if not jd_text.strip():
                        job_result = _build_job_match_result(
                            job_id,
                            llm_match_error="missing_job_description",
                        )
                        matched_jobs.append(job_result)
                        _log_job_match_result(job_result)
                        continue

                    base_prompt = _build_fit_prompt(jd_text, resume_text)

                    parsed = None
                    last_error = None
                    for attempt in range(3):
                        prompt = base_prompt
                        if attempt > 0:
                            prompt += " Previous output was invalid JSON. Return strictly valid JSON only."

                        try:
                            parsed = _request_llm_match(
                                request_url=request_url,
                                api_key=api_key,
                                model_name=model_name,
                                prompt=prompt,
                            )
                            break
                        except requests.HTTPError as error:
                            status_code = (
                                error.response.status_code
                                if error.response is not None
                                else "unknown"
                            )
                            job_result = _build_job_match_result(
                                job_id,
                                llm_match_error=(
                                    f"llm_api_http_error status={status_code} "
                                    f"job_id={job_id} error={error}"
                                ),
                            )
                            matched_jobs.append(job_result)
                            _log_job_match_result(job_result)
                            return _return_api_error(
                                job_result["llm_match_error"],
                                matched_jobs,
                            )
                        except requests.RequestException as error:
                            job_result = _build_job_match_result(
                                job_id,
                                llm_match_error=(
                                    f"llm_api_request_error job_id={job_id} error={error}"
                                ),
                            )
                            matched_jobs.append(job_result)
                            _log_job_match_result(job_result)
                            return _return_api_error(
                                job_result["llm_match_error"],
                                matched_jobs,
                            )
                        except Exception as error:
                            last_error = str(error)

                    if parsed is not None:
                        job_result = _build_job_match_result(
                            job_id,
                            llm_match=json.dumps(parsed, ensure_ascii=False),
                        )
                    else:
                        job_result = _build_job_match_result(
                            job_id,
                            llm_match_error=last_error or "invalid_json_response",
                        )
                except Exception as error:
                    job_result = _build_job_match_result(
                        job_id,
                        llm_match_error=f"unexpected_job_error: {error}",
                    )

                matched_jobs.append(job_result)
                _log_job_match_result(job_result)

        return _build_match_task_result(matched_jobs)

    @task.branch
    def branch_after_llm_match(match_result):
        jobs, api_error, api_error_message = _unwrap_match_task_result(match_result)
        if api_error:
            print(
                f"LLM API error detected after processing {len(jobs)} jobs. "
                f"Branching to notify + finalize path: {api_error_message}"
            )
            return ["notify_llm_api_error", "store_fitting_results"]
        print(f"LLM matching finished without API error. jobs={len(jobs)}")
        return "store_fitting_results"

    @task
    def store_fitting_results(match_result):
        jobs_with_match_records, _, _ = _unwrap_match_task_result(match_result)
        jobs_with_match_records = jobs_with_match_records or []
        if not jobs_with_match_records:
            return 0

        save_df = pd.DataFrame(jobs_with_match_records)
        for column in ["id", "llm_match", "llm_match_error"]:
            if column not in save_df.columns:
                save_df[column] = None

        database.save_llm_matches(save_df[["id", "llm_match", "llm_match_error"]])

        success_count = sum(
            1
            for row in jobs_with_match_records
            if row.get("llm_match") and not row.get("llm_match_error")
        )
        error_count = len(jobs_with_match_records) - success_count
        print(
            f"Persisted LLM results: total={len(jobs_with_match_records)} "
            f"success={success_count} error={error_count}"
        )
        return len(jobs_with_match_records)

    @task
    def notify_llm_api_error(match_result):
        jobs_with_match_records, api_error, api_error_message = (
            _unwrap_match_task_result(match_result)
        )
        if not api_error:
            return {"alert_sent": False, "affected_jobs": 0}

        load_env()
        alert_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = (
            "⚠️ LLM API Alert\n"
            f"Time: {alert_time}\n"
            f"Error: {api_error_message or 'llm_api_error'}\n"
            f"Affected Jobs: {len(jobs_with_match_records or [])}\n"
            "Action: fitting DAG stopped early."
        )
        alert_sent, alert_error = _send_discord_message(message)
        if not alert_sent:
            print(f"Failed to send LLM API alert: {alert_error}")

        return {
            "alert_sent": alert_sent,
            "alert_error": alert_error,
            "affected_jobs": len(jobs_with_match_records or []),
        }

    @task
    def finalize_fitting_queue(queue_items, match_result):
        jobs_with_match_records, api_error, api_error_message = (
            _unwrap_match_task_result(match_result)
        )
        if not queue_items:
            return {"done": 0, "failed": 0}

        max_attempts = int(os.getenv("FITTING_MAX_ATTEMPTS", "3"))

        results = {}
        for row in jobs_with_match_records or []:
            job_id = row.get("id")
            if not job_id:
                continue
            results[job_id] = {
                "llm_match": row.get("llm_match"),
                "llm_match_error": row.get("llm_match_error"),
            }

        done = 0
        failed = 0
        default_error = api_error_message or "missing_llm_match"
        for item in queue_items:
            job_id = item.get("job_id")
            attempts = int(item.get("attempts") or 0)
            result = results.get(job_id)
            if result and result.get("llm_match") and not result.get("llm_match_error"):
                database.mark_fitting_done(job_id)
                done += 1
            else:
                error = default_error if api_error else "missing_llm_match"
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
        if jobs_df is None or jobs_df.empty:
            return []

        notify_columns = [
            "id",
            "title",
            "company",
            "fit_score",
            "fit_decision",
            "job_url",
            "llm_match",
        ]
        for col in notify_columns:
            if col not in jobs_df.columns:
                jobs_df[col] = None

        return df_to_xcom_records(jobs_df[notify_columns])

    @task
    def notify_discord(jobs_to_notify):
        import json
        from datetime import datetime

        load_env()

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
                    parsed = (
                        llm_match
                        if isinstance(llm_match, dict)
                        else json.loads(llm_match)
                    )
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

    queue_items_task = claim_fitting_tasks()
    fitting_job_ids_task = extract_job_ids_for_fitting(queue_items_task)
    match_result_task = match_jobs_with_resume_llm(fitting_job_ids_task)
    branch_task = branch_after_llm_match(match_result_task)
    store_result_task = store_fitting_results(match_result_task)
    notify_llm_api_error_task = notify_llm_api_error(match_result_task)
    finalize_result_task = finalize_fitting_queue(queue_items_task, match_result_task)

    branch_task >> [notify_llm_api_error_task, store_result_task]
    store_result_task >> finalize_result_task

    jobs_to_notify_task = select_jobs_for_notification()
    finalize_result_task >> jobs_to_notify_task
    notify_discord(jobs_to_notify_task)


linkedin_fitting_notifier()
