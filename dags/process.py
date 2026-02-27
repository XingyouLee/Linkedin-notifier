from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import os
import pandas as pd
from dotenv import load_dotenv
import random
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


_load_env()


@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False,
    tags=['linkedin_notifier'],
)
def linkedin_notifier():

    @task
    def scan_and_save_jobs(
        search_term="Data Engineer",
        search_terms=None,
        location="Netherlands",
        geo_id="102890719",
        hours_old=168,
        results_wanted=10,
    ):
        """Scrape LinkedIn jobs via guest API logic and save unique rows."""
        import math
        import re
        import time
        from html import unescape

        import requests

        api_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
        request_page_size = int(os.getenv("SCAN_REQUEST_PAGE_SIZE", "10"))
        http_max_retries = max(0, int(os.getenv("SCAN_HTTP_MAX_RETRIES", "6")))
        http_base_delay_sec = float(os.getenv("SCAN_HTTP_BASE_DELAY_SEC", "2.0"))
        http_max_delay_sec = float(os.getenv("SCAN_HTTP_MAX_DELAY_SEC", "60.0"))
        http_jitter_sec = float(os.getenv("SCAN_HTTP_JITTER_SEC", "1.5"))
        between_req_min_sec = float(os.getenv("SCAN_BETWEEN_REQUESTS_MIN_SEC", "0.8"))
        between_req_max_sec = float(os.getenv("SCAN_BETWEEN_REQUESTS_MAX_SEC", "1.8"))
        between_terms_delay_sec = float(os.getenv("SCAN_BETWEEN_TERMS_DELAY_SEC", "8.0"))
        request_timeout_sec = int(os.getenv("SCAN_REQUEST_TIMEOUT_SEC", "45"))
        tag_re = re.compile(r"<[^>]+>")
        whitespace_re = re.compile(r"\s+")
        job_url_re = re.compile(r'href="([^"]*?/jobs/view/[^"]+)"')
        title_re = re.compile(r"<h3[^>]*>(.*?)</h3>", re.S)
        company_re = re.compile(r"<h4[^>]*>(.*?)</h4>", re.S)
        job_id_re = re.compile(r"-(\d+)\?")
        item_re = re.compile(r"<li>(.*?)</li>", re.S)

        def _clean_text(raw: str) -> str:
            if not raw:
                return ""
            text = unescape(raw)
            text = tag_re.sub(" ", text)
            text = whitespace_re.sub(" ", text)
            return text.strip()

        def _extract_job_id(job_url: str) -> str:
            if not job_url:
                return ""
            match = job_id_re.search(job_url)
            return match.group(1) if match else ""

        def _build_params(term: str, start: int):
            params = {
                "keywords": term,
                "distance": str(distance),
                "start": str(start),
            }
            if location:
                params["location"] = location
            if geo_id:
                params["geoId"] = str(geo_id)
            if hours_old > 0:
                params["f_TPR"] = f"r{hours_old * 3600}"
            return params

        def _parse_items(html_text: str):
            rows = []
            for item in item_re.findall(html_text):
                url_match = job_url_re.search(item)
                title_match = title_re.search(item)
                company_match = company_re.search(item)
                if not url_match or not title_match or not company_match:
                    continue

                job_url = unescape(url_match.group(1))
                job_id = _extract_job_id(job_url)
                if not job_id:
                    continue

                rows.append(
                    {
                        "id": job_id,
                        "title": _clean_text(title_match.group(1)),
                        "company": _clean_text(company_match.group(1)),
                        "job_url": job_url,
                        "site": "linkedin",
                    }
                )
            return rows

        def _fetch_page(session: requests.Session, term: str, start: int):
            for attempt in range(http_max_retries + 1):
                try:
                    response = session.get(
                        api_url,
                        params=_build_params(term=term, start=start),
                        timeout=request_timeout_sec,
                        headers={
                            "User-Agent": (
                                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/145.0.0.0 Safari/537.36"
                            )
                        },
                    )
                except requests.RequestException as error:
                    if attempt >= http_max_retries:
                        print(
                            f"term={term}, start={start}, network_error={error}, "
                            f"attempt={attempt + 1}/{http_max_retries + 1} -> stop page"
                        )
                        return []

                    backoff = min(http_max_delay_sec, http_base_delay_sec * (2 ** attempt))
                    sleep_sec = backoff + random.uniform(0, http_jitter_sec)
                    print(
                        f"term={term}, start={start}, network_error={error}, "
                        f"retry_in={sleep_sec:.1f}s, attempt={attempt + 1}/{http_max_retries + 1}"
                    )
                    time.sleep(sleep_sec)
                    continue

                status_code = response.status_code
                if status_code == 429 or status_code >= 500:
                    if attempt >= http_max_retries:
                        print(
                            f"term={term}, start={start}, status={status_code}, "
                            f"attempt={attempt + 1}/{http_max_retries + 1} -> stop page"
                        )
                        return []

                    retry_after = response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        sleep_sec = float(retry_after)
                    else:
                        backoff = min(http_max_delay_sec, http_base_delay_sec * (2 ** attempt))
                        sleep_sec = backoff + random.uniform(0, http_jitter_sec)

                    print(
                        f"term={term}, start={start}, status={status_code}, "
                        f"retry_in={sleep_sec:.1f}s, attempt={attempt + 1}/{http_max_retries + 1}"
                    )
                    time.sleep(max(0.0, sleep_sec))
                    continue

                if status_code >= 400:
                    print(f"term={term}, start={start}, status={status_code} -> stop page")
                    return []

                return _parse_items(response.text)

            return []

        def _scrape_one_term(session: requests.Session, term: str, target_count: int):
            expected_pages = math.ceil(target_count / 25)
            term_rows = []
            seen_ids = set()
            start = 0
            request_count = 0

            while len(term_rows) < target_count:
                page_rows = _fetch_page(session=session, term=term, start=start)
                request_count += 1

                if not page_rows:
                    print(f"term={term}, request={request_count}, start={start}, fetched=0 -> stop")
                    break

                added = 0
                for row in page_rows:
                    if row["id"] in seen_ids:
                        continue
                    seen_ids.add(row["id"])
                    row["search_term"] = term
                    term_rows.append(row)
                    added += 1
                    if len(term_rows) >= target_count:
                        break

                logical_page = math.ceil(len(term_rows) / 25) if term_rows else 1
                print(
                    f"term={term}, request={request_count}, logical_page~{logical_page}/{expected_pages}, "
                    f"start={start}, fetched={len(page_rows)}, added={added}, total={len(term_rows)}"
                )

                if added == 0:
                    break

                start += request_page_size
                if between_req_max_sec > 0:
                    delay_min = min(between_req_min_sec, between_req_max_sec)
                    delay_max = max(between_req_min_sec, between_req_max_sec)
                    time.sleep(random.uniform(delay_min, delay_max))

            return term_rows

        env_terms = os.getenv("SCAN_SEARCH_TERMS", "")
        terms_input = search_terms if search_terms is not None else env_terms
        if isinstance(terms_input, str):
            terms = [t.strip() for t in terms_input.split(",") if t.strip()]
        else:
            terms = [str(t).strip() for t in (terms_input or []) if str(t).strip()]
        if not terms:
            terms = [search_term]

        results_per_term = int(os.getenv("SCAN_RESULTS_PER_TERM", str(results_wanted)))
        hours_old = int(os.getenv("SCAN_HOURS_OLD", str(hours_old)))
        distance = int(os.getenv("SCAN_DISTANCE", "25"))

        print(
            f"Scanning LinkedIn jobs: source=scripts_guest_api, terms={terms}, location={location}, "
            f"hours_old={hours_old}, results_per_term={results_per_term}, "
            f"distance={distance}"
        )

        all_rows = []
        with requests.Session() as session:
            for idx, term in enumerate(terms):
                all_rows.extend(
                    _scrape_one_term(
                        session=session,
                        term=term,
                        target_count=max(1, results_per_term),
                    )
                )
                if idx < len(terms) - 1 and between_terms_delay_sec > 0:
                    print(f"term={term} done, cooldown_before_next_term={between_terms_delay_sec:.1f}s")
                    time.sleep(between_terms_delay_sec)

        jobs_df = pd.DataFrame(all_rows)

        if jobs_df is None or jobs_df.empty:
            print("No jobs returned by scripts scraper.")
            return {"scanned_raw": 0, "scanned_unique": 0}

        raw_count = len(jobs_df)
        jobs_df["id"] = jobs_df["id"].fillna("").astype(str).str.strip()
        jobs_df = jobs_df[jobs_df["id"] != ""]
        jobs_df = jobs_df.drop_duplicates(subset=["id"], keep="first")
        unique_count = len(jobs_df)
        duplicate_count = raw_count - unique_count

        if unique_count == 0:
            print("No valid job ids found after normalization.")
            return {"scanned_raw": raw_count, "scanned_unique": 0, "scanned_duplicates": raw_count}

        if "search_term" not in jobs_df.columns:
            jobs_df["search_term"] = None
        per_term_stats = (
            jobs_df.groupby("search_term")["id"].nunique().sort_values(ascending=False).to_dict()
        )

        print(f"Per-term unique stats: {per_term_stats}")
        print(
            "Merged id stats: "
            f"total={raw_count}, unique={unique_count}, duplicates={duplicate_count}"
        )

        database.save_jobs(jobs_df[["id", "site", "job_url", "title", "company"]])

        return {
            "terms": terms,
            "scan_source": "scripts_guest_api",
            "scanned_raw": raw_count,
            "scanned_unique": unique_count,
            "scanned_duplicates": duplicate_count,
        }

    @task
    def filter_jobs():
        df = database.get_latest_batch_jobs()
        blocked_companies = ["Elevation Group", "Capgemini", "Jobster", "Sogeti", "Info Support", "CGI Nederland", ""]
        df = df[~df['company'].isin(blocked_companies)]
        df = df[~df['title'].str.contains("Senior", na=False)]
        df = df[~df['title'].str.contains("Medior", na=False)]
        print("Filtered records:", df.shape[0])
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
        import math
        import time
        from openclaw_jd_worker import run_once

        job_ids = job_ids or []
        if not job_ids:
            return {"processed": 0, "done": 0, "failed": 0, "pending": 0, "total": 0}

        worker_batch_size = int(os.getenv("JD_WORKER_BATCH_SIZE", str(worker_batch_size)))
        idle_loop_limit = int(os.getenv("JD_WORKER_IDLE_LOOP_LIMIT", str(idle_loop_limit)))
        worker_batch_size = max(1, worker_batch_size)
        idle_loop_limit = max(1, idle_loop_limit)

        total = len(job_ids)
        min_loops_needed = math.ceil(total / worker_batch_size) + idle_loop_limit
        max_loops = int(os.getenv("JD_WORKER_MAX_LOOPS", str(max(max_loops, min_loops_needed))))
        if max_loops <= 0:
            max_loops = min_loops_needed

        total_processed = 0
        idle_loops = 0

        print(
            f"JD worker config: batch_size={worker_batch_size}, "
            f"max_loops={max_loops}, idle_loop_limit={idle_loop_limit}, "
            f"min_loops_needed={min_loops_needed}, total={total}"
        )

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
            time.sleep(random.randint(1, 3))

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
            raise RuntimeError(
                "JD worker stopped with pending jobs: "
                f"{result}. Increase JD_WORKER_MAX_LOOPS (suggested >= {min_loops_needed}) "
                f"or JD_WORKER_BATCH_SIZE (current {worker_batch_size})."
            )

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
    fitting_enqueue_meta = enqueue_fitting_tasks(jobs_with_jd)

    trigger_fitting_notifier = TriggerDagRunOperator(
        task_id="trigger_fitting_notifier",
        trigger_dag_id="linkedin_fitting_notifier",
        conf={"source_dag_run_id": "{{ dag_run.run_id }}"},
        wait_for_completion=False,
    )
    fitting_enqueue_meta >> trigger_fitting_notifier


linkedin_notifier()
