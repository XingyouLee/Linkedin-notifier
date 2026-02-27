from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import os
import pandas as pd
from dotenv import load_dotenv
import time
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
        """Scrape LinkedIn jobs via JobSpy/Playwright and normalize ids for DB dedupe."""
        from jobspy import scrape_jobs

        def _normalize_jobs_df(raw_df: pd.DataFrame) -> pd.DataFrame:
            jobs_df = raw_df.copy()
            if "job_url" not in jobs_df.columns and "job_url_direct" in jobs_df.columns:
                jobs_df["job_url"] = jobs_df["job_url_direct"]
            if "job_url" not in jobs_df.columns:
                jobs_df["job_url"] = None
            if "site" not in jobs_df.columns:
                jobs_df["site"] = "linkedin"
            jobs_df["site"] = jobs_df["site"].fillna("linkedin")
            if "title" not in jobs_df.columns:
                jobs_df["title"] = None
            if "company" not in jobs_df.columns:
                jobs_df["company"] = None
            if "id" not in jobs_df.columns:
                jobs_df["id"] = None

            jobs_df["id"] = jobs_df["id"].fillna("").astype(str).str.strip()
            jobs_df["id"] = jobs_df["id"].str.replace(r"^li-", "", regex=True)

            missing_mask = jobs_df["id"].eq("") & jobs_df["job_url"].notna()
            if missing_mask.any():
                jobs_df.loc[missing_mask, "id"] = (
                    jobs_df.loc[missing_mask, "job_url"]
                    .astype(str)
                    .str.extract(r"(\d{8,})", expand=False)
                    .fillna("")
                )

            jobs_df = jobs_df[jobs_df["id"] != ""]
            jobs_df = jobs_df[["id", "site", "job_url", "title", "company"]]
            return jobs_df

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
        scan_source = os.getenv("SCAN_SOURCE", "hybrid").strip().lower()
        if scan_source not in {"jobspy", "playwright", "hybrid"}:
            scan_source = "hybrid"

        scroll_target_per_term = int(os.getenv("SCAN_SCROLL_TARGET_PER_TERM", "25"))
        scroll_target_per_term = max(1, min(results_per_term, scroll_target_per_term))
        scroll_max_rounds = int(os.getenv("SCAN_SCROLL_MAX_ROUNDS", "18"))

        print(
            f"Scanning LinkedIn jobs: source={scan_source}, terms={terms}, location={location}, "
            f"hours_old={hours_old}, results_per_term={results_per_term}, "
            f"scroll_target_per_term={scroll_target_per_term}"
        )
        if geo_id and scan_source == "jobspy":
            print(f"geo_id={geo_id} is ignored by JobSpy LinkedIn scraping.")

        base_cols = ["id", "site", "job_url", "title", "company"]
        empty_df = pd.DataFrame(columns=base_cols)
        term_frames = {term: empty_df.copy() for term in terms}
        term_stats = {
            term: {
                "term": term,
                "jobspy_unique": 0,
                "scroll_unique": 0,
                "merged_unique": 0,
            }
            for term in terms
        }
        scroll_terms = []

        for term in terms:
            jobspy_df = empty_df.copy()
            if scan_source in {"jobspy", "hybrid"}:
                try:
                    raw_df = scrape_jobs(
                        site_name=["linkedin"],
                        search_term=term,
                        location=location,
                        results_wanted=results_per_term,
                        hours_old=hours_old,
                        country_indeed="Netherlands",
                    )
                except Exception as e:
                    print(f"JobSpy failed for term='{term}': {e}")
                    raw_df = None

                if raw_df is not None and not raw_df.empty:
                    jobspy_df = _normalize_jobs_df(raw_df)
                    jobspy_df = jobspy_df.drop_duplicates(subset=["id"], keep="first")

                term_frames[term] = jobspy_df
                term_stats[term]["jobspy_unique"] = len(jobspy_df)
                time.sleep(random.randint(100, 200))

            needs_scroll = scan_source == "playwright" or (
                scan_source == "hybrid" and len(jobspy_df) < scroll_target_per_term
            )
            if needs_scroll:
                scroll_terms.append(term)

        if scan_source in {"playwright", "hybrid"} and scroll_terms:
            from linkedin_search_worker import scrape_linkedin_jobs

            scroll_df = scrape_linkedin_jobs(
                terms=scroll_terms,
                location=location,
                geo_id=geo_id,
                hours_old=hours_old,
                max_results_per_term=scroll_target_per_term,
                max_scroll_rounds=scroll_max_rounds,
            )

            if scroll_df is None or scroll_df.empty:
                print(f"Playwright scroll scanner returned no rows for terms={scroll_terms}")
            else:
                if "search_term" not in scroll_df.columns:
                    scroll_df["search_term"] = None

                for term in scroll_terms:
                    term_scroll_df = scroll_df[scroll_df["search_term"] == term].copy()
                    if term_scroll_df.empty:
                        continue

                    term_scroll_df = _normalize_jobs_df(term_scroll_df)
                    term_scroll_df = term_scroll_df.drop_duplicates(subset=["id"], keep="first")

                    merged_term_df = pd.concat(
                        [term_frames.get(term, empty_df.copy()), term_scroll_df],
                        ignore_index=True,
                    )
                    merged_term_df = merged_term_df.drop_duplicates(subset=["id"], keep="first")
                    term_frames[term] = merged_term_df
                    term_stats[term]["scroll_unique"] = len(term_scroll_df)

        merged = []
        for term in terms:
            term_stats[term]["merged_unique"] = len(term_frames[term])
            if not term_frames[term].empty:
                merged.append(term_frames[term])

        if not merged:
            print("No jobs returned by scanners for all terms.")
            return {"scanned_raw": 0, "scanned_unique": 0}

        jobs_df = pd.concat(merged, ignore_index=True)
        raw_count = len(jobs_df)
        jobs_df = jobs_df.drop_duplicates(subset=["id"], keep="first")
        unique_count = len(jobs_df)
        duplicate_count = raw_count - unique_count

        per_term_stats = [term_stats[term] for term in terms]
        print(f"Per-term stats: {per_term_stats}")
        print(
            "Merged id stats: "
            f"total={raw_count}, unique={unique_count}, duplicates={duplicate_count}"
        )

        database.save_jobs(jobs_df)

        return {
            "terms": terms,
            "scan_source": scan_source,
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
