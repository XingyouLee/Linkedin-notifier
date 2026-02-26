from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import os
import pandas as pd
from dotenv import load_dotenv

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
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=['linkedin_notifier'],
)
def linkedin_notifier():

    @task
    def scan_and_save_jobs(
        search_term="Data Engineer",
        location="Netherlands",
        geo_id="102890719",
        hours_old=168,
        results_wanted=10,
    ):
        """Scrape LinkedIn jobs via JobSpy and normalize job ids for DB dedupe."""
        from jobspy import scrape_jobs

        print(
            f"Scanning LinkedIn with JobSpy: term={search_term}, location={location}, "
            f"hours_old={hours_old}, results_wanted={results_wanted}"
        )
        if geo_id:
            print(f"geo_id={geo_id} is ignored by JobSpy LinkedIn scraping.")

        jobs_df = scrape_jobs(
            site_name=["linkedin"],
            search_term=search_term,
            location=location,
            results_wanted=results_wanted,
            hours_old=hours_old,
            country_indeed="Netherlands",
        )

        if jobs_df is None or jobs_df.empty:
            print("JobSpy returned no jobs.")
            return {"scanned_raw": 0, "scanned_unique": 0}

        jobs_df = jobs_df.copy()
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
        raw_count = len(jobs_df)
        jobs_df = jobs_df.drop_duplicates(subset=["id"], keep="first")
        unique_count = len(jobs_df)
        duplicate_count = raw_count - unique_count
        if duplicate_count > 0:
            print(
                "JobSpy returned duplicate job ids: "
                f"total={raw_count}, unique={unique_count}, duplicates={duplicate_count}"
            )
        else:
            print(f"JobSpy id stats: total={raw_count}, unique={unique_count}, duplicates=0")

        jobs_df = jobs_df[["id", "site", "job_url", "title", "company"]]
        database.save_jobs(jobs_df)

        return {
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
    fitting_enqueue_meta = enqueue_fitting_tasks(jobs_with_jd)

    trigger_fitting_notifier = TriggerDagRunOperator(
        task_id="trigger_fitting_notifier",
        trigger_dag_id="linkedin_fitting_notifier",
        conf={"source_dag_run_id": "{{ dag_run.run_id }}"},
        wait_for_completion=False,
    )
    fitting_enqueue_meta >> trigger_fitting_notifier


linkedin_notifier()
