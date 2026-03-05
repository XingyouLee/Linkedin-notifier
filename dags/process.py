from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import os
import pandas as pd
import random
import re
import time
from html import unescape

import requests
from dags import database
from dags.runtime_utils import df_to_xcom_records, load_env, spark_to_xcom_records
from dags.spark_runtime import get_spark_session


load_env()

SCAN_API_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
SCAN_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

SCAN_TAG_RE = re.compile(r"<[^>]+>")
SCAN_WHITESPACE_RE = re.compile(r"\s+")
SCAN_JOB_URL_RE = re.compile(r'href="([^"]*?/jobs/view/[^"]+)"')
SCAN_TITLE_RE = re.compile(r"<h3[^>]*>(.*?)</h3>", re.S)
SCAN_COMPANY_RE = re.compile(r"<h4[^>]*>(.*?)</h4>", re.S)
SCAN_JOB_ID_RE = re.compile(r"-(\d+)\?")
SCAN_ITEM_RE = re.compile(r"<li>(.*?)</li>", re.S)


def _resolve_scan_terms(search_term: str, search_terms) -> list[str]:
    env_terms = os.getenv("SCAN_SEARCH_TERMS", "")
    terms_input = search_terms if search_terms is not None else env_terms
    if isinstance(terms_input, str):
        terms = [t.strip() for t in terms_input.split(",") if t.strip()]
    else:
        terms = [str(t).strip() for t in (terms_input or []) if str(t).strip()]
    return terms or [search_term]


def _build_scan_config(results_wanted: int, hours_old: int) -> dict:
    return {
        "request_page_size": int(os.getenv("SCAN_REQUEST_PAGE_SIZE", "10")),
        "http_max_retries": max(0, int(os.getenv("SCAN_HTTP_MAX_RETRIES", "6"))),
        "http_base_delay_sec": float(os.getenv("SCAN_HTTP_BASE_DELAY_SEC", "2.0")),
        "http_max_delay_sec": float(os.getenv("SCAN_HTTP_MAX_DELAY_SEC", "60.0")),
        "http_jitter_sec": float(os.getenv("SCAN_HTTP_JITTER_SEC", "1.5")),
        "between_req_min_sec": float(os.getenv("SCAN_BETWEEN_REQUESTS_MIN_SEC", "0.8")),
        "between_req_max_sec": float(os.getenv("SCAN_BETWEEN_REQUESTS_MAX_SEC", "1.8")),
        "between_terms_delay_sec": float(os.getenv("SCAN_BETWEEN_TERMS_DELAY_SEC", "8.0")),
        "request_timeout_sec": int(os.getenv("SCAN_REQUEST_TIMEOUT_SEC", "45")),
        "results_per_term": int(os.getenv("SCAN_RESULTS_PER_TERM", str(results_wanted))),
        "hours_old": int(os.getenv("SCAN_HOURS_OLD", str(hours_old))),
        "distance": int(os.getenv("SCAN_DISTANCE", "25")),
    }


def _clean_scan_text(raw: str) -> str:
    if not raw:
        return ""
    text = unescape(raw)
    text = SCAN_TAG_RE.sub(" ", text)
    text = SCAN_WHITESPACE_RE.sub(" ", text)
    return text.strip()


def _extract_scan_job_id(job_url: str) -> str:
    if not job_url:
        return ""
    match = SCAN_JOB_ID_RE.search(job_url)
    return match.group(1) if match else ""


def _build_scan_params(
    term: str,
    start: int,
    *,
    distance: int,
    location: str,
    geo_id: str,
    hours_old: int,
) -> dict:
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


def _parse_scan_items(html_text: str) -> list[dict]:
    rows = []
    for item in SCAN_ITEM_RE.findall(html_text):
        url_match = SCAN_JOB_URL_RE.search(item)
        title_match = SCAN_TITLE_RE.search(item)
        company_match = SCAN_COMPANY_RE.search(item)
        if not url_match or not title_match or not company_match:
            continue

        job_url = unescape(url_match.group(1))
        job_id = _extract_scan_job_id(job_url)
        if not job_id:
            continue

        rows.append(
            {
                "id": job_id,
                "title": _clean_scan_text(title_match.group(1)),
                "company": _clean_scan_text(company_match.group(1)),
                "job_url": job_url,
                "site": "linkedin",
            }
        )
    return rows


def _scan_fetch_page(
    session: requests.Session,
    term: str,
    start: int,
    *,
    location: str,
    geo_id: str,
    scan_config: dict,
) -> list[dict]:
    for attempt in range(scan_config["http_max_retries"] + 1):
        try:
            response = session.get(
                SCAN_API_URL,
                params=_build_scan_params(
                    term=term,
                    start=start,
                    distance=scan_config["distance"],
                    location=location,
                    geo_id=geo_id,
                    hours_old=scan_config["hours_old"],
                ),
                timeout=scan_config["request_timeout_sec"],
                headers={"User-Agent": SCAN_USER_AGENT},
            )
        except requests.RequestException as error:
            if attempt >= scan_config["http_max_retries"]:
                print(
                    f"term={term}, start={start}, network_error={error}, "
                    f"attempt={attempt + 1}/{scan_config['http_max_retries'] + 1} -> stop page"
                )
                return []

            backoff = min(
                scan_config["http_max_delay_sec"],
                scan_config["http_base_delay_sec"] * (2 ** attempt),
            )
            sleep_sec = backoff + random.uniform(0, scan_config["http_jitter_sec"])
            print(
                f"term={term}, start={start}, network_error={error}, "
                f"retry_in={sleep_sec:.1f}s, attempt={attempt + 1}/{scan_config['http_max_retries'] + 1}"
            )
            time.sleep(sleep_sec)
            continue

        status_code = response.status_code
        if status_code == 429 or status_code >= 500:
            if attempt >= scan_config["http_max_retries"]:
                print(
                    f"term={term}, start={start}, status={status_code}, "
                    f"attempt={attempt + 1}/{scan_config['http_max_retries'] + 1} -> stop page"
                )
                return []

            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                sleep_sec = float(retry_after)
            else:
                backoff = min(
                    scan_config["http_max_delay_sec"],
                    scan_config["http_base_delay_sec"] * (2 ** attempt),
                )
                sleep_sec = backoff + random.uniform(0, scan_config["http_jitter_sec"])

            print(
                f"term={term}, start={start}, status={status_code}, "
                f"retry_in={sleep_sec:.1f}s, attempt={attempt + 1}/{scan_config['http_max_retries'] + 1}"
            )
            time.sleep(max(0.0, sleep_sec))
            continue

        if status_code >= 400:
            print(f"term={term}, start={start}, status={status_code} -> stop page")
            return []

        return _parse_scan_items(response.text)

    return []


def _scan_one_term(
    session: requests.Session,
    term: str,
    target_count: int,
    *,
    location: str,
    geo_id: str,
    scan_config: dict,
) -> list[dict]:
    import math

    expected_pages = math.ceil(target_count / 25)
    term_rows = []
    seen_ids = set()
    start = 0
    request_count = 0

    while len(term_rows) < target_count:
        page_rows = _scan_fetch_page(
            session=session,
            term=term,
            start=start,
            location=location,
            geo_id=geo_id,
            scan_config=scan_config,
        )
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

        start += scan_config["request_page_size"]
        if scan_config["between_req_max_sec"] > 0:
            delay_min = min(scan_config["between_req_min_sec"], scan_config["between_req_max_sec"])
            delay_max = max(scan_config["between_req_min_sec"], scan_config["between_req_max_sec"])
            time.sleep(random.uniform(delay_min, delay_max))

    return term_rows


def _collect_scan_rows(
    terms: list[str],
    *,
    location: str,
    geo_id: str,
    scan_config: dict,
) -> list[dict]:
    all_rows = []
    with requests.Session() as session:
        for idx, term in enumerate(terms):
            all_rows.extend(
                _scan_one_term(
                    session=session,
                    term=term,
                    target_count=max(1, scan_config["results_per_term"]),
                    location=location,
                    geo_id=geo_id,
                    scan_config=scan_config,
                )
            )
            if idx < len(terms) - 1 and scan_config["between_terms_delay_sec"] > 0:
                print(
                    f"term={term} done, "
                    f"cooldown_before_next_term={scan_config['between_terms_delay_sec']:.1f}s"
                )
                time.sleep(scan_config["between_terms_delay_sec"])
    return all_rows


def _normalize_and_save_scan_rows(all_rows: list[dict]) -> dict:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    spark = None
    try:
        spark = get_spark_session("linkedin_notifier_scan")
        scan_schema = T.StructType(
            [
                T.StructField("id", T.StringType(), True),
                T.StructField("title", T.StringType(), True),
                T.StructField("company", T.StringType(), True),
                T.StructField("job_url", T.StringType(), True),
                T.StructField("site", T.StringType(), True),
                T.StructField("search_term", T.StringType(), True),
            ]
        )
        jobs_sdf = spark.createDataFrame(all_rows, schema=scan_schema)

        raw_count = jobs_sdf.count()
        normalized_sdf = (
            jobs_sdf.select(
                F.trim(F.coalesce(F.col("id").cast("string"), F.lit(""))).alias("id"),
                F.nullif(F.trim(F.col("title").cast("string")), F.lit("")).alias("title"),
                F.nullif(F.trim(F.col("company").cast("string")), F.lit("")).alias("company"),
                F.nullif(F.trim(F.col("job_url").cast("string")), F.lit("")).alias("job_url"),
                F.when(
                    F.trim(F.coalesce(F.col("site").cast("string"), F.lit(""))) == "",
                    F.lit("linkedin"),
                ).otherwise(F.trim(F.col("site").cast("string"))).alias("site"),
                F.nullif(F.trim(F.col("search_term").cast("string")), F.lit("")).alias("search_term"),
            )
            .filter(F.col("id").rlike(r"^[0-9]+$"))
        )
        normalized_count = normalized_sdf.count()
        invalid_id_count = raw_count - normalized_count

        if normalized_count == 0:
            print("No valid job ids found after normalization.")
            return {
                "scanned_raw": raw_count,
                "scanned_unique": 0,
                "scanned_duplicates": 0,
                "scanned_invalid_ids": invalid_id_count,
            }

        per_term_rows = (
            normalized_sdf
            .select("search_term", "id")
            .dropDuplicates(["search_term", "id"])
            .groupBy("search_term")
            .agg(F.count("id").alias("job_count"))
            .orderBy(F.desc("job_count"))
            .collect()
        )
        per_term_stats = {row["search_term"]: row["job_count"] for row in per_term_rows}

        quality_score = (
            F.when(F.col("job_url").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("title").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("company").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("search_term").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        )
        dedupe_window = Window.partitionBy("id").orderBy(
            F.desc("quality_score"),
            F.desc(F.length(F.coalesce(F.col("title"), F.lit("")))),
            F.desc(F.length(F.coalesce(F.col("company"), F.lit("")))),
        )
        deduped_sdf = (
            normalized_sdf
            .withColumn("quality_score", quality_score)
            .withColumn("row_num", F.row_number().over(dedupe_window))
            .filter(F.col("row_num") == 1)
            .drop("quality_score", "row_num", "search_term")
        )
        unique_count = deduped_sdf.count()
        duplicate_count = normalized_count - unique_count

        print(f"Per-term unique stats: {per_term_stats}")
        print(
            "Merged id stats: "
            f"total={raw_count}, valid={normalized_count}, unique={unique_count}, "
            f"duplicates={duplicate_count}, invalid_ids={invalid_id_count}"
        )

        jobs_to_save_records = spark_to_xcom_records(
            deduped_sdf.select("id", "site", "job_url", "title", "company")
        )
        jobs_to_save_df = pd.DataFrame(jobs_to_save_records)
        database.save_jobs(jobs_to_save_df)

        return {
            "scanned_raw": raw_count,
            "scanned_unique": unique_count,
            "scanned_duplicates": duplicate_count,
            "scanned_invalid_ids": invalid_id_count,
        }
    finally:
        if spark is not None:
            spark.stop()


@dag(
    start_date=datetime(2023, 1, 1),
    schedule="0 */12 * * *",
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
        terms = _resolve_scan_terms(search_term, search_terms)
        scan_config = _build_scan_config(results_wanted=results_wanted, hours_old=hours_old)

        print(
            f"Scanning LinkedIn jobs: source=scripts_guest_api, terms={terms}, location={location}, "
            f"hours_old={scan_config['hours_old']}, results_per_term={scan_config['results_per_term']}, "
            f"distance={scan_config['distance']}"
        )

        all_rows = _collect_scan_rows(
            terms=terms,
            location=location,
            geo_id=geo_id,
            scan_config=scan_config,
        )

        if not all_rows:
            print("No jobs returned by scripts scraper.")
            return {"scanned_raw": 0, "scanned_unique": 0}

        scan_stats = _normalize_and_save_scan_rows(all_rows)

        return {
            "terms": terms,
            "scan_source": "scripts_guest_api",
            **scan_stats,
        }

    @task
    def filter_jobs():
        df = database.get_latest_batch_jobs()
        blocked_companies = ["Elevation Group", "Capgemini", "Jobster", "Sogeti", "CGI Nederland", "Mercor"]
        if df is None or df.empty:
            print("No jobs in latest batch. Skip downstream pipeline.")
            return []

        if "company" not in df.columns or "title" not in df.columns:
            print("Missing company/title columns in latest batch. Skip downstream pipeline.")
            return []

        filter_columns = ["id", "site", "job_url", "title", "company"]
        for col in filter_columns:
            if col not in df.columns:
                df[col] = None

        from pyspark.sql import functions as F
        from pyspark.sql import types as T

        spark = None
        try:
            spark = get_spark_session("linkedin_notifier_filter")
            records = df_to_xcom_records(df[filter_columns])
            if not records:
                print("No records available for filtering.")
                return []

            filter_schema = T.StructType(
                [
                    T.StructField("id", T.StringType(), True),
                    T.StructField("site", T.StringType(), True),
                    T.StructField("job_url", T.StringType(), True),
                    T.StructField("title", T.StringType(), True),
                    T.StructField("company", T.StringType(), True),
                ]
            )
            jobs_sdf = spark.createDataFrame(records, schema=filter_schema)
            company_col = F.coalesce(F.col("company").cast("string"), F.lit(""))
            title_col = F.lower(F.coalesce(F.col("title").cast("string"), F.lit("")))
            filtered_sdf = (
                jobs_sdf
                .filter(~company_col.isin(blocked_companies))
                .filter(~title_col.contains("senior"))
                .filter(~title_col.contains("medior"))
            )
            filtered_records = spark_to_xcom_records(filtered_sdf)
            print("Filtered records:", len(filtered_records))
            return filtered_records
        finally:
            if spark is not None:
                spark.stop()

    @task
    def normalize_job_records(job_records):
        if not job_records:
            return []
        return [
            {
                "id": record.get("id"),
                "job_url": record.get("job_url"),
            }
            for record in job_records
            if record.get("id")
        ]

    @task.branch
    def branch_after_filter(job_records):
        count = len(job_records or [])
        if count == 0:
            print("No new jobs after filter_jobs. End DAG run.")
            return "finish_no_new_jobs"
        print(f"Found {count} new jobs after filtering. Continue pipeline.")
        return "enqueue_jd_requests"

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
        from jd_playwright_worker import run_once

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
        return df_to_xcom_records(jobs_df)

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

    filtered_job_records = filter_jobs()
    scan_meta >> filtered_job_records
    normalized_job_records = normalize_job_records(filtered_job_records)
    branch_next = branch_after_filter(normalized_job_records)
    finish_no_new_jobs = EmptyOperator(task_id="finish_no_new_jobs")

    branch_next >> finish_no_new_jobs

    queued_job_ids = enqueue_jd_requests(normalized_job_records)
    branch_next >> queued_job_ids
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
