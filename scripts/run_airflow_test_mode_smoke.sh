#!/usr/bin/env bash
set -euo pipefail

if [[ "${LINKEDIN_TEST_MODE:-}" != "true" ]]; then
  echo "ERROR: LINKEDIN_TEST_MODE=true is required for this real Airflow smoke run." >&2
  exit 2
fi

RUN_SUFFIX="smoke_$(date -u +%Y%m%dT%H%M%SZ)"

echo "Running real Airflow test-mode smoke."
echo "LINKEDIN_TEST_MAX_JOBS=${LINKEDIN_TEST_MAX_JOBS:-default}"
echo "LINKEDIN_TEST_MAX_SCAN_ROWS=${LINKEDIN_TEST_MAX_SCAN_ROWS:-default}"
echo "LINKEDIN_TEST_MAX_JD_JOBS=${LINKEDIN_TEST_MAX_JD_JOBS:-default}"
echo "LINKEDIN_TEST_MAX_FIT_JOBS=${LINKEDIN_TEST_MAX_FIT_JOBS:-default}"
echo "LINKEDIN_TEST_MAX_NOTIFY_JOBS=${LINKEDIN_TEST_MAX_NOTIFY_JOBS:-3}"

if command -v astro >/dev/null 2>&1; then
  AIRFLOW_CMD=(astro dev run)
else
  AIRFLOW_CMD=(airflow)
fi

echo "Triggering linkedin_notifier (${RUN_SUFFIX}_scan)..."
"${AIRFLOW_CMD[@]}" dags trigger linkedin_notifier --run-id "${RUN_SUFFIX}_scan" --conf "{\"LINKEDIN_TEST_MODE\": true, \"LINKEDIN_TEST_MAX_JOBS\": ${LINKEDIN_TEST_MAX_JOBS:-3}, \"LINKEDIN_TEST_MAX_SCAN_ROWS\": ${LINKEDIN_TEST_MAX_SCAN_ROWS:-5}, \"LINKEDIN_TEST_MAX_JD_JOBS\": ${LINKEDIN_TEST_MAX_JD_JOBS:-5}, \"LINKEDIN_TEST_MAX_FIT_JOBS\": ${LINKEDIN_TEST_MAX_FIT_JOBS:-5}, \"LINKEDIN_TEST_MAX_NOTIFY_JOBS\": ${LINKEDIN_TEST_MAX_NOTIFY_JOBS:-3}}"

cat <<MSG
Smoke scan trigger submitted.
Run id:
  - ${RUN_SUFFIX}_scan

Do not trigger linkedin_fitting_notifier manually for this smoke: linkedin_notifier triggers it after scan/JD/fitting enqueue. Triggering both at once can cause database DDL/queue deadlocks in local Airflow.
Expected: linkedin_notifier completes successfully, then an automatically-triggered linkedin_fitting_notifier run completes and creates a test profile notification_run.
MSG
