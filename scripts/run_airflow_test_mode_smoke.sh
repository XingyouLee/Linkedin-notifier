#!/usr/bin/env bash
set -euo pipefail

if [[ "${LINKEDIN_TEST_MODE:-}" != "true" ]]; then
  echo "ERROR: LINKEDIN_TEST_MODE=true is required for this real Airflow smoke run." >&2
  exit 2
fi

# This script triggers real Airflow DAGs against the configured Airflow
# metadata database. It must only run inside GitHub Actions.
if [[ "${GITHUB_ACTIONS:-}" != "true" ]]; then
  echo "ERROR: Real Airflow smoke runs are only permitted in GitHub Actions." >&2
  echo "ERROR: Use the smoke-test workflow_dispatch workflow in GitHub Actions." >&2
  exit 4
fi

if [[ -z "${JOBS_DB_URL:-}" ]]; then
  echo "ERROR: JOBS_DB_URL must be configured for the smoke workflow." >&2
  exit 5
fi

if [[ -z "${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-}" ]]; then
  echo "ERROR: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN must point to the deployed Airflow metadata DB." >&2
  exit 5
fi

RUN_SUFFIX="smoke_$(date -u +%Y%m%dT%H%M%SZ)"
SCAN_RUN_ID="${RUN_SUFFIX}_scan"

echo "=== Real Airflow test-mode smoke run ==="
echo "Scan run id: ${SCAN_RUN_ID}"
echo "LINKEDIN_TEST_MAX_JOBS=${LINKEDIN_TEST_MAX_JOBS:-default}"
echo "LINKEDIN_TEST_MAX_SCAN_ROWS=${LINKEDIN_TEST_MAX_SCAN_ROWS:-default}"
echo "LINKEDIN_TEST_MAX_JD_JOBS=${LINKEDIN_TEST_MAX_JD_JOBS:-default}"
echo "LINKEDIN_TEST_MAX_FIT_JOBS=${LINKEDIN_TEST_MAX_FIT_JOBS:-default}"
echo "LINKEDIN_TEST_MAX_NOTIFY_JOBS=${LINKEDIN_TEST_MAX_NOTIFY_JOBS:-3}"

if ! command -v airflow >/dev/null 2>&1; then
  echo "ERROR: airflow CLI is required. Run this script inside the project Airflow Docker image." >&2
  exit 5
fi
AIRFLOW_CMD=(airflow)

# ---------- Discord config diagnosis ----------
discord_warnings=()
if [[ -z "${DISCORD_BOT_TOKEN:-}" ]]; then
  discord_warnings+=("DISCORD_BOT_TOKEN is not set — notification delivery will fail")
fi
if [[ -n "${DISCORD_BOT_TOKEN:-}" ]]; then
  echo "Discord: DISCORD_BOT_TOKEN is configured"
else
  echo "Discord: DISCORD_BOT_TOKEN missing"
fi

if [[ ${#discord_warnings[@]} -gt 0 ]]; then
  echo "--- Discord diagnostics ---"
  for warning in "${discord_warnings[@]}"; do
    echo "  WARNING: ${warning}"
  done
  if [[ "${REQUIRE_DISCORD_VERIFICATION:-}" == "true" ]]; then
    echo "ERROR: Discord verification is required but config is incomplete." >&2
    exit 3
  fi
  echo "Discord verification is not required for this run; continuing."
fi
echo ""

_active_runs_for_dag() {
  local dag_id="$1"
  "${AIRFLOW_CMD[@]}" dags list-runs "${dag_id}" --no-backfill -o json 2>/dev/null | python3 -c "
import json, sys
raw = sys.stdin.read()
start = raw.find('[{')
if start == -1:
    start = raw.find('[')
data = json.loads(raw[start:]) if start != -1 else []
active_states = {'queued', 'running', 'scheduled'}
for run in data:
    state = str(run.get('state') or '').lower()
    if state in active_states:
        print(f\"{run.get('dag_id') or '${dag_id}'} {run.get('run_id')} {state}\")
"
}

_assert_no_active_runs() {
  local active
  active="$(_active_runs_for_dag "linkedin_notifier"; _active_runs_for_dag "linkedin_fitting_notifier")"
  if [[ -n "${active}" ]]; then
    echo "ERROR: Refusing to start smoke while Airflow already has active DAG runs:" >&2
    echo "${active}" >&2
    echo "ERROR: Resolve existing runs first; smoke must not queue behind or overlap production/scheduled work." >&2
    exit 6
  fi
}

_assert_no_active_runs

# Build dag_run conf for the test-mode DAG run.
TRIGGER_CONF=$(python3 - <<'PYEOF'
import json, os
conf = {
    "LINKEDIN_TEST_MODE": True,
    "LINKEDIN_TEST_MAX_JOBS":       int(os.environ.get("LINKEDIN_TEST_MAX_JOBS", 3)),
    "LINKEDIN_TEST_MAX_SCAN_ROWS":  int(os.environ.get("LINKEDIN_TEST_MAX_SCAN_ROWS", 5)),
    "LINKEDIN_TEST_MAX_JD_JOBS":    int(os.environ.get("LINKEDIN_TEST_MAX_JD_JOBS", 5)),
    "LINKEDIN_TEST_MAX_FIT_JOBS":   int(os.environ.get("LINKEDIN_TEST_MAX_FIT_JOBS", 5)),
    "LINKEDIN_TEST_MAX_NOTIFY_JOBS": int(os.environ.get("LINKEDIN_TEST_MAX_NOTIFY_JOBS", 3)),
}
print(json.dumps(conf))
PYEOF
)

# ---------- Trigger scan DAG ----------
echo "Triggering linkedin_notifier ..."
"${AIRFLOW_CMD[@]}" dags trigger linkedin_notifier --run-id "${SCAN_RUN_ID}" --conf "${TRIGGER_CONF}"

# ---------- Wait helpers ----------
MAX_WAIT_SECONDS="${SMOKE_MAX_WAIT_SECONDS:-600}"
POLL_INTERVAL="${SMOKE_POLL_INTERVAL:-10}"

_poll_dag_run_state() {
  local dag_id="$1"
  local run_id="$2"
  local state
  state=$("${AIRFLOW_CMD[@]}" dags list-runs "${dag_id}" --no-backfill -o json 2>/dev/null | python3 -c "
import json, sys
raw = sys.stdin.read()
start = raw.find('[{')
if start == -1:
    start = raw.find('[')
data = json.loads(raw[start:]) if start != -1 else []
for run in data:
    if run.get('run_id') == '${run_id}':
        print(run.get('state', ''))
        break
" 2>/dev/null || echo "")
  echo "${state}"
}

_wait_for_run() {
  local dag_id="$1"
  local run_id="$2"
  local start_time
  start_time=$(date +%s)

  while true; do
    local elapsed
    elapsed=$(($(date +%s) - start_time))
    if (( elapsed > MAX_WAIT_SECONDS )); then
      echo "TIMEOUT"
      return
    fi

    local state
    state=$(_poll_dag_run_state "${dag_id}" "${run_id}")
    case "${state}" in
      success)
        echo "SUCCESS"
        return
        ;;
      failed)
        echo "FAILED"
        return
        ;;
      running|queued|scheduled)
        if (( elapsed % 30 == 0 )) || (( elapsed < 30 )); then
          echo "  [${elapsed}s] ${dag_id} run ${run_id} state=${state}, waiting..." >&2
        fi
        sleep "${POLL_INTERVAL}"
        ;;
      "")
        # run not found yet (may still be starting)
        sleep "${POLL_INTERVAL}"
        ;;
      *)
        echo "  [${elapsed}s] ${dag_id} run ${run_id} state=${state}, waiting..." >&2
        sleep "${POLL_INTERVAL}"
        ;;
    esac
  done
}

# ---------- Wait for scan DAG ----------
echo ""
echo "Waiting for linkedin_notifier run ${SCAN_RUN_ID} (timeout: ${MAX_WAIT_SECONDS}s)..."
SCAN_RESULT=$(_wait_for_run "linkedin_notifier" "${SCAN_RUN_ID}")

# Find the triggered fitting run
FITTING_RUN_ID="fitting__${SCAN_RUN_ID}"

FITTING_RESULT="NO_TRIGGERED_RUN"
if [[ "${SCAN_RESULT}" == "SUCCESS" ]]; then
  echo ""
  echo "Waiting for linkedin_fitting_notifier run ${FITTING_RUN_ID} (timeout: ${MAX_WAIT_SECONDS}s)..."
  FITTING_RESULT=$(_wait_for_run "linkedin_fitting_notifier" "${FITTING_RUN_ID}")
else
  FITTING_RESULT="SKIPPED_SCAN_NOT_SUCCESS"
fi

# ---------- Summary ----------
echo ""
echo "========================================="
echo "  Smoke run summary"
echo "========================================="
echo "  Scan run:         ${SCAN_RUN_ID}  => ${SCAN_RESULT}"
echo "  Fitting run:      ${FITTING_RUN_ID:-none}  => ${FITTING_RESULT}"
echo "  Max wait:         ${MAX_WAIT_SECONDS}s"
echo "  POLL interval:    ${POLL_INTERVAL}s"
echo ""

PASS=true
FAILURES=()

if [[ "${SCAN_RESULT}" != "SUCCESS" ]]; then
  PASS=false
  FAILURES+=("Scan DAG: expected SUCCESS, got ${SCAN_RESULT}")
fi

if [[ "${FITTING_RESULT}" != "SUCCESS" ]]; then
  PASS=false
  FAILURES+=("Fitting DAG: expected SUCCESS, got ${FITTING_RESULT}")
fi

if [[ ${#discord_warnings[@]} -gt 0 ]]; then
  for warning in "${discord_warnings[@]}"; do
    FAILURES+=("Discord: ${warning}")
  done
  # Discord warnings alone do not fail the smoke (config may be intentional)
fi

if ${PASS}; then
  echo "RESULT: PASS"
  echo "  Both DAGs completed successfully."
  echo "  Check the notification run table and Discord for TEST-marked messages."
  exit 0
else
  echo "RESULT: FAIL"
  for failure in "${FAILURES[@]}"; do
    echo "  - ${failure}"
  done
  exit 1
fi
