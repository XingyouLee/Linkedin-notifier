#!/usr/bin/env sh
set -eu

export AIRFLOW_HOME="${AIRFLOW_HOME:-/usr/local/airflow}"
AIRFLOW_API_HOST="${AIRFLOW_API_HOST:-0.0.0.0}"
AIRFLOW_API_PORT="${AIRFLOW_API_PORT:-${PORT:-8080}}"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="${AIRFLOW__CORE__EXECUTION_API_SERVER_URL:-http://127.0.0.1:${AIRFLOW_API_PORT}/execution/}"

usernames="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS:-admin:admin}"
primary_username="$(printf '%s' "$usernames" | cut -d',' -f1 | cut -d':' -f1)"
export AIRFLOW_ADMIN_USERNAME="$primary_username"

if [ -n "${AIRFLOW_ADMIN_PASSWORD:-}" ]; then
  password_file="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-/usr/local/airflow/simple_auth_manager_passwords.json}"
  export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="$password_file"
  python - <<'PY'
import json
import os
from pathlib import Path

password_file = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
username = os.environ["AIRFLOW_ADMIN_USERNAME"]
password = os.environ["AIRFLOW_ADMIN_PASSWORD"]
password_file.write_text(json.dumps({username: password}) + "\n", encoding="utf-8")
PY
fi

echo "Waiting for Airflow metadata database..."
airflow db check \
  --retry "${AIRFLOW_DB_WAIT_RETRIES:-30}" \
  --retry-delay "${AIRFLOW_DB_WAIT_RETRY_DELAY_SEC:-2}"

airflow db migrate

airflow scheduler &
airflow dag-processor &
airflow triggerer &

exec airflow api-server \
  --host "$AIRFLOW_API_HOST" \
  --port "$AIRFLOW_API_PORT" \
  --proxy-headers
