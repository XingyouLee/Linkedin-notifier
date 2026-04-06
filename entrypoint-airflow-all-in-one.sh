#!/usr/bin/env sh
set -eu

export AIRFLOW_HOME="${AIRFLOW_HOME:-/usr/local/airflow}"

if [ -n "${AIRFLOW_ADMIN_PASSWORD:-}" ]; then
  password_file="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-/usr/local/airflow/simple_auth_manager_passwords.json}"
  usernames="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS:-admin:admin}"
  primary_username="$(printf '%s' "$usernames" | cut -d',' -f1 | cut -d':' -f1)"
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
else
  primary_username="$(printf '%s' "${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS:-admin:admin}" | cut -d',' -f1 | cut -d':' -f1)"
fi

export AIRFLOW_ADMIN_USERNAME="$primary_username"

airflow db migrate

airflow scheduler &
airflow dag-processor &
airflow triggerer &

exec airflow api-server
