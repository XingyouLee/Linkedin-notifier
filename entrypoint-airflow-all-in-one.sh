#!/usr/bin/env sh
set -eu

export AIRFLOW_HOME="${AIRFLOW_HOME:-/usr/local/airflow}"

airflow db migrate

airflow scheduler &
airflow dag-processor &
airflow triggerer &

exec airflow api-server
