#!/usr/bin/env sh
set -eu

export AIRFLOW_HOME="${AIRFLOW_HOME:-/usr/local/airflow}"
exec airflow api-server
