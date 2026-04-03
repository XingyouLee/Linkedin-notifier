#!/usr/bin/env bash

set -euo pipefail

readonly SIMPLE_AUTH_MANAGER="airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"

AIRFLOW_SERVICE_ROLE="${AIRFLOW_SERVICE_ROLE:-all-in-one}"
AIRFLOW_HOME="${AIRFLOW_HOME:-/usr/local/airflow}"
PORT="${PORT:-8080}"

export AIRFLOW_HOME
export AIRFLOW__CORE__AUTH_MANAGER="${AIRFLOW__CORE__AUTH_MANAGER:-$SIMPLE_AUTH_MANAGER}"
export AIRFLOW__CORE__LOAD_EXAMPLES="${AIRFLOW__CORE__LOAD_EXAMPLES:-False}"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW__CORE__DAGS_FOLDER:-${AIRFLOW_HOME}/dags}"
export AIRFLOW__CORE__PLUGINS_FOLDER="${AIRFLOW__CORE__PLUGINS_FOLDER:-${AIRFLOW_HOME}/plugins}"

require_env() {
    local name="$1"
    if [ -z "${!name:-}" ]; then
        echo "Missing required environment variable: ${name}" >&2
        exit 1
    fi
}

print_command() {
    printf '%s\n' "$*"
}

wait_for_any_exit() {
    while true; do
        for pid in "$@"; do
            if ! kill -0 "$pid" 2>/dev/null; then
                wait "$pid"
                return $?
            fi
        done
        sleep 2
    done
}

terminate_children() {
    for pid in "$@"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done

    for pid in "$@"; do
        wait "$pid" 2>/dev/null || true
    done
}

db_migrate_cmd=(airflow db migrate)
api_server_cmd=(airflow api-server -H 0.0.0.0 -p "${PORT}" --proxy-headers)
scheduler_cmd=(airflow scheduler)
dag_processor_cmd=(airflow dag-processor)
triggerer_cmd=(airflow triggerer)

case "${AIRFLOW_SERVICE_ROLE}" in
    all-in-one|api-server|scheduler|dag-processor|triggerer)
        ;;
    *)
        echo "Unsupported AIRFLOW_SERVICE_ROLE: ${AIRFLOW_SERVICE_ROLE}" >&2
        exit 1
        ;;
esac

simple_users="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS:-}"
if [ "${AIRFLOW__CORE__AUTH_MANAGER}" = "${SIMPLE_AUTH_MANAGER}" ] && \
   { [ "${AIRFLOW_SERVICE_ROLE}" = "all-in-one" ] || [ "${AIRFLOW_SERVICE_ROLE}" = "api-server" ]; } && \
   [ -z "${simple_users}" ]; then
    echo "Missing required environment variable: AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS (format: username:role, for example levi:admin)" >&2
    exit 1
fi

require_env AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
require_env JOBS_DB_URL

if [ "${AIRFLOW_START_DRY_RUN:-0}" = "1" ]; then
    echo "AIRFLOW_SERVICE_ROLE=${AIRFLOW_SERVICE_ROLE}"
    echo "AIRFLOW_HOME=${AIRFLOW_HOME}"
    echo "AIRFLOW__CORE__AUTH_MANAGER=${AIRFLOW__CORE__AUTH_MANAGER}"
    print_command "${db_migrate_cmd[@]}"
    case "${AIRFLOW_SERVICE_ROLE}" in
        all-in-one)
            print_command "${scheduler_cmd[@]}"
            print_command "${dag_processor_cmd[@]}"
            print_command "${api_server_cmd[@]}"
            ;;
        api-server)
            print_command "${api_server_cmd[@]}"
            ;;
        scheduler)
            print_command "${scheduler_cmd[@]}"
            ;;
        dag-processor)
            print_command "${dag_processor_cmd[@]}"
            ;;
        triggerer)
            print_command "${triggerer_cmd[@]}"
            ;;
    esac
    exit 0
fi

mkdir -p "${AIRFLOW_HOME}/logs" "${AIRFLOW_HOME}/plugins"

"${db_migrate_cmd[@]}"

case "${AIRFLOW_SERVICE_ROLE}" in
    api-server)
        exec "${api_server_cmd[@]}"
        ;;
    scheduler)
        exec "${scheduler_cmd[@]}"
        ;;
    dag-processor)
        exec "${dag_processor_cmd[@]}"
        ;;
    triggerer)
        exec "${triggerer_cmd[@]}"
        ;;
    all-in-one)
        "${scheduler_cmd[@]}" &
        scheduler_pid=$!
        "${dag_processor_cmd[@]}" &
        dag_processor_pid=$!
        "${api_server_cmd[@]}" &
        api_server_pid=$!

        cleanup() {
            terminate_children "${scheduler_pid}" "${dag_processor_pid}" "${api_server_pid}"
        }

        trap cleanup INT TERM EXIT

        set +e
        wait_for_any_exit "${scheduler_pid}" "${dag_processor_pid}" "${api_server_pid}"
        exit_code=$?
        set -e

        cleanup
        trap - INT TERM EXIT
        exit "${exit_code}"
        ;;
esac
