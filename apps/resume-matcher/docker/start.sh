#!/usr/bin/env sh
set -eu

# Backend stays internal for API rewrites and health checks.
BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${BACKEND_PORT:-8000}"

# Frontend is the public entrypoint.
FRONTEND_HOST="${FRONTEND_HOST:-0.0.0.0}"
FRONTEND_PORT="${PORT:-3000}"

cd /app/backend
python -m uvicorn app.main:app --host "${BACKEND_HOST}" --port "${BACKEND_PORT}" &
BACKEND_PID=$!

cleanup() {
  kill "${BACKEND_PID}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cd /app/frontend
HOSTNAME="${FRONTEND_HOST}" PORT="${FRONTEND_PORT}" node server.js &
FRONTEND_PID=$!

wait "${FRONTEND_PID}"
FRONTEND_STATUS=$?

kill "${BACKEND_PID}" >/dev/null 2>&1 || true
wait "${BACKEND_PID}" >/dev/null 2>&1 || true

exit "${FRONTEND_STATUS}"
