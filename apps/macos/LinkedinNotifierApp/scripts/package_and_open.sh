#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
APP_NAME="LinkedinNotifierApp"
APP_BUNDLE="${APP_ROOT}/dist/${APP_NAME}.app"

bash "${SCRIPT_DIR}/package_app.sh"

if pgrep -x "${APP_NAME}" >/dev/null 2>&1; then
  osascript -e "tell application \"${APP_NAME}\" to quit" >/dev/null 2>&1 || true
  for _ in {1..20}; do
    if ! pgrep -x "${APP_NAME}" >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
  pkill -x "${APP_NAME}" >/dev/null 2>&1 || true
fi

open "${APP_BUNDLE}"
