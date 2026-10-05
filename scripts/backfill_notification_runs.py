#!/usr/bin/env python3
"""Backfill durable notification run tables from historical profile_jobs.notified_at."""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dags import database


def main() -> None:
    result = database.backfill_notification_runs()
    print(
        "Backfilled notification runs: "
        f"created_runs={result['created_runs']} inserted_jobs={result['inserted_jobs']}"
    )


if __name__ == "__main__":
    main()
