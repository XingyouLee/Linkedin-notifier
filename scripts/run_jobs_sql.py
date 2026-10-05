#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]

# Quick-edit mode:
# 1. Put your SQL into DEFAULT_SQL below.
# 2. Run: python scripts/run_jobs_sql.py
# CLI argument / --file / stdin still override this block.
DEFAULT_SQL = """




"""
DEFAULT_EXPANDED = False
DEFAULT_ROLLBACK = False


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run SQL against the configured jobs database using psql inside a Docker container. "
            "You can either edit DEFAULT_SQL in this file or pass SQL via CLI."
        )
    )
    parser.add_argument(
        "sql",
        nargs="?",
        help="SQL text to execute. If omitted, use --file or pipe SQL via stdin.",
    )
    parser.add_argument(
        "--file",
        type=Path,
        help="Read SQL from a .sql file.",
    )
    parser.add_argument(
        "--container",
        help="Explicit Postgres container name. Default: auto-detect.",
    )
    parser.add_argument(
        "--db",
        help="Database name. Default: derived from JOBS_DB_URL.",
    )
    parser.add_argument(
        "--user",
        help="Database user. Default: derived from JOBS_DB_URL.",
    )
    parser.add_argument(
        "--expanded",
        action="store_true",
        help="Use psql expanded output (-x).",
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Wrap the SQL in BEGIN/ROLLBACK so no changes are persisted.",
    )
    return parser.parse_args()


def _read_sql(args: argparse.Namespace) -> str:
    if args.sql:
        if args.file:
            raise SystemExit("Provide SQL using exactly one of: argument, --file, or stdin.")
        sql_text = args.sql
    elif args.file:
        sql_text = args.file.read_text(encoding="utf-8")
    elif not sys.stdin.isatty():
        sql_text = sys.stdin.read()
    elif DEFAULT_SQL.strip():
        sql_text = DEFAULT_SQL
    else:
        raise SystemExit(
            "No SQL provided. Edit DEFAULT_SQL in this file, pass SQL text, use --file, or pipe SQL via stdin."
        )

    sql_text = sql_text.strip()
    if not sql_text:
        raise SystemExit("SQL input is empty.")
    return sql_text


def _load_local_env() -> dict[str, str]:
    env_values: dict[str, str] = {}
    for candidate in (REPO_ROOT / ".env", REPO_ROOT / "dags" / ".env"):
        if not candidate.exists():
            continue
        for line in candidate.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            env_values[key.strip()] = value.strip().strip("\"'").strip()
    return env_values


def _resolve_db_config(args: argparse.Namespace) -> tuple[str, str]:
    env_values = _load_local_env()
    raw_url = os.getenv("JOBS_DB_URL") or env_values.get("JOBS_DB_URL") or ""
    parsed = urlparse(raw_url) if raw_url else None

    if not parsed and not args.db and not args.user:
        raise SystemExit(
            "Set JOBS_DB_URL or pass both --db and --user before running SQL."
        )

    db_name = args.db or (parsed.path.lstrip("/") if parsed and parsed.path else "")
    db_user = args.user or (parsed.username if parsed and parsed.username else "")

    if not db_name or not db_user:
        raise SystemExit(
            "Could not determine database name and user. Set JOBS_DB_URL or pass --db and --user explicitly."
        )

    return db_name, db_user


def _detect_postgres_container() -> str:
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    names = [line.strip() for line in result.stdout.splitlines() if line.strip()]

    preferred = [
        name
        for name in names
        if "linkedin-notifier" in name.lower() and "postgres" in name.lower()
    ]
    if len(preferred) == 1:
        return preferred[0]

    candidates = [name for name in names if "postgres" in name.lower()]
    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        raise SystemExit("No running Postgres container found. Is the Docker stack up?")

    joined = "\n".join(f"  - {name}" for name in candidates)
    raise SystemExit(
        "Multiple Postgres containers found. Re-run with --container.\n"
        f"{joined}"
    )


def _build_sql(sql_text: str, *, rollback: bool) -> str:
    if not rollback:
        return sql_text if sql_text.endswith(";") else f"{sql_text};"
    return f"BEGIN;\n{sql_text.rstrip().rstrip(';')};\nROLLBACK;\n"


def _run_psql(
    sql_text: str,
    *,
    container: str,
    db_name: str,
    db_user: str,
    expanded: bool,
) -> int:
    command = [
        "docker",
        "exec",
        "-i",
        container,
        "psql",
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-P",
        "pager=off",
        "-U",
        db_user,
        "-d",
        db_name,
    ]
    if expanded:
        command.append("-x")

    result = subprocess.run(command, input=sql_text, text=True)
    return result.returncode


def main() -> None:
    args = _parse_args()
    sql_text = _read_sql(args)
    db_name, db_user = _resolve_db_config(args)
    container = args.container or _detect_postgres_container()
    rollback = args.rollback or DEFAULT_ROLLBACK
    expanded = args.expanded or DEFAULT_EXPANDED
    final_sql = _build_sql(sql_text, rollback=rollback)

    exit_code = _run_psql(
        final_sql,
        container=container,
        db_name=db_name,
        db_user=db_user,
        expanded=expanded,
    )
    if exit_code != 0:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
