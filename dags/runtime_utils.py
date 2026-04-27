from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
from dotenv import load_dotenv


def _default_env_candidates() -> list[str]:
    dags_dir = Path(__file__).resolve().parent
    return [
        str((dags_dir.parent / ".env").resolve()),
        str((dags_dir / ".env").resolve()),
        "/usr/local/airflow/.env",
        "/usr/local/airflow/dags/.env",
    ]


def load_env(
    required_keys: Sequence[str] | None = None,
    override_if_missing: bool = False,
    env_candidates: Iterable[str] | None = None,
) -> None:
    candidates = list(env_candidates or _default_env_candidates())
    for env_path in candidates:
        try:
            load_dotenv(env_path, override=False)
        except Exception:
            pass

    if (
        required_keys
        and override_if_missing
        and any(not os.getenv(key) for key in required_keys)
    ):
        for env_path in candidates:
            try:
                load_dotenv(env_path, override=True)
            except Exception:
                pass


def to_xcom_safe_value(value):
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    return value


def df_to_xcom_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []

    safe_df = df.astype(object).where(pd.notna(df), None).copy()
    for col in safe_df.columns:
        safe_df[col] = safe_df[col].map(to_xcom_safe_value)
    return safe_df.to_dict(orient="records")


def runtime_conf_value(key: str, default=None):
    """Return a value from the current Airflow dag_run.conf when available."""
    try:
        from airflow.sdk import get_current_context

        context = get_current_context()
        dag_run = context.get("dag_run") if context else None
        conf = getattr(dag_run, "conf", None) or {}
        if isinstance(conf, dict) and key in conf:
            return conf.get(key)
    except Exception:
        pass
    return default


def runtime_bool(key: str, default: bool = False) -> bool:
    value = runtime_conf_value(key, None)
    if value is None:
        value = os.getenv(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "on"}:
        return True
    if normalized in {"false", "0", "no", "n", "off"}:
        return False
    return default


def runtime_int(
    key: str,
    default: int,
    *,
    fallback_key: str | None = None,
    minimum: int | None = None,
) -> int:
    value = runtime_conf_value(key, None)
    if value is None and fallback_key:
        value = runtime_conf_value(fallback_key, None)
    if value is None:
        value = os.getenv(key)
    if value is None and fallback_key:
        value = os.getenv(fallback_key)
    try:
        result = int(value) if value is not None else int(default)
    except (TypeError, ValueError):
        result = int(default)
    if minimum is not None:
        result = max(minimum, result)
    return result
