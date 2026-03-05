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
            load_dotenv(env_path)
        except Exception:
            pass

    if required_keys and override_if_missing and any(not os.getenv(key) for key in required_keys):
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


def spark_to_xcom_records(spark_df) -> list[dict]:
    if spark_df is None:
        return []

    records = []
    for row in spark_df.collect():
        row_dict = row.asDict(recursive=True)
        records.append({key: to_xcom_safe_value(value) for key, value in row_dict.items()})
    return records
