"""Shared environment loading helpers for worktree and primary repo layouts."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[5]
PRIMARY_REPO_ROOT = REPO_ROOT.parent.parent if REPO_ROOT.parent.name == ".worktrees" else REPO_ROOT


@lru_cache(maxsize=1)
def load_shared_env_values() -> dict[str, str]:
    """Load environment values from the repo root and dags/.env files."""
    values: dict[str, str] = {}
    env_roots = [REPO_ROOT]
    if PRIMARY_REPO_ROOT != REPO_ROOT:
        env_roots.append(PRIMARY_REPO_ROOT)

    for root in env_roots:
        for path in (root / ".env", root / "dags" / ".env"):
            if not path.exists():
                continue
            for key, value in dotenv_values(path).items():
                if key and value and key not in values:
                    values[key] = str(value)
    return values


def get_shared_setting(name: str) -> str:
    """Read a setting from process env with repo env-file fallback."""
    value = os.getenv(name)
    if value:
        return value
    return load_shared_env_values().get(name, "")
