"""Shared environment loading helpers for worktree and container layouts."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import dotenv_values


def _resolve_repo_root(anchor: Path) -> Path:
    """Resolve the best available project root for env/path fallbacks.

    Priority:
    1) Monorepo root (`include/user_info` + `dags`) when present
    2) Resume Matcher repo root (`apps/backend` + `apps/frontend`)
    3) Container runtime root (`backend` + `frontend`)
    4) Directory containing the anchor
    """
    anchor = anchor.resolve()
    candidates = [anchor.parent, *anchor.parents]

    for candidate in candidates:
        if (candidate / "include" / "user_info").exists() and (candidate / "dags").exists():
            return candidate

    for candidate in candidates:
        if (candidate / "apps" / "backend").exists() and (candidate / "apps" / "frontend").exists():
            return candidate

    for candidate in candidates:
        if (candidate / "backend").exists() and (candidate / "frontend").exists():
            return candidate

    return anchor.parent


def _resolve_primary_repo_root(repo_root: Path) -> Path:
    if repo_root.parent.name == ".worktrees":
        return repo_root.parent.parent
    return repo_root


REPO_ROOT = _resolve_repo_root(Path(__file__))
PRIMARY_REPO_ROOT = _resolve_primary_repo_root(REPO_ROOT)


def resolve_repo_roots(anchor: Path) -> tuple[Path, Path]:
    """Resolve runtime repo and primary repo roots from an anchor file path."""
    repo_root = _resolve_repo_root(anchor)
    return repo_root, _resolve_primary_repo_root(repo_root)


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
