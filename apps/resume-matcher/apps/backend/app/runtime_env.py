"""Shared environment loading helpers for Resume Matcher repo layouts."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import dotenv_values


def resolve_repo_root(anchor: Path) -> Path:
    """Resolve the Resume Matcher project root from an anchor path."""
    anchor = anchor.resolve()
    candidates = [anchor.parent, *anchor.parents]

    for candidate in candidates:
        if (candidate / "apps" / "backend").exists() and (candidate / "apps" / "frontend").exists():
            return candidate

    for candidate in candidates:
        if (candidate / "backend").exists() and (candidate / "frontend").exists():
            return candidate

    return anchor.parent


def _env_file_candidates(repo_root: Path) -> list[Path]:
    candidates = [repo_root / ".env"]
    if (repo_root / "apps" / "backend").exists():
        candidates.insert(0, repo_root / "apps" / "backend" / ".env")
    if (repo_root / "backend").exists():
        candidates.insert(0, repo_root / "backend" / ".env")

    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


REPO_ROOT = resolve_repo_root(Path(__file__))


def resolve_repo_roots(anchor: Path) -> tuple[Path, Path]:
    """Backward-compatible wrapper for callers expecting a two-tuple."""
    repo_root = resolve_repo_root(anchor)
    return repo_root, repo_root


@lru_cache(maxsize=1)
def load_shared_env_values() -> dict[str, str]:
    """Load environment values from Resume Matcher env files."""
    values: dict[str, str] = {}
    for path in _env_file_candidates(REPO_ROOT):
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
