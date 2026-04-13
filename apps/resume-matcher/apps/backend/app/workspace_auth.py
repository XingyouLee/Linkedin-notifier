"""Workspace-scoped session helpers for Resume Matcher."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any, Iterator

from fastapi import HTTPException, Request, Response

from app.runtime_env import get_shared_setting

WORKSPACE_SESSION_COOKIE_NAME = "resume_matcher_session"
DEFAULT_WORKSPACE_SESSION_TTL_SECONDS = 60 * 60 * 24 * 7

_current_workspace_id: ContextVar[str | None] = ContextVar("resume_matcher_workspace_id", default=None)


def _urlsafe_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _urlsafe_b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(f"{data}{padding}")


def _get_workspace_session_secret() -> str:
    secret = (
        get_shared_setting("RESUME_MATCHER_SESSION_SECRET").strip()
        or get_shared_setting("MATERIALS_LINK_SECRET").strip()
    )
    if not secret:
        raise RuntimeError(
            "Resume Matcher workspace session secret is not configured. "
            "Set RESUME_MATCHER_SESSION_SECRET or MATERIALS_LINK_SECRET."
        )
    return secret


def build_workspace_session_token(
    *,
    workspace_id: str,
    expires_at: int | None = None,
    ttl_seconds: int = DEFAULT_WORKSPACE_SESSION_TTL_SECONDS,
) -> str:
    """Build a signed workspace session token."""
    normalized_workspace_id = str(workspace_id or "").strip()
    if not normalized_workspace_id:
        raise ValueError("workspace_id is required")

    if expires_at is None:
        expires_at = int(time.time()) + max(1, int(ttl_seconds))

    payload = {
        "workspace_id": normalized_workspace_id,
        "exp": int(expires_at),
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signature = hmac.new(
        _get_workspace_session_secret().encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).digest()
    return f"{_urlsafe_b64encode(payload_bytes)}.{_urlsafe_b64encode(signature)}"


def verify_workspace_session_token(token: str) -> dict[str, Any]:
    """Verify and decode a workspace session token."""
    try:
        encoded_payload, encoded_signature = str(token).split(".", 1)
    except ValueError as exc:
        raise ValueError("invalid workspace session token format") from exc

    payload_bytes = _urlsafe_b64decode(encoded_payload)
    expected_signature = hmac.new(
        _get_workspace_session_secret().encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).digest()
    actual_signature = _urlsafe_b64decode(encoded_signature)
    if not hmac.compare_digest(expected_signature, actual_signature):
        raise ValueError("invalid workspace session signature")

    payload = json.loads(payload_bytes.decode("utf-8"))
    if int(payload.get("exp", 0)) < int(time.time()):
        raise ValueError("workspace session expired")
    return payload


def derive_workspace_id_from_launch_token(token: str) -> str:
    """Derive a deterministic workspace id from a launch token."""
    digest = hmac.new(
        _get_workspace_session_secret().encode("utf-8"),
        f"launch-workspace:{token}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"launch-{digest[:32]}"


def set_workspace_session_cookie(
    response: Response,
    *,
    workspace_id: str,
    expires_at: int,
    request: Request | None = None,
) -> None:
    """Attach a workspace session cookie to a response."""
    now = int(time.time())
    max_age = max(1, int(expires_at) - now)
    secure = bool(request and request.url.scheme == "https")
    response.set_cookie(
        WORKSPACE_SESSION_COOKIE_NAME,
        build_workspace_session_token(workspace_id=workspace_id, expires_at=int(expires_at)),
        max_age=max_age,
        httponly=True,
        samesite="lax",
        secure=secure,
        path="/",
    )


def get_current_workspace_id() -> str | None:
    """Return the currently bound workspace id for the active request context."""
    return _current_workspace_id.get()


@contextmanager
def workspace_scope(workspace_id: str) -> Iterator[None]:
    """Temporarily bind a workspace id to the current execution context."""
    token = _current_workspace_id.set(str(workspace_id))
    try:
        yield
    finally:
        _current_workspace_id.reset(token)


def bind_workspace_from_request(request: Request) -> Token[str | None] | None:
    """Bind request workspace cookie to the current context if it is valid."""
    cookie = request.cookies.get(WORKSPACE_SESSION_COOKIE_NAME)
    if not cookie:
        request.state.workspace_session = None
        request.state.invalid_workspace_session = False
        return None

    try:
        payload = verify_workspace_session_token(cookie)
    except ValueError:
        request.state.workspace_session = None
        request.state.invalid_workspace_session = True
        return None

    request.state.workspace_session = payload
    request.state.invalid_workspace_session = False
    return _current_workspace_id.set(str(payload["workspace_id"]))


def reset_bound_workspace(token: Token[str | None] | None) -> None:
    """Reset the current workspace context after a request finishes."""
    if token is not None:
        _current_workspace_id.reset(token)


async def require_workspace_session(request: Request) -> dict[str, Any]:
    """Require a valid workspace session cookie for protected API routes."""
    payload = getattr(request.state, "workspace_session", None)
    if payload:
        return payload

    if getattr(request.state, "invalid_workspace_session", False):
        raise HTTPException(status_code=401, detail="Workspace session is invalid or expired.")

    cookie = request.cookies.get(WORKSPACE_SESSION_COOKIE_NAME)
    if not cookie:
        raise HTTPException(status_code=401, detail="Workspace session is required.")

    try:
        return verify_workspace_session_token(cookie)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Workspace session is invalid or expired.") from exc


async def forbid_workspace_session(request: Request) -> None:
    """Block shared admin/config routes from workspace-scoped user sessions."""
    if getattr(request.state, "workspace_session", None):
        raise HTTPException(
            status_code=403,
            detail="Settings are not available inside workspace sessions.",
        )
