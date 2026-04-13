from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from urllib.parse import urlencode


DEFAULT_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 7


def _urlsafe_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _urlsafe_b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(f"{data}{padding}")


def build_materials_launch_token(
    *,
    profile_id: int,
    job_id: str,
    secret: str,
    expires_at: int | None = None,
    ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS,
) -> str:
    normalized_secret = str(secret or "").strip()
    if not normalized_secret:
        raise ValueError("materials launch secret is required")

    if expires_at is None:
        expires_at = int(time.time()) + max(1, int(ttl_seconds))

    payload = {
        "profile_id": int(profile_id),
        "job_id": str(job_id),
        "exp": int(expires_at),
    }
    payload_bytes = json.dumps(
        payload,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    signature = hmac.new(
        normalized_secret.encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).digest()
    return f"{_urlsafe_b64encode(payload_bytes)}.{_urlsafe_b64encode(signature)}"


def verify_materials_launch_token(*, token: str, secret: str) -> dict:
    normalized_secret = str(secret or "").strip()
    if not normalized_secret:
        raise ValueError("materials launch secret is required")

    try:
        encoded_payload, encoded_signature = str(token).split(".", 1)
    except ValueError as exc:
        raise ValueError("invalid token format") from exc

    payload_bytes = _urlsafe_b64decode(encoded_payload)
    expected_signature = hmac.new(
        normalized_secret.encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).digest()
    actual_signature = _urlsafe_b64decode(encoded_signature)
    if not hmac.compare_digest(expected_signature, actual_signature):
        raise ValueError("invalid token signature")

    payload = json.loads(payload_bytes.decode("utf-8"))
    if int(payload.get("exp", 0)) < int(time.time()):
        raise ValueError("token expired")
    return payload


def build_materials_launch_url(
    *,
    base_url: str,
    profile_id: int,
    job_id: str,
    secret: str,
    ttl_seconds: int = DEFAULT_TOKEN_TTL_SECONDS,
) -> str:
    normalized_base_url = str(base_url or "").strip().rstrip("/")
    if not normalized_base_url:
        raise ValueError("resume matcher base url is required")

    token = build_materials_launch_token(
        profile_id=profile_id,
        job_id=job_id,
        secret=secret,
        ttl_seconds=ttl_seconds,
    )
    return f"{normalized_base_url}/launch?{urlencode({'token': token})}"


def maybe_build_materials_launch_url(
    *,
    profile_id: int | None,
    job_id: str | None,
    base_url: str | None = None,
    secret: str | None = None,
) -> str | None:
    if profile_id is None or not job_id:
        return None

    resolved_base_url = base_url or os.getenv("RESUME_MATCHER_BASE_URL")
    resolved_secret = secret or os.getenv("MATERIALS_LINK_SECRET")
    if not resolved_base_url or not resolved_secret:
        return None

    return build_materials_launch_url(
        base_url=resolved_base_url,
        profile_id=int(profile_id),
        job_id=str(job_id),
        secret=str(resolved_secret),
    )
