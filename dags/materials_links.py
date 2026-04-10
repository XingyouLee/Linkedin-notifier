from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone

from dags import database


DEFAULT_TOKEN_TTL_HOURS = 24 * 7


def _get_materials_secret() -> str:
    return (
        os.getenv("MATERIALS_LINK_SECRET")
        or os.getenv("AIRFLOW__API_AUTH__JWT_SECRET")
        or "dev-materials-secret"
    )


def _urlsafe_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _urlsafe_b64decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def build_materials_token(*, profile_id: int, job_id: str, expires_at: datetime) -> str:
    payload = {
        "profile_id": int(profile_id),
        "job_id": str(job_id),
        "purpose": "materials_generate",
        "exp": int(expires_at.replace(tzinfo=timezone.utc).timestamp()),
    }
    payload_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    payload_b64 = _urlsafe_b64encode(payload_text.encode("utf-8"))
    signature = hmac.new(
        _get_materials_secret().encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    signature_b64 = _urlsafe_b64encode(signature)
    return f"{payload_b64}.{signature_b64}"


def verify_materials_token(token: str) -> dict:
    try:
        payload_b64, signature_b64 = str(token).split(".", 1)
    except ValueError as error:
        raise ValueError("invalid_materials_token") from error

    expected_signature = hmac.new(
        _get_materials_secret().encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    actual_signature = _urlsafe_b64decode(signature_b64)
    if not hmac.compare_digest(expected_signature, actual_signature):
        raise ValueError("invalid_materials_token_signature")

    payload = json.loads(_urlsafe_b64decode(payload_b64).decode("utf-8"))
    if int(payload.get("exp") or 0) < int(datetime.now(timezone.utc).timestamp()):
        raise ValueError("expired_materials_token")
    return payload


def issue_materials_link(*, profile_id: int, job_id: str) -> str:
    base_url = (os.getenv("MATERIALS_APP_BASE_URL") or "http://127.0.0.1:8000").rstrip("/")
    expires_at = datetime.now(timezone.utc) + timedelta(hours=DEFAULT_TOKEN_TTL_HOURS)
    token = build_materials_token(
        profile_id=profile_id,
        job_id=job_id,
        expires_at=expires_at,
    )
    database.create_material_access_token(
        token=token,
        profile_id=profile_id,
        job_id=job_id,
        expires_at=expires_at,
    )
    return f"{base_url}/materials?token={token}"
