from __future__ import annotations

import json
import os
from typing import Any

import requests


def normalize_text(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def extract_output_text(response_json: dict[str, Any]) -> str | None:
    output_text = response_json.get("output_text")
    if output_text:
        return output_text

    for item in response_json.get("output") or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            text = content.get("text")
            if text:
                return text
    return None


def request_llm_json(
    *,
    request_url: str,
    api_key: str,
    model_name: str,
    prompt: str,
) -> dict:
    payload = {
        "model": model_name,
        "input": prompt,
    }
    response = requests.post(
        request_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        json=payload,
        timeout=120,
    )
    response.raise_for_status()

    response_json = response.json()
    output_text = extract_output_text(response_json)
    if not output_text:
        raise ValueError("response_missing_output_text")

    parsed = json.loads(output_text)
    if not isinstance(parsed, dict):
        raise ValueError("response_json_not_object")
    return parsed


def parse_llm_endpoints_from_env() -> list[dict[str, str]]:
    endpoints_json = normalize_text(os.getenv("LLM_ENDPOINTS_JSON"))
    endpoints: list[dict[str, str]] = []
    if endpoints_json:
        try:
            parsed = json.loads(endpoints_json)
        except Exception as error:
            raise ValueError(f"invalid_llm_endpoints_json: {error}") from error
        if not isinstance(parsed, list):
            raise ValueError("invalid_llm_endpoints_json: root must be a list")
        for index, entry in enumerate(parsed):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"invalid_llm_endpoints_json: entry {index} must be an object"
                )
            request_url = normalize_text(entry.get("request_url") or entry.get("url"))
            api_key = normalize_text(entry.get("api_key"))
            api_key_env = normalize_text(entry.get("api_key_env"))
            name = normalize_text(entry.get("name")) or f"endpoint_{index + 1}"
            if api_key is None and api_key_env:
                api_key = normalize_text(os.getenv(api_key_env))
            if not request_url or not api_key:
                raise ValueError(
                    f"invalid_llm_endpoints_json: entry {index} requires request_url and api_key/api_key_env"
                )
            entry_dict: dict[str, str] = {
                "name": name,
                "request_url": request_url,
                "api_key": api_key,
            }
            model = normalize_text(entry.get("model"))
            if model:
                entry_dict["model"] = model
            endpoints.append(entry_dict)

    if endpoints:
        return endpoints

    request_url = normalize_text(os.getenv("FITTING_REQUEST_URL"))
    api_key = normalize_text(os.getenv("LLM_API_KEY")) or normalize_text(
        os.getenv("GMN_API_KEY")
    )
    if request_url and api_key:
        return [
            {
                "name": "primary",
                "request_url": request_url,
                "api_key": api_key,
            }
        ]

    return []


def is_transient_llm_http_status(status_code) -> bool:
    try:
        normalized = int(status_code)
    except (TypeError, ValueError):
        return False
    return normalized in {408, 409, 425, 429, 500, 502, 503, 504}


def request_llm_json_with_fallback(
    *,
    endpoints: list[dict[str, str]],
    model_name: str,
    prompt: str,
    start_index: int = 0,
    return_metadata: bool = False,
) -> dict | tuple[dict, str]:
    transient_errors: list[str] = []
    fatal_errors: list[str] = []

    if not endpoints:
        raise RuntimeError("FATAL_API::no_llm_endpoints_available")

    normalized_start_index = start_index % len(endpoints)
    ordered_endpoints = endpoints[normalized_start_index:] + endpoints[:normalized_start_index]

    for endpoint in ordered_endpoints:
        endpoint_name = endpoint.get("name") or endpoint.get("request_url") or "endpoint"
        effective_model_name = endpoint.get("model") or model_name
        try:
            parsed = request_llm_json(
                request_url=endpoint["request_url"],
                api_key=endpoint["api_key"],
                model_name=effective_model_name,
                prompt=prompt,
            )
            if return_metadata:
                return parsed, effective_model_name
            return parsed
        except requests.HTTPError as error:
            status_code = error.response.status_code if error.response is not None else "unknown"
            message = (
                f"endpoint={endpoint_name} model_name={effective_model_name} "
                f"status={status_code} error={error}"
            )
            if is_transient_llm_http_status(status_code):
                transient_errors.append(message)
                continue
            fatal_errors.append(message)
            continue
        except (requests.Timeout, requests.ConnectionError) as error:
            transient_errors.append(
                f"endpoint={endpoint_name} model_name={effective_model_name} error={error}"
            )
            continue
        except requests.RequestException as error:
            fatal_errors.append(
                f"endpoint={endpoint_name} model_name={effective_model_name} error={error}"
            )
            continue
        except ValueError as error:
            message = (
                f"endpoint={endpoint_name} model_name={effective_model_name} error={error}"
            )
            if str(error) == "response_missing_output_text":
                transient_errors.append(message)
                continue
            fatal_errors.append(message)
            continue

    if transient_errors and not fatal_errors:
        raise RuntimeError("TRANSIENT_API::" + " | ".join(transient_errors))

    all_errors = fatal_errors + transient_errors
    if all_errors:
        raise RuntimeError("FATAL_API::" + " | ".join(all_errors))

    raise RuntimeError("FATAL_API::no_llm_endpoints_available")
