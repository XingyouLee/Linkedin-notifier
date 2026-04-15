"""LiteLLM wrapper for multi-provider AI support."""

import asyncio
import json
import logging
import os
import re
import threading
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

import litellm
from litellm import Router
from litellm.router import RetryPolicy
from pydantic import BaseModel

from app.config import settings
from app.runtime_env import load_shared_env_values

LITELLM_LOGGER_NAMES = ("LiteLLM", "LiteLLM Router", "LiteLLM Proxy")


def _configure_litellm_logging() -> None:
    """Align LiteLLM logger levels with application settings."""
    numeric_level = getattr(logging, settings.log_llm, logging.WARNING)
    for logger_name in LITELLM_LOGGER_NAMES:
        logging.getLogger(logger_name).setLevel(numeric_level)


_configure_litellm_logging()

# LLM timeout configuration (seconds) - base values
LLM_TIMEOUT_HEALTH_CHECK = 30
LLM_TIMEOUT_COMPLETION = 120
LLM_TIMEOUT_JSON = 180  # JSON completions may take longer

# JSON-010: JSON extraction safety limits
MAX_JSON_EXTRACTION_RECURSION = 10
MAX_JSON_CONTENT_SIZE = 1024 * 1024  # 1MB


class LLMConfig(BaseModel):
    """LLM configuration model."""

    provider: str
    model: str
    api_key: str
    api_base: str | None = None


def _normalize_api_base(provider: str, api_base: str | None) -> str | None:
    """Normalize api_base for LiteLLM provider-specific expectations.

    When using proxies/aggregators, users often paste a base URL that already
    includes a version segment (e.g., `/v1`). Some LiteLLM provider handlers
    append those segments internally, which can lead to duplicated paths like
    `/v1/v1/...` and cause 404s.
    """
    if not api_base:
        return None

    base = api_base.strip()
    if not base:
        return None

    base = base.rstrip("/")

    # Anthropic handler appends '/v1/messages'. If base already ends with '/v1',
    # strip it to avoid '/v1/v1/messages'.
    if provider == "anthropic" and base.endswith("/v1"):
        base = base[: -len("/v1")].rstrip("/")

    # Gemini handler appends '/v1/models/...'. If base already ends with '/v1',
    # strip it to avoid '/v1/v1/models/...'.
    if provider == "gemini" and base.endswith("/v1"):
        base = base[: -len("/v1")].rstrip("/")

    # OpenRouter base is https://openrouter.ai/api/v1. LiteLLM appends /v1
    # internally, so strip it to avoid /v1/v1.
    if provider == "openrouter" and base.endswith("/v1"):
        base = base[: -len("/v1")].rstrip("/")

    # Ollama doesn't use /v1 paths. Strip common suffixes users might paste:
    # /v1, /api/chat, /api/generate
    if provider == "ollama":
        for suffix in ("/v1", "/api/chat", "/api/generate", "/api"):
            if base.endswith(suffix):
                base = base[: -len(suffix)].rstrip("/")
                break

    return base or None


def _extract_text_parts(value: Any, depth: int = 0, max_depth: int = 10) -> list[str]:
    """Recursively extract text segments from nested response structures.

    Handles strings, lists, dicts with 'text'/'content'/'value' keys, and objects
    with text/content attributes. Limits recursion depth to avoid cycles.

    Args:
        value: Input value that may contain text in strings, lists, dicts, or objects.
        depth: Current recursion depth.
        max_depth: Maximum recursion depth before returning no content.

    Returns:
        A list of extracted text segments.
    """
    if depth >= max_depth:
        return []

    if value is None:
        return []

    if isinstance(value, str):
        return [value]

    if isinstance(value, list):
        parts: list[str] = []
        next_depth = depth + 1
        for item in value:
            parts.extend(_extract_text_parts(item, next_depth, max_depth))
        return parts

    if isinstance(value, dict):
        next_depth = depth + 1
        if "text" in value:
            return _extract_text_parts(value.get("text"), next_depth, max_depth)
        if "content" in value:
            return _extract_text_parts(value.get("content"), next_depth, max_depth)
        if "value" in value:
            return _extract_text_parts(value.get("value"), next_depth, max_depth)
        return []

    next_depth = depth + 1
    if hasattr(value, "text"):
        return _extract_text_parts(getattr(value, "text"), next_depth, max_depth)
    if hasattr(value, "content"):
        return _extract_text_parts(getattr(value, "content"), next_depth, max_depth)

    return []


def _join_text_parts(parts: list[str]) -> str | None:
    """Join text parts with newlines, filtering empty strings.

    Args:
        parts: Candidate text segments.

    Returns:
        Joined string or None if the result is empty.
    """
    joined = "\n".join(part for part in parts if part).strip()
    return joined or None


def _extract_message_text(message: Any) -> str | None:
    """Extract plain text from a LiteLLM message object across providers."""
    content: Any = None

    if hasattr(message, "content"):
        content = message.content
    elif isinstance(message, dict):
        content = message.get("content")

    return _join_text_parts(_extract_text_parts(content))


def _safe_get(obj: Any, key: str) -> Any:
    """Get attribute or dict key from an object."""
    if hasattr(obj, key):
        return getattr(obj, key)
    if isinstance(obj, dict):
        return obj.get(key)
    return None


def _extract_choice_text(choice: Any) -> str | None:
    """Extract plain text from a LiteLLM choice object.

    Tries message.content first, then choice.text, then choice.delta. Handles both
    object attributes and dict keys.
    """
    content = _extract_message_text(_safe_get(choice, "message"))
    if content:
        return content

    for field in ("text", "delta"):
        value = _safe_get(choice, field)
        if value is not None:
            extracted = _join_text_parts(_extract_text_parts(value))
            if extracted:
                return extracted

    return None


def _to_code_block(content: str | None, language: str = "text") -> str:
    """Wrap content in a markdown code block for client display."""
    text = (content or "").strip()
    if not text:
        text = "<empty>"
    return f"```{language}\n{text}\n```"


def _load_stored_config() -> dict:
    """Load config from config.json file."""
    config_path = settings.config_path
    if config_path.exists():
        try:
            return json.loads(config_path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _shared_env_get(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    return load_shared_env_values().get(name, "")


def _extract_responses_output_text(response_json: dict[str, Any]) -> str | None:
    output_text = response_json.get("output_text")
    if output_text:
        return str(output_text)

    for item in response_json.get("output") or []:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if text:
                return str(text)
    return None


def _parse_shared_llm_endpoints() -> list[dict[str, str]]:
    endpoints_json = (_shared_env_get("LLM_ENDPOINTS_JSON") or "").strip()
    endpoints: list[dict[str, str]] = []
    if endpoints_json:
        try:
            parsed = json.loads(endpoints_json)
        except Exception as exc:
            raise ValueError(f"invalid_llm_endpoints_json: {exc}") from exc
        if not isinstance(parsed, list):
            raise ValueError("invalid_llm_endpoints_json: root must be a list")
        for index, entry in enumerate(parsed):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"invalid_llm_endpoints_json: entry {index} must be an object"
                )
            request_url = str(entry.get("request_url") or entry.get("url") or "").strip()
            api_key = str(entry.get("api_key") or "").strip()
            api_key_env = str(entry.get("api_key_env") or "").strip()
            name = str(entry.get("name") or f"endpoint_{index + 1}").strip()
            model = str(entry.get("model") or "").strip()
            if not api_key and api_key_env:
                api_key = _shared_env_get(api_key_env).strip()
            if request_url and api_key:
                endpoint = {
                    "name": name or f"endpoint_{index + 1}",
                    "request_url": request_url,
                    "api_key": api_key,
                }
                if model:
                    endpoint["model"] = model
                endpoints.append(endpoint)

    if endpoints:
        return endpoints

    request_url = (_shared_env_get("FITTING_REQUEST_URL") or "").strip()
    api_key = (_shared_env_get("LLM_API_KEY") or _shared_env_get("GMN_API_KEY") or "").strip()
    if request_url and api_key:
        return [{"name": "primary", "request_url": request_url, "api_key": api_key}]

    return []


def _shared_env_model_name() -> str:
    return (_shared_env_get("FITTING_MODEL_NAME") or settings.llm_model or "gpt-5.4").strip()


def _shared_env_completion_is_enabled(config: LLMConfig) -> bool:
    if config.provider == "ollama" and config.api_key:
        return False
    return not config.api_key and bool(_parse_shared_llm_endpoints())


def _is_transient_fallback_http_status(status_code: int | str | None) -> bool:
    try:
        code = int(status_code) if status_code is not None else 0
    except (TypeError, ValueError):
        return False
    return code == 408 or code == 429 or code >= 500


def _serialize_shared_responses_input(messages: list[dict[str, str]]) -> str:
    """Flatten chat messages into one prompt string for proxy compatibility.

    Some OpenAI-compatible proxy endpoints used by this project reject
    Responses API payloads that contain message arrays with a `system` role,
    while the same content succeeds when sent as a single string input.
    Keep the shared-env path compatible with those proxies by collapsing the
    conversation into one text prompt.
    """
    instruction_parts: list[str] = []
    conversation_parts: list[str] = []

    for message in messages:
        role = str(message.get("role") or "user").strip().lower()
        content = str(message.get("content") or "").strip()
        if not content:
            continue

        if role in {"system", "developer"}:
            instruction_parts.append(content)
            continue

        if role == "user":
            conversation_parts.append(content)
            continue

        conversation_parts.append(f"{role.upper()}:\n{content}")

    if instruction_parts and not conversation_parts:
        return "\n\n".join(instruction_parts)

    parts: list[str] = []
    if instruction_parts:
        parts.append("\n\n".join(instruction_parts))
    if conversation_parts:
        parts.append("\n\n".join(conversation_parts))
    return "\n\n".join(parts).strip()


def _request_shared_responses_text(
    *,
    request_url: str,
    api_key: str,
    model_name: str,
    messages: list[dict[str, str]],
    max_tokens: int,
) -> str:
    payload = {
        "model": model_name,
        "input": _serialize_shared_responses_input(messages),
        "max_output_tokens": max_tokens,
    }
    request = urllib_request.Request(
        request_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with urllib_request.urlopen(request, timeout=LLM_TIMEOUT_JSON) as response:
        response_json = json.load(response)
    output_text = _extract_responses_output_text(response_json)
    if not output_text:
        raise ValueError("response_missing_output_text")
    return output_text


def _request_shared_responses_text_with_fallback(
    *,
    messages: list[dict[str, str]],
    model_name: str,
    max_tokens: int,
) -> str:
    transient_errors: list[str] = []
    fatal_errors: list[str] = []

    for endpoint in _parse_shared_llm_endpoints():
        endpoint_name = endpoint.get("name") or endpoint.get("request_url") or "endpoint"
        endpoint_model_name = str(endpoint.get("model") or model_name).strip() or model_name
        try:
            return _request_shared_responses_text(
                request_url=endpoint["request_url"],
                api_key=endpoint["api_key"],
                model_name=endpoint_model_name,
                messages=messages,
                max_tokens=max_tokens,
            )
        except urllib_error.HTTPError as exc:
            status_code = exc.code
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", "ignore")
            except Exception:
                error_body = str(exc)
            message = (
                f"endpoint={endpoint_name} model_name={endpoint_model_name} "
                f"status={status_code} error={error_body or exc}"
            )
            if _is_transient_fallback_http_status(status_code):
                transient_errors.append(message)
                continue
            fatal_errors.append(message)
        except (urllib_error.URLError, TimeoutError) as exc:
            transient_errors.append(
                f"endpoint={endpoint_name} model_name={endpoint_model_name} error={exc}"
            )
        except Exception as exc:
            fatal_errors.append(
                f"endpoint={endpoint_name} model_name={endpoint_model_name} error={exc}"
            )

    if transient_errors and not fatal_errors:
        raise RuntimeError("TRANSIENT_API::" + " | ".join(transient_errors))

    all_errors = fatal_errors + transient_errors
    if all_errors:
        raise RuntimeError("FATAL_API::" + " | ".join(all_errors))

    raise RuntimeError("FATAL_API::no_llm_endpoints_available")


async def _complete_via_shared_responses(
    *,
    messages: list[dict[str, str]],
    max_tokens: int,
) -> str:
    return await asyncio.to_thread(
        _request_shared_responses_text_with_fallback,
        messages=messages,
        model_name=_shared_env_model_name(),
        max_tokens=max_tokens,
    )


_PROVIDER_KEY_MAP: dict[str, str] = {
    "openai": "openai",
    "anthropic": "anthropic",
    "gemini": "google",
    "openrouter": "openrouter",
    "deepseek": "deepseek",
    "ollama": "ollama",
}


def resolve_api_key(stored: dict, provider: str) -> str:
    """Resolve the effective API key from stored config.

    Priority: top-level api_key > api_keys[provider] > env/settings default.

    This is the single source of truth for key resolution.  Every code path
    that needs an API key (runtime, config display, health check, test
    endpoint) must call this function instead of reading ``stored["api_key"]``
    directly.
    """
    api_key = stored.get("api_key", "")
    if not api_key:
        api_keys = stored.get("api_keys", {})
        if not isinstance(api_keys, dict):
            api_keys = {}
        config_provider = _PROVIDER_KEY_MAP.get(provider, provider)
        api_key = api_keys.get(config_provider, settings.llm_api_key)
    return api_key


def get_runtime_llm_config() -> LLMConfig:
    """Get env/shared-runtime LLM configuration without loading stored config."""
    return LLMConfig(
        provider=settings.llm_provider,
        model=settings.llm_model,
        api_key=settings.llm_api_key,
        api_base=settings.llm_api_base,
    )


def is_llm_configured(config: LLMConfig) -> bool:
    """Return whether the supplied config has enough information to attempt generation."""
    if config.provider == "ollama":
        return True
    return bool(config.api_key) or _shared_env_completion_is_enabled(config)


def get_llm_config() -> LLMConfig:
    """Get current LLM configuration.

    Priority for api_key: top-level api_key > api_keys[provider] > env/settings
    """
    stored = _load_stored_config()
    provider = stored.get("provider", settings.llm_provider)
    api_key = resolve_api_key(stored, provider)

    return LLMConfig(
        provider=provider,
        model=stored.get("model", settings.llm_model),
        api_key=api_key,
        api_base=stored.get("api_base", settings.llm_api_base),
    )


def get_model_name(config: LLMConfig) -> str:
    """Convert provider/model to LiteLLM format.

    For most providers, adds the provider prefix if not already present.
    For OpenRouter, always adds 'openrouter/' prefix since OpenRouter models
    use nested prefixes like 'openrouter/anthropic/claude-3.5-sonnet'.
    """
    provider_prefixes = {
        "openai": "",  # OpenAI models don't need prefix
        "anthropic": "anthropic/",
        "openrouter": "openrouter/",
        "gemini": "gemini/",
        "deepseek": "deepseek/",
        "ollama": "ollama_chat/",  # ollama_chat/ routes to /api/chat (supports messages array)
    }

    prefix = provider_prefixes.get(config.provider, "")

    # OpenRouter is special: always add openrouter/ prefix unless already present
    # OpenRouter models use nested format: openrouter/anthropic/claude-3.5-sonnet
    if config.provider == "openrouter":
        if config.model.startswith("openrouter/"):
            return config.model
        return f"openrouter/{config.model}"

    # For other providers, don't add prefix if model already has a known prefix
    known_prefixes = ["openrouter/", "anthropic/",
                      "gemini/", "deepseek/", "ollama/", "ollama_chat/"]
    if any(config.model.startswith(p) for p in known_prefixes):
        return config.model

    # Add provider prefix for models that need it
    return f"{prefix}{config.model}" if prefix else config.model


# ---------------------------------------------------------------------------
# Router — centralises transport retries, cooldowns, and error-type policies
# ---------------------------------------------------------------------------

_router: Router | None = None
_router_config_key: str = ""
_router_lock = threading.Lock()


def _config_fingerprint(config: LLMConfig) -> str:
    """Generate a fingerprint to detect config changes.

    Uses Python's built-in ``hash()`` on the API key — stable within a
    single process (which is the cache lifetime), collision-resistant,
    and not a cryptographic function so it won't trigger CodeQL alerts.
    The raw key is never stored in the fingerprint string.
    """
    key_hash = hash(config.api_key) if config.api_key else 0
    return f"{config.provider}|{config.model}|{key_hash}|{config.api_base}"


def _build_router(config: LLMConfig) -> Router:
    """Build a LiteLLM Router with error-type retry policies."""
    model_name = get_model_name(config)

    litellm_params: dict[str, Any] = {"model": model_name}
    if config.api_key:
        litellm_params["api_key"] = config.api_key
    api_base = _normalize_api_base(config.provider, config.api_base)
    if api_base:
        litellm_params["api_base"] = api_base

    return Router(
        model_list=[
            {
                "model_name": "primary",
                "litellm_params": litellm_params,
            }
        ],
        num_retries=3,
        retry_policy=RetryPolicy(
            AuthenticationErrorRetries=0,
            BadRequestErrorRetries=0,
            TimeoutErrorRetries=2,
            RateLimitErrorRetries=3,
            ContentPolicyViolationErrorRetries=0,
            InternalServerErrorRetries=2,
        ),
        # Cooldowns disabled: with a single deployment and no fallback,
        # cooldowns would blackout the backend on transient failures.
        # Re-enable when a fallback deployment is added.
        disable_cooldowns=True,
    )


def get_router(config: LLMConfig | None = None) -> tuple[Router, LLMConfig]:
    """Get or rebuild the LiteLLM Router.

    The Router is cached and only rebuilt when the underlying config changes.
    Returns the Router and the config it was built from.
    """
    global _router, _router_config_key

    if config is None:
        config = get_llm_config()

    key = _config_fingerprint(config)
    with _router_lock:
        if _router is None or _router_config_key != key:
            _router = _build_router(config)
            _router_config_key = key
            logging.info("LiteLLM Router rebuilt for %s/%s", config.provider, config.model)
        router = _router

    return router, config


def _supports_temperature(provider: str, model: str) -> bool:
    """Return whether passing `temperature` is supported for this model/provider combo.

    Some models (e.g., OpenAI gpt-5 family) reject temperature values other than 1,
    and LiteLLM may error when temperature is passed.
    """
    _ = provider
    model_lower = model.lower()
    if "gpt-5" in model_lower:
        return False
    return True


def _get_reasoning_effort(provider: str, model: str) -> str | None:
    """Return a default reasoning_effort for models that require it.

    Some OpenAI gpt-5 models may return empty message.content unless a supported
    `reasoning_effort` is explicitly set. This keeps downstream JSON parsing reliable.
    """
    _ = provider
    model_lower = model.lower()
    if "gpt-5" in model_lower:
        return "minimal"
    return None


async def check_llm_health(
    config: LLMConfig | None = None,
    *,
    include_details: bool = False,
    test_prompt: str | None = None,
) -> dict[str, Any]:
    """Check if the LLM provider is accessible and working."""
    if config is None:
        config = get_llm_config()

    if _shared_env_completion_is_enabled(config):
        provider = "shared-env-responses"
        model_name = _shared_env_model_name()
        prompt = test_prompt or "Hi"
        try:
            content = await _complete_via_shared_responses(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=32,
            )
            result: dict[str, Any] = {
                "healthy": True,
                "provider": provider,
                "model": model_name,
                "response_model": model_name,
            }
            if include_details:
                result["test_prompt"] = _to_code_block(prompt)
                result["model_output"] = _to_code_block(content)
            return result
        except Exception as e:
            logging.exception(
                "Shared env LLM health check failed",
                extra={"provider": provider, "model": model_name},
            )
            result = {
                "healthy": False,
                "provider": provider,
                "model": model_name,
                "error_code": "health_check_failed",
            }
            if include_details:
                result["test_prompt"] = _to_code_block(prompt)
                result["model_output"] = _to_code_block(None)
                result["error_detail"] = _to_code_block(str(e))
            return result

    # Check if API key is configured (except for Ollama)
    if config.provider != "ollama" and not config.api_key:
        return {
            "healthy": False,
            "provider": config.provider,
            "model": config.model,
            "error_code": "api_key_missing",
        }

    model_name = get_model_name(config)

    prompt = test_prompt or "Hi"

    try:
        # Make a minimal test call with timeout
        # Pass API key directly to avoid race conditions with global os.environ
        kwargs: dict[str, Any] = {
            "model": model_name,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 16,
            "api_key": config.api_key,
            "api_base": _normalize_api_base(config.provider, config.api_base),
            "timeout": LLM_TIMEOUT_HEALTH_CHECK,
        }
        reasoning_effort = _get_reasoning_effort(config.provider, model_name)
        if reasoning_effort:
            kwargs["reasoning_effort"] = reasoning_effort

        response = await litellm.acompletion(**kwargs)
        content = _extract_choice_text(response.choices[0])
        if not content:
            # Check if the model responded with reasoning/thinking content
            message = response.choices[0].message
            has_reasoning = getattr(message, "reasoning_content", None) or getattr(
                message, "thinking", None)
            if not has_reasoning:
                # LLM-003: Empty response should mark health check as unhealthy
                logging.warning(
                    "LLM health check returned empty content",
                    extra={"provider": config.provider, "model": config.model},
                )
                result: dict[str, Any] = {
                    "healthy": False,  # Fixed: empty content means unhealthy
                    "provider": config.provider,
                    "model": config.model,
                    "response_model": response.model if response else None,
                    "error_code": "empty_content",  # Changed from warning_code
                    "message": "LLM returned empty response",
                }
                if include_details:
                    result["test_prompt"] = _to_code_block(prompt)
                    result["model_output"] = _to_code_block(None)
                return result

        result = {
            "healthy": True,
            "provider": config.provider,
            "model": config.model,
            "response_model": response.model if response else None,
        }
        if include_details:
            result["test_prompt"] = _to_code_block(prompt)
            result["model_output"] = _to_code_block(content)
        return result
    except Exception as e:
        # Log full exception details server-side, but do not expose them to clients
        logging.exception(
            "LLM health check failed",
            extra={"provider": config.provider, "model": config.model},
        )

        # Provide a minimal, actionable client-facing hint without leaking secrets.
        error_code = "health_check_failed"
        message = str(e)
        if "404" in message and "/v1/v1/" in message:
            error_code = "duplicate_v1_path"
        elif "404" in message:
            error_code = "not_found_404"
        elif "<!doctype html" in message.lower() or "<html" in message.lower():
            error_code = "html_response"
        result = {
            "healthy": False,
            "provider": config.provider,
            "model": config.model,
            "error_code": error_code,
        }
        if include_details:
            result["test_prompt"] = _to_code_block(prompt)
            result["model_output"] = _to_code_block(None)
            result["error_detail"] = _to_code_block(message)
        return result


async def complete(
    prompt: str,
    system_prompt: str | None = None,
    config: LLMConfig | None = None,
    max_tokens: int = 4096,
    temperature: float = 0.7,
) -> str:
    """Make a completion request to the LLM.

    Transport retries (429, 500, timeout) are handled by the Router.
    """
    router, config = get_router(config)
    model_name = get_model_name(config)

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    if _shared_env_completion_is_enabled(config):
        try:
            content = await _complete_via_shared_responses(
                messages=messages,
                max_tokens=max_tokens,
            )
            if not content:
                raise ValueError("Empty response from shared env LLM")
            if "<think>" in content:
                content = _strip_thinking_tags(content)
                if not content:
                    raise ValueError("Response contained only thinking content, no output")
            return content
        except Exception as e:
            logging.error(
                f"Shared env LLM completion failed: {e}",
                extra={"model": _shared_env_model_name()},
            )
            raise ValueError(
                "LLM completion failed. Please check your API configuration and try again."
            ) from e

    try:
        kwargs: dict[str, Any] = {
            "model": "primary",
            "messages": messages,
            "max_tokens": max_tokens,
            "timeout": LLM_TIMEOUT_COMPLETION,
        }
        if _supports_temperature(config.provider, model_name):
            kwargs["temperature"] = temperature
        reasoning_effort = _get_reasoning_effort(config.provider, model_name)
        if reasoning_effort:
            kwargs["reasoning_effort"] = reasoning_effort

        response = await router.acompletion(**kwargs)

        content = _extract_choice_text(response.choices[0])
        if not content:
            raise ValueError("Empty response from LLM")
        # Strip thinking tags from reasoning models (deepseek-r1, qwq, etc.)
        if "<think>" in content:
            content = _strip_thinking_tags(content)
            if not content:
                raise ValueError("Response contained only thinking content, no output")
        return content
    except Exception as e:
        # Log the actual error server-side for debugging
        logging.error(f"LLM completion failed: {e}", extra={
                      "model": model_name})
        raise ValueError(
            "LLM completion failed. Please check your API configuration and try again."
        ) from e


def _supports_json_mode(model_name: str) -> bool:
    """Check if the model supports JSON mode via LiteLLM's model registry.

    Queries LiteLLM's model info for every provider (including openai,
    anthropic, etc.) so that capability is always determined from the
    registry rather than a hardcoded provider list.

    Ollama models support JSON mode natively (format="json") but are
    often not in LiteLLM's registry (custom/local models), so we
    always return True for ollama.

    Args:
        model_name: LiteLLM-formatted model name (from get_model_name).
    """
    # Ollama supports JSON mode natively via format="json" even when
    # models aren't in LiteLLM's registry (custom, quantized, etc.)
    if model_name.startswith(("ollama/", "ollama_chat/")):
        return True

    try:
        info = litellm.get_model_info(model=model_name)
        supported_params = info.get("supported_openai_params", [])
        return "response_format" in supported_params
    except Exception:
        # Model not in LiteLLM's registry — fall back to prompt-only JSON
        # mode (the system prompt already instructs "respond with valid JSON
        # only"). This avoids sending response_format to models that may
        # reject it.
        logging.debug("Model %s not in LiteLLM registry, skipping JSON mode", model_name)
        return False


def _appears_truncated(data: dict) -> bool:
    """LLM-001: Check if JSON data appears to be truncated.

    Detects suspicious patterns indicating incomplete responses.
    """
    if not isinstance(data, dict):
        return False

    # Check for empty arrays that should typically have content
    suspicious_empty_arrays = ["workExperience", "education", "skills"]
    for key in suspicious_empty_arrays:
        if key in data and data[key] == []:
            # Log warning - these are rarely empty in real resumes
            logging.warning(
                "Possible truncation detected: '%s' is empty",
                key,
            )
            return True

    # personalInfo is intentionally excluded: the improve prompts tell the LLM
    # to skip it, and _preserve_personal_info() restores it from the original.
    # Checking for it here caused 3 wasteful retry attempts on every request.

    return False


def _get_retry_temperature(attempt: int, base_temp: float = 0.1) -> float:
    """LLM-002: Get temperature for retry attempt - increases with each retry.

    Higher temperature on retries gives the model more variation to produce
    different (hopefully valid) output.
    """
    temperatures = [base_temp, 0.3, 0.5, 0.7]
    return temperatures[min(attempt, len(temperatures) - 1)]


def _calculate_timeout(
    operation: str,
    max_tokens: int = 4096,
    provider: str = "openai",
) -> int:
    """LLM-005: Calculate adaptive timeout based on operation and parameters."""
    base_timeouts = {
        "health_check": LLM_TIMEOUT_HEALTH_CHECK,
        "completion": LLM_TIMEOUT_COMPLETION,
        "json": LLM_TIMEOUT_JSON,
    }

    base = base_timeouts.get(operation, LLM_TIMEOUT_COMPLETION)

    # Scale by token count (relative to 4096 baseline)
    token_factor = max(1.0, max_tokens / 4096)

    # Provider-specific latency adjustments
    provider_factors = {
        "openai": 1.0,
        "anthropic": 1.2,
        "openrouter": 1.5,  # More variable latency
        "ollama": 2.0,  # Local models can be slower
    }
    provider_factor = provider_factors.get(provider, 1.0)

    return int(base * token_factor * provider_factor)


def _strip_thinking_tags(content: str) -> str:
    """Strip thinking/reasoning tags from model output.

    Ollama thinking models (deepseek-r1, qwq, etc.) wrap their reasoning
    in <think>...</think> tags. The actual answer follows after the closing
    tag. Strip these so JSON extraction finds the real output.
    """
    # Remove <think>...</think> blocks (including multiline)
    stripped = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL)
    # Also handle unclosed <think> tag (model may still be "thinking" at end)
    stripped = re.sub(r"<think>.*", "", stripped, flags=re.DOTALL)
    return stripped.strip()


def _extract_json(content: str, _depth: int = 0) -> str:
    """Extract JSON from LLM response, handling various formats.

    LLM-001: Improved to detect and reject likely truncated JSON.
    LLM-007: Improved error messages for debugging.
    JSON-010: Added recursion depth and size limits.
    """
    # JSON-010: Safety limits
    if _depth > MAX_JSON_EXTRACTION_RECURSION:
        raise ValueError(
            f"JSON extraction exceeded max recursion depth: {_depth}")
    if len(content) > MAX_JSON_CONTENT_SIZE:
        raise ValueError(
            f"Content too large for JSON extraction: {len(content)} bytes")

    original = content

    # Strip thinking model tags (deepseek-r1, qwq, etc.)
    if "<think>" in content:
        content = _strip_thinking_tags(content)

    # Remove markdown code blocks
    if "```json" in content:
        content = content.split("```json")[1].split("```")[0]
    elif "```" in content:
        parts = content.split("```")
        if len(parts) >= 2:
            content = parts[1]
            # Remove language identifier if present (e.g., "json\n{...")
            if content.startswith(("json", "JSON")):
                content = content[4:]

    content = content.strip()

    # If content starts with {, find the matching }
    if content.startswith("{"):
        depth = 0
        end_idx = -1
        in_string = False
        escape_next = False

        for i, char in enumerate(content):
            if escape_next:
                escape_next = False
                continue
            if char == "\\":
                escape_next = True
                continue
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            if in_string:
                continue
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    end_idx = i
                    break

        # LLM-001: Check for unbalanced braces - loop ended without depth reaching 0
        if end_idx == -1 and depth != 0:
            logging.warning(
                "JSON extraction found unbalanced braces (depth=%d), possible truncation",
                depth,
            )

        if end_idx != -1:
            return content[: end_idx + 1]

    # Try to find JSON object in the content (only if not already at start)
    start_idx = content.find("{")
    if start_idx > 0:
        # Only recurse if { is found after position 0 to avoid infinite recursion
        return _extract_json(content[start_idx:], _depth + 1)

    # LLM-007: Log unrecognized format for debugging
    logging.error(
        "Could not extract JSON from response format. Content preview: %s",
        content[:200] if content else "<empty>",
    )
    raise ValueError(f"No JSON found in response: {original[:200]}")


async def complete_json(
    prompt: str,
    system_prompt: str | None = None,
    config: LLMConfig | None = None,
    max_tokens: int = 4096,
    retries: int = 2,
) -> dict[str, Any]:
    """Make a completion request expecting JSON response.

    Uses JSON mode when available, with app-level retries for content-quality
    issues (malformed JSON, truncation).  Transport retries (429, 500, timeout)
    are handled by the Router and are NOT retried again here.
    """
    router, config = get_router(config)
    model_name = get_model_name(config)

    # Build messages
    json_system = (
        system_prompt or ""
    ) + "\n\nYou must respond with valid JSON only. No explanations, no markdown."
    messages = [
        {"role": "system", "content": json_system},
        {"role": "user", "content": prompt},
    ]

    # Check if we can use JSON mode
    use_json_mode = _supports_json_mode(model_name)
    use_shared_env = _shared_env_completion_is_enabled(config)

    for attempt in range(retries + 1):
        try:
            if use_shared_env:
                content = await _complete_via_shared_responses(
                    messages=messages,
                    max_tokens=max_tokens,
                )
            else:
                kwargs: dict[str, Any] = {
                    "model": "primary",
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "timeout": _calculate_timeout("json", max_tokens, config.provider),
                }
                if _supports_temperature(config.provider, model_name):
                    # LLM-002: Increase temperature on retry for variation
                    kwargs["temperature"] = _get_retry_temperature(attempt)
                reasoning_effort = _get_reasoning_effort(
                    config.provider, model_name)
                if reasoning_effort:
                    kwargs["reasoning_effort"] = reasoning_effort

                # Add JSON mode if supported
                if use_json_mode:
                    kwargs["response_format"] = {"type": "json_object"}

                response = await router.acompletion(**kwargs)
                content = _extract_choice_text(response.choices[0])

            if not content:
                raise ValueError("Empty response from LLM")

            logging.debug(
                f"LLM response (attempt {attempt + 1}): {content[:300]}")

            # Extract and parse JSON
            json_str = _extract_json(content)
            result = json.loads(json_str)

            # LLM-001: Check if parsed result appears truncated
            if isinstance(result, dict) and _appears_truncated(result):
                if attempt < retries:
                    logging.warning(
                        "Parsed JSON appears truncated (attempt %d/%d), retrying",
                        attempt + 1,
                        retries + 1,
                    )
                    messages[-1]["content"] = (
                        prompt
                        + "\n\nIMPORTANT: Output the COMPLETE JSON object with ALL sections including personalInfo. Do not truncate."
                    )
                    continue
                logging.warning(
                    "Parsed JSON appears truncated on final attempt, proceeding with result"
                )

            return result

        except json.JSONDecodeError as e:
            # Content quality — malformed JSON, retry with prompt hint
            logging.warning(f"JSON parse failed (attempt {attempt + 1}): {e}")
            if attempt < retries:
                messages[-1]["content"] = (
                    prompt
                    + "\n\nIMPORTANT: Output ONLY a valid JSON object. Start with { and end with }."
                )
                continue
            raise ValueError(
                f"Failed to parse JSON after {retries + 1} attempts: {e}")

        except ValueError as e:
            # Content quality — empty response, JSON extraction failure
            logging.warning(f"Content extraction failed (attempt {attempt + 1}): {e}")
            if attempt < retries:
                continue
            raise

        except Exception:
            # Transport errors — Router already retried with backoff.
            # Cooldowns are disabled (see _build_router); no additional
            # retry is attempted here.
            raise

    raise ValueError(f"Failed after {retries + 1} attempts")
