"""Unit tests for shared-env LLM fallback."""

import json
from io import BytesIO
from unittest.mock import AsyncMock, patch

from app.llm import (
    LLMConfig,
    _request_shared_responses_text,
    _request_shared_responses_text_with_fallback,
    _serialize_shared_responses_input,
    check_llm_health,
    complete,
    complete_json,
)


def _missing_key_config() -> LLMConfig:
    return LLMConfig(
        provider="openai",
        model="gpt-5-nano-2025-08-07",
        api_key="",
        api_base=None,
    )


class TestSharedEnvFallback:
    def test_shared_env_input_serialization_keeps_system_text_but_avoids_message_array(self):
        serialized = _serialize_shared_responses_input(
            [
                {"role": "system", "content": "Return valid JSON only."},
                {"role": "user", "content": "Parse this resume."},
            ]
        )

        assert serialized == "Return valid JSON only.\n\nParse this resume."

    @patch("app.llm.urllib_request.urlopen")
    def test_shared_env_request_sends_flat_string_input(self, mock_urlopen):
        class _DummyResponse(BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        mock_urlopen.return_value = _DummyResponse(
            json.dumps({"output_text": '{"ok": true}'}).encode("utf-8")
        )

        result = _request_shared_responses_text(
            request_url="https://nc.example.test/v1/responses",
            api_key="test-key",
            model_name="gpt-5.4",
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": "Parse this resume."},
            ],
            max_tokens=256,
        )

        assert result == '{"ok": true}'
        request = mock_urlopen.call_args.args[0]
        payload = json.loads(request.data.decode("utf-8"))
        assert payload["input"] == "Return JSON only.\n\nParse this resume."
        assert isinstance(payload["input"], str)

    @patch("app.llm._request_shared_responses_text")
    @patch(
        "app.llm._parse_shared_llm_endpoints",
        return_value=[
            {
                "name": "nc",
                "request_url": "https://nc.example.test/v1/responses",
                "api_key": "nc-key",
            },
            {
                "name": "yuan",
                "request_url": "https://yuan.example.test/v1/responses",
                "api_key": "yuan-key",
                "model": "glm-5.1",
            },
        ],
    )
    def test_shared_env_fallback_starts_from_first_endpoint_every_call(
        self,
        _mock_endpoints,
        mock_request,
    ):
        calls: list[tuple[str, str]] = []

        def side_effect(*, request_url, api_key, model_name, messages, max_tokens):
            calls.append((request_url, model_name))
            if request_url == "https://nc.example.test/v1/responses":
                raise RuntimeError("temporary nc failure")
            return '{"ok": true}'

        mock_request.side_effect = side_effect

        first = _request_shared_responses_text_with_fallback(
            messages=[{"role": "user", "content": "Return JSON."}],
            model_name="gpt-5.4",
            max_tokens=256,
        )
        second = _request_shared_responses_text_with_fallback(
            messages=[{"role": "user", "content": "Return JSON."}],
            model_name="gpt-5.4",
            max_tokens=256,
        )

        assert first == '{"ok": true}'
        assert second == '{"ok": true}'
        assert calls == [
            ("https://nc.example.test/v1/responses", "gpt-5.4"),
            ("https://yuan.example.test/v1/responses", "glm-5.1"),
            ("https://nc.example.test/v1/responses", "gpt-5.4"),
            ("https://yuan.example.test/v1/responses", "glm-5.1"),
        ]

    @patch("app.llm._request_shared_responses_text")
    @patch(
        "app.llm._parse_shared_llm_endpoints",
        return_value=[
            {
                "name": "yuan",
                "request_url": "https://example.test/v1/responses",
                "api_key": "test-key",
                "model": "glm-5.1",
            }
        ],
    )
    def test_shared_env_fallback_uses_endpoint_model_override(
        self,
        _mock_endpoints,
        mock_request,
    ):
        mock_request.return_value = '{"ok": true}'

        result = _request_shared_responses_text_with_fallback(
            messages=[{"role": "user", "content": "Return JSON."}],
            model_name="gpt-5.4",
            max_tokens=256,
        )

        assert result == '{"ok": true}'
        mock_request.assert_called_once_with(
            request_url="https://example.test/v1/responses",
            api_key="test-key",
            model_name="glm-5.1",
            messages=[{"role": "user", "content": "Return JSON."}],
            max_tokens=256,
        )

    @patch("app.llm._complete_via_shared_responses", new_callable=AsyncMock)
    @patch("app.llm._shared_env_completion_is_enabled", return_value=True)
    async def test_complete_json_uses_shared_env_fallback(
        self,
        _mock_enabled,
        mock_complete,
    ):
        mock_complete.return_value = '{"ok": true}'

        result = await complete_json(
            "Return JSON.",
            config=_missing_key_config(),
            retries=0,
        )

        assert result == {"ok": True}
        mock_complete.assert_awaited_once()

    @patch("app.llm._complete_via_shared_responses", new_callable=AsyncMock)
    @patch("app.llm._shared_env_completion_is_enabled", return_value=True)
    async def test_complete_uses_shared_env_fallback(
        self,
        _mock_enabled,
        mock_complete,
    ):
        mock_complete.return_value = "fallback text"

        result = await complete(
            "Hello",
            config=_missing_key_config(),
        )

        assert result == "fallback text"
        mock_complete.assert_awaited_once()

    @patch("app.llm._shared_env_model_name", return_value="gpt-5.4")
    @patch("app.llm._complete_via_shared_responses", new_callable=AsyncMock)
    @patch("app.llm._shared_env_completion_is_enabled", return_value=True)
    async def test_health_check_uses_shared_env_fallback(
        self,
        _mock_enabled,
        mock_complete,
        _mock_model_name,
    ):
        mock_complete.return_value = "ok"

        result = await check_llm_health(config=_missing_key_config())

        assert result["healthy"] is True
        assert result["provider"] == "shared-env-responses"
        assert result["model"] == "gpt-5.4"
        mock_complete.assert_awaited_once()
