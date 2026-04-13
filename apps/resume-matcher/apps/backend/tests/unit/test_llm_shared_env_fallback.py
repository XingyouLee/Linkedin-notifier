"""Unit tests for shared-env LLM fallback."""

from unittest.mock import AsyncMock, patch

from app.llm import LLMConfig, check_llm_health, complete, complete_json


def _missing_key_config() -> LLMConfig:
    return LLMConfig(
        provider="openai",
        model="gpt-5-nano-2025-08-07",
        api_key="",
        api_base=None,
    )


class TestSharedEnvFallback:
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
