"""Service tests for cover letter prompt generation."""

from unittest.mock import AsyncMock, patch


class TestGenerateCoverLetter:
    @patch("app.services.cover_letter.complete", new_callable=AsyncMock)
    async def test_cover_letter_prompt_includes_dutch_style_guidance(
        self, mock_complete, sample_resume, sample_job_description
    ):
        from app.services.cover_letter import generate_cover_letter

        mock_complete.return_value = "Tailored cover letter"

        result = await generate_cover_letter(
            sample_resume,
            sample_job_description,
            language="en",
        )

        assert result == "Tailored cover letter"
        prompt = mock_complete.call_args.kwargs.get("prompt") or mock_complete.call_args.args[0]
        assert "dutch companies and dutch hr review" in prompt.lower()
        assert "output plain text only" in prompt.lower()
        assert "write in english" in prompt.lower()
