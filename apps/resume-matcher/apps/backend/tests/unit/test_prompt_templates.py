"""Unit tests for shared prompt template guidance."""

from app.prompts.templates import (
    COVER_LETTER_PROMPT,
    DIFF_IMPROVE_PROMPT,
    DUTCH_STYLE_GUIDANCE,
    IMPROVE_RESUME_PROMPT_FULL,
    IMPROVE_RESUME_PROMPT_KEYWORDS,
    IMPROVE_RESUME_PROMPT_NUDGE,
)


def test_all_resume_prompt_surfaces_include_shared_dutch_style_guidance():
    prompts = [
        DIFF_IMPROVE_PROMPT,
        IMPROVE_RESUME_PROMPT_NUDGE,
        IMPROVE_RESUME_PROMPT_KEYWORDS,
        IMPROVE_RESUME_PROMPT_FULL,
    ]

    for prompt in prompts:
        assert DUTCH_STYLE_GUIDANCE in prompt


def test_cover_letter_prompt_includes_shared_dutch_style_guidance():
    assert DUTCH_STYLE_GUIDANCE in COVER_LETTER_PROMPT


def test_resume_prompt_surfaces_preserve_output_language_placeholder():
    prompts = [
        DIFF_IMPROVE_PROMPT,
        IMPROVE_RESUME_PROMPT_NUDGE,
        IMPROVE_RESUME_PROMPT_KEYWORDS,
        IMPROVE_RESUME_PROMPT_FULL,
    ]

    for prompt in prompts:
        assert "{output_language}" in prompt


def test_cover_letter_prompt_preserves_plain_text_output_contract():
    assert "{output_language}" in COVER_LETTER_PROMPT
    assert "Output plain text only." in COVER_LETTER_PROMPT
