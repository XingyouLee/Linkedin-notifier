import json
from pathlib import Path

from dags import fitting_notifier


PROFILES_PATH = Path(__file__).resolve().parents[1] / 'include' / 'user_info' / 'profiles.json'


def _load_profile(profile_key: str) -> dict:
    profiles = json.loads(PROFILES_PATH.read_text(encoding='utf-8'))
    return next(profile for profile in profiles if profile['profile_key'] == profile_key)


def test_xingyou_profile_prompt_tightens_moderate_precision_without_killing_light_mid_roles():
    profile = _load_profile('Xingyou Li')
    candidate_summary = profile['candidate_summary']

    assert 'Plausible on light-mid roles only when pipeline, SQL/warehouse, and scoped execution are clearly central.' in candidate_summary['summary']
    assert 'Backend Engineer (Python, data workflow)' in candidate_summary['target_roles']
    assert 'Python Developer (data workflow)' in candidate_summary['target_roles']

    prompt = fitting_notifier._build_fit_prompt(
        'Software Engineer',
        'Generic software role with some data mentions.',
        'Resume text',
        candidate_summary,
        prompt_text=profile['fit_prompt'],
    )

    assert 'MODERATE PRECISION GUARDRAILS' in prompt
    assert 'Analyst, support, BI, and consulting-flavored data roles are only Moderate when hands-on pipeline' in prompt
    assert 'Protect recall for true junior or light-mid data roles' in prompt


def test_george_profile_prompt_requires_explicit_backend_or_data_centrality_for_generic_titles():
    profile = _load_profile('George Gu')
    candidate_summary = profile['candidate_summary']

    assert 'Plausible for generic software engineer titles only when backend or data responsibilities are explicit and clearly dominant.' in candidate_summary['summary']
    assert 'Software Engineer (backend/data-leaning only)' in candidate_summary['target_roles']
    assert 'Platform/Data Workflow Engineer' in candidate_summary['target_roles']

    prompt = fitting_notifier._build_fit_prompt(
        'Software Engineer 1',
        'Broad early-career posting with frontend, backend, and mobile options.',
        'Resume text',
        candidate_summary,
        prompt_text=profile['fit_prompt'],
    )

    assert 'Frontend-heavy, product-generalist, or broad full-stack roles are weak-fit by default' in prompt
    assert 'Generic Software Engineer, Developer, or Full Stack titles should usually stay Weak Fit' in prompt
    assert 'Protect recall for plausible junior-to-mid backend/data roles' in prompt
