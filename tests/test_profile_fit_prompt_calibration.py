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

    assert 'Adjacent BI/analyst roles are only credible' in candidate_summary['summary']
    assert 'Backend Engineer (Python, data/workflow-oriented)' in candidate_summary['target_roles']
    assert 'Python Developer (data/workflow-oriented)' in candidate_summary['target_roles']

    prompt = fitting_notifier._build_fit_prompt(
        'Software Engineer',
        'Generic software role with some data mentions.',
        'Resume text',
        candidate_summary,
        prompt_text=profile['fit_prompt'],
    )

    assert 'Adjacent BI/analyst roles are only credible' in candidate_summary['summary']
    assert 'Pure dashboarding, business-insights, marketing analytics, or stakeholder-reporting roles should usually be Weak Fit' in prompt
    assert 'dashboarding, stakeholder reporting, campaign/eCommerce insights, or BI tooling support' in prompt


def test_george_profile_prompt_requires_explicit_backend_or_data_centrality_for_generic_titles():
    profile = _load_profile('George Gu')
    candidate_summary = profile['candidate_summary']

    assert 'Generic software or full-stack roles are only credible when backend/API ownership is clearly central' in candidate_summary['summary']
    assert 'Software Engineer (backend/API-focused)' in candidate_summary['target_roles']
    assert 'Integration Engineer (implementation-focused)' in candidate_summary['target_roles']

    prompt = fitting_notifier._build_fit_prompt(
        'Software Engineer 1',
        'Broad early-career posting with frontend, backend, and mobile options.',
        'Resume text',
        candidate_summary,
        prompt_text=profile['fit_prompt'],
    )

    assert 'Software Engineer 1, Graduate Software Engineer, or New Grad Software Engineer' in prompt
    assert 'broad rotation/team-placement language should default to Weak Fit' in prompt
    assert 'default to Weak Fit unless backend/API scope is explicit and central' in prompt
