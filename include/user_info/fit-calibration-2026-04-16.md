# Job-fit calibration review — 2026-04-16

Source of truth: `/Users/levi/Linkedin-notifier/.omx/specs/deep-interview-job-prompt-experience-filtering.md`

## Scope

This review documents the prompt-calibration boundaries for the 2026-04-16 experience-filtering pass.
It exists to keep future edits anchored to the same rules used for this tuning round.

## Current evidence snapshot

Rolling last-7-day notified-slice review findings used for this pass:

- `George Gu`: **159 `Moderate Fit`** notifications in the slice; the noisiest groups were **68 generic software/developer roles** plus **24 generic graduate/intern roles**.
- `Xingyou Li`: **119 `Moderate Fit`** notifications in the slice; the noisiest groups were **28 analyst/BI roles** plus **5 generic software/developer roles**.
- `Strong Fit` looked mostly junior/data-aligned and was treated as a control bucket, not the main tightening target.

The practical implication is that this calibration round should mostly improve **`Moderate Fit` precision** while leaving `Strong Fit` behavior largely intact unless new DB evidence shows a systematic problem.

## Guardrails for prompt tuning

1. **`include/user_info/profiles.json` is the source of truth.** Runtime sync should copy changes into Postgres; do not treat DB-edited prompt text as canonical.
2. **Use `fit_prompt` as the primary lever.** Only make small `candidate_summary` edits when they stay resume-grounded and clarify the baseline rather than weakening the candidate.
3. **Protect junior / plausible-mid recall.** Do not solve noise by globally scoring down ambiguous jobs.
4. **Downgrade aggressively only on explicit blocker signals**, especially:
   - explicit `4+` / `5+` years requirements,
   - explicit `Senior` / `Lead` / `Staff` / `Principal` / `Architect` / `Manager` signals,
   - explicit Dutch requirements,
   - clear ownership / architecture / people-leadership scope.
5. **Keep `Strong Fit` mostly stable.** Tightening should concentrate on `Moderate Fit` wording and decision discipline.

## Profile-specific review notes

### Xingyou Li

- Preserve inclusion for junior and plausible-mid **data engineering / analytics engineering / Python-heavy backend-data workflow** roles.
- Be skeptical of `Moderate Fit` promotions for analyst/BI work that lacks clear pipeline or engineering depth.
- Generic software/developer titles need concrete junior data/backend workflow evidence before they should remain above `Weak Fit`.

### George Gu

- Preserve inclusion for junior-to-mid **backend/data/system** roles where backend ownership is clearly central.
- Be stricter on vague generic software/developer titles because this profile's search surface is much broader.
- Generic graduate/intern titles should not be promoted on keyword overlap alone unless the actual responsibilities are clearly backend/data aligned.
- Frontend-heavy or design-heavy full-stack roles should remain heavily penalized.

## Review checklist for future edits

When changing profile prompts again, re-check all of the following against a fresh last-7-day notified slice:

- `Moderate Fit` still means “worth a serious screening call / possible apply,” not merely “adjacent on keywords”.
- Junior or plausible-mid roles are not disappearing because of vague seniority inference.
- Analyst/BI noise for Xingyou and generic software/developer noise for George do not drift back upward.
- `Strong Fit` volume and quality remain broadly stable unless there is explicit evidence to tighten it.

## Code-quality note

The clean ownership boundary in this area is:

- prompt + baseline config: `include/user_info/profiles.json`
- prompt assembly and placeholder normalization: `dags/fitting_notifier.py` and `dags/database.py`
- policy/regression coverage: `tests/test_fitting_notifier.py`, `tests/test_fitting_notifier_policy.py`, `tests/test_database_queue_semantics.py`

Keep those responsibilities separate; avoid duplicating prompt policy in multiple sources.
