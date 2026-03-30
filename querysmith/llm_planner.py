from __future__ import annotations

import json
from typing import Any

import httpx

from querysmith.config import Settings
from querysmith.models import LLMSuggestion, QueryInput, RuleFinding, SourceInfo, TruthBundle


def _system_prompt() -> str:
    return (
        "You are a MongoDB aggregation optimizer. You must follow the user's playbook: "
        "fix types, expand views conceptually, push $match early, simplify lookups, "
        "reduce projection width, control unwind/group explosion, add pagination if unbounded, "
        "and only then consider indexes. "
        "If rule SCH-04 is present, the pipeline uses a literal whose JSON type does not match sampled field types "
        "(e.g. string vs int): fix the literal to the correct BSON type first; do not replace with $type checks "
        "unless type coercion is explicitly required. "
        "If rule RUN-01 is present, the timed run returned 0 rows: use the sampled example values in the finding "
        "or fix later pipeline stages. "
        "Respond with JSON only, matching the requested schema. "
        "If you cannot improve safely, set suggested_pipeline to null and explain in risks."
    )


def _user_payload(
    q: QueryInput,
    source: SourceInfo,
    truth: TruthBundle,
    findings: list[RuleFinding],
    explain_stats: dict[str, Any],
    environment: str,
) -> str:
    payload = {
        "database": q.database,
        "source": q.source,
        "mode": q.mode,
        "environment": environment,
        "source_kind": source.kind,
        "view_pipeline": source.view_pipeline,
        "field_types": [f.model_dump() for f in truth.field_types[:200]],
        "indexes": truth.indexes[:50],
        "rule_findings": [f.model_dump() for f in findings],
        "explain_execution_stats": explain_stats,
        "original": {
            "pipeline": q.pipeline,
            "filter": q.filter,
            "projection": q.projection,
            "sort": q.sort,
            "limit": q.limit,
        },
    }
    return json.dumps(payload, default=str)


def suggest_rewrite(
    settings: Settings,
    q: QueryInput,
    source: SourceInfo,
    truth: TruthBundle,
    findings: list[RuleFinding],
    explain_stats: dict[str, Any],
    environment: str,
) -> LLMSuggestion:
    if not settings.openai_api_key:
        return LLMSuggestion(
            skipped_reason="OPENAI_API_KEY not set; configure for LLM-assisted rewrite.",
            rationale="",
            confidence=0.0,
            risks=["LLM planner disabled"],
        )

    body = {
        "model": settings.llm_model,
        "messages": [
            {"role": "system", "content": _system_prompt()},
            {
                "role": "user",
                "content": _user_payload(q, source, truth, findings, explain_stats, environment)
                + "\n\nReturn JSON with keys: suggested_pipeline (array or null), suggested_find (object or null), "
                "rationale (string), confidence (0-1 number), risks (string array), expected_gain (string).",
            },
        ],
        "response_format": {"type": "json_object"},
    }

    base = settings.openai_base_url or "https://api.openai.com/v1"
    url = base.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {settings.openai_api_key}"}
    try:
        with httpx.Client(timeout=120.0) as client:
            r = client.post(url, headers=headers, json=body)
            r.raise_for_status()
            data = r.json()
        content = data["choices"][0]["message"].get("content") or "{}"
        parsed = json.loads(content)
        return LLMSuggestion(
            suggested_pipeline=parsed.get("suggested_pipeline"),
            suggested_find=parsed.get("suggested_find"),
            rationale=parsed.get("rationale") or "",
            confidence=float(parsed.get("confidence") or 0.0),
            risks=list(parsed.get("risks") or []),
            expected_gain=parsed.get("expected_gain") or "",
        )
    except Exception as e:
        return LLMSuggestion(
            skipped_reason=f"LLM call failed: {e}",
            rationale="",
            confidence=0.0,
            risks=[str(e)],
        )
