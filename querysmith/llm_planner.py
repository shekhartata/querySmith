from __future__ import annotations

import json
from typing import Any

import httpx

from querysmith.config import Settings
from querysmith.models import LLMSuggestion, QueryInput, RuleFinding, SourceInfo, TruthBundle, ViewFlattenSuggestion


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


def _view_flatten_system_prompt(trigger: str) -> str:
    if trigger == "source_timeout":
        return (
            "You are a MongoDB view optimization expert. The user's aggregation query against a view "
            "timed out or ran unacceptably slowly. You are given:\n"
            "- The view's pipeline definition (the stages that define the view).\n"
            "- The user's query pipeline (the stages they ran on top of the view).\n"
            "- The flattened pipeline (view stages + user stages concatenated), targeting the base collection.\n"
            "- Schema field types and rule findings for context.\n\n"
            "Your job: produce a PRUNED version of the flattened pipeline that runs directly against the "
            "base collection. Remove view stages that do NOT contribute fields or filters needed by the "
            "user's downstream stages. Keep all user stages intact unless you can optimize them too. "
            "The pruned pipeline MUST produce semantically equivalent results.\n\n"
            "Respond with JSON only: {\"suggested_pipeline\": [...], \"rationale\": \"...\", "
            "\"confidence\": 0.0-1.0, \"risks\": [\"...\"]}"
        )
    return (
        "You are a MongoDB view optimization expert. A $lookup stage in the user's pipeline targets "
        "a view and is identified as slow in the explain plan. You are given:\n"
        "- The original user pipeline.\n"
        "- The slow $lookup's stage index and the view it targets.\n"
        "- The view's pipeline definition and the base collection it reads from.\n"
        "- Schema field types and rule findings for context.\n\n"
        "Your job: rewrite the user's pipeline so that the slow $lookup targets the BASE COLLECTION "
        "instead of the view, inlining only the NECESSARY view stages as a sub-pipeline within the "
        "$lookup. Strip view stages that don't contribute to the fields the lookup actually needs.\n\n"
        "Respond with JSON only: {\"suggested_pipeline\": [...], \"rationale\": \"...\", "
        "\"confidence\": 0.0-1.0, \"risks\": [\"...\"]}"
    )


def suggest_view_flatten(
    settings: Settings,
    trigger: str,
    original_source: str,
    base_collection: str,
    view_chain: list[str],
    view_pipeline: list[dict[str, Any]],
    user_pipeline: list[dict[str, Any]],
    flattened_pipeline: list[dict[str, Any]],
    truth: TruthBundle,
    findings: list[RuleFinding],
    explain_stats: dict[str, Any],
    lookup_stage_index: int | None = None,
) -> ViewFlattenSuggestion:
    """Ask LLM to prune a flattened view+user pipeline (or inline a view into a $lookup)."""
    base = ViewFlattenSuggestion(
        trigger=trigger,
        original_source=original_source,
        base_collection=base_collection,
        view_chain=view_chain,
        view_stage_count=len(view_pipeline),
        user_stage_count=len(user_pipeline),
        flattened_pipeline=flattened_pipeline,
        lookup_stage_index=lookup_stage_index,
        lookup_from=original_source if trigger == "slow_lookup" else None,
    )

    if not settings.openai_api_key:
        base.rationale = "LLM not available; review the flattened pipeline manually and prune unnecessary view stages."
        base.risks = ["LLM planner disabled — manual review required."]
        return base

    payload = {
        "trigger": trigger,
        "original_source": original_source,
        "base_collection": base_collection,
        "view_chain": view_chain,
        "view_pipeline": view_pipeline,
        "user_pipeline": user_pipeline,
        "flattened_pipeline": flattened_pipeline,
        "view_stage_count": len(view_pipeline),
        "user_stage_count": len(user_pipeline),
        "field_types": [f.model_dump() for f in truth.field_types[:200]],
        "indexes": truth.indexes[:50],
        "rule_findings": [f.model_dump() for f in findings],
        "explain_stats": explain_stats,
        "lookup_stage_index": lookup_stage_index,
    }

    body = {
        "model": settings.llm_model,
        "messages": [
            {"role": "system", "content": _view_flatten_system_prompt(trigger)},
            {"role": "user", "content": json.dumps(payload, default=str)},
        ],
        "response_format": {"type": "json_object"},
    }

    api_base = settings.openai_base_url or "https://api.openai.com/v1"
    url = api_base.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {settings.openai_api_key}"}
    try:
        with httpx.Client(timeout=120.0) as client:
            r = client.post(url, headers=headers, json=body)
            r.raise_for_status()
            data = r.json()
        content = data["choices"][0]["message"].get("content") or "{}"
        parsed = json.loads(content)
        base.suggested_pipeline = parsed.get("suggested_pipeline")
        base.rationale = parsed.get("rationale") or ""
        base.confidence = float(parsed.get("confidence") or 0.0)
        base.risks = list(parsed.get("risks") or [])
    except Exception as e:
        base.rationale = f"LLM call failed: {e}"
        base.risks = [str(e)]
    return base
