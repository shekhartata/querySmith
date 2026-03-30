from __future__ import annotations

import json
from typing import Any

from querysmith.models import LLMSuggestion, RuleFinding, V1Report


def build_v1_report(
    *,
    issue_summary: str,
    bottlenecks: list[str],
    findings: list[RuleFinding],
    explain_interpretation: str,
    suggestion: LLMSuggestion | None,
    index_recs: list[Any],
    risk_notes: list[str],
    confidence_rating: str,
    environment: str,
    original_query_summary: str,
    metadata: dict[str, Any],
) -> V1Report:
    from querysmith.models import IndexRecommendation

    idx_models: list[IndexRecommendation] = []
    for r in index_recs:
        if isinstance(r, IndexRecommendation):
            idx_models.append(r)
        elif isinstance(r, dict):
            idx_models.append(IndexRecommendation(**r))

    return V1Report(
        issue_summary=issue_summary,
        suspected_bottlenecks=bottlenecks,
        rule_violations=findings,
        explain_interpretation=explain_interpretation,
        optimized_candidate=suggestion,
        index_recommendations=idx_models,
        risk_notes=risk_notes,
        confidence_rating=confidence_rating,
        environment=environment,
        original_query_summary=original_query_summary,
        metadata=metadata,
    )


def report_to_markdown(report: V1Report) -> str:
    lines: list[str] = []
    lines.append("## QuerySmith V1 — Optimization memo\n")
    lines.append(f"**Environment:** {report.environment}\n")
    lines.append(f"**Confidence:** {report.confidence_rating}\n")
    lines.append("\n### Executive summary\n")
    lines.append(report.issue_summary + "\n")
    ns = report.metadata.get("namespace")
    if isinstance(ns, dict):
        lines.append("\n### Namespace / sampling\n")
        lines.append(
            f"- **database:** `{ns.get('database')}`  **source:** `{ns.get('source')}`\n"
            f"- **listed in DB:** {ns.get('namespace_found')} "
            f"  **sampled docs:** {ns.get('sample_doc_count')} "
            f"  **estimated count:** {ns.get('approximate_document_count')}\n"
        )
    lines.append("\n### Suspected bottlenecks\n")
    for b in report.suspected_bottlenecks:
        lines.append(f"- {b}\n")
    lines.append("\n### Rule findings\n")
    for f in report.rule_violations:
        lines.append(f"- **[{f.severity.upper()}] {f.rule_id}** ({f.category}): {f.message}\n")
    lines.append("\n### Explain / runtime\n")
    lines.append(report.explain_interpretation + "\n")
    lines.append("\n### Suggested candidate\n")
    sug = report.optimized_candidate
    if sug:
        if sug.skipped_reason:
            lines.append(f"_LLM:_ {sug.skipped_reason}\n")
        else:
            lines.append(f"_Rationale:_ {sug.rationale}\n")
            lines.append(f"_Expected gain:_ {sug.expected_gain}\n")
            lines.append(f"_Confidence:_ {sug.confidence}\n")
            for r in sug.risks:
                lines.append(f"- risk: {r}\n")
            if sug.suggested_pipeline:
                lines.append("\n```json\n")
                lines.append(json.dumps(sug.suggested_pipeline, indent=2, default=str))
                lines.append("\n```\n")
            if sug.suggested_find:
                lines.append("\n**Suggested find filter:**\n\n```json\n")
                lines.append(json.dumps(sug.suggested_find, indent=2, default=str))
                lines.append("\n```\n")
    else:
        lines.append("_No suggestion generated._\n")
    lines.append("\n### Index recommendations\n")
    for ix in report.index_recommendations:
        lines.append(f"- keys `{ix.keys}` — {ix.rationale}\n")
    lines.append("\n### Risk notes\n")
    for r in report.risk_notes:
        lines.append(f"- {r}\n")
    lines.append("\n### Original query (summary)\n")
    lines.append(report.original_query_summary + "\n")
    return "".join(lines)
