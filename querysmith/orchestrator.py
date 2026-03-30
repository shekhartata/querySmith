from __future__ import annotations

import json
from typing import Any

from querysmith.config import Settings, load_settings
from querysmith.llm_planner import suggest_rewrite
from querysmith.models import QueryInput, V1Report
from querysmith.mongo_client import MongoService
from querysmith.pipeline_parse import extract_match_equality_literals, normalize_aggregate
from querysmith.report_builder import build_v1_report, report_to_markdown
from querysmith.rules_engine import (
    analyze_aggregate,
    analyze_find,
    findings_for_zero_timed_run,
    index_recommendations_from_find,
    index_recommendations_from_pipeline,
)


def _interpret_explain(stats: dict[str, Any], timed_ms: float | None, returned: int | None) -> str:
    parts = []
    for k, label in (
        ("executionTimeMillis", "executionTimeMillis (explain)"),
        ("totalDocsExamined", "totalDocsExamined"),
        ("totalKeysExamined", "totalKeysExamined"),
        ("nReturned", "nReturned"),
    ):
        if k in stats:
            parts.append(f"- {label}: {stats[k]}")
    if timed_ms is not None:
        parts.append(f"- timed run wall clock (ms): {timed_ms:.2f}")
    if returned is not None:
        parts.append(f"- documents returned (timed run): {returned}")
    return "\n".join(parts) if parts else "No executionStats extracted; inspect raw explain in metadata."


def _bottlenecks_from(findings: list[Any], stats: dict[str, Any]) -> list[str]:
    out: list[str] = []
    docs = stats.get("totalDocsExamined")
    if isinstance(docs, int) and docs > 10_000:
        out.append(f"High documents examined ({docs}) — check selectivity and indexes.")
    keys = stats.get("totalKeysExamined")
    if isinstance(keys, int) and isinstance(docs, int) and docs > 0 and keys < docs / 10:
        out.append("Low keys examined vs docs — candidate collection scan or inefficient plan.")
    for f in findings:
        if f.severity == "warn" and f.category in ("ordering", "lookup", "index", "source"):
            out.append(f"{f.rule_id}: {f.message}")
    return out[:12]


def run_v1(
    q: QueryInput,
    settings: Settings | None = None,
) -> tuple[V1Report, str]:
    settings = settings or load_settings()
    max_ms = q.max_time_ms or settings.default_timeout_ms
    mongo = MongoService(settings)
    try:
        src = mongo.resolve_source(q.database, q.source)
        truth = mongo.build_truth_bundle(q.database, q.source, src)

        if q.mode == "aggregate":
            pipeline = normalize_aggregate(q.pipeline or [])
            explain = mongo.run_explain(
                q.database,
                q.source,
                "aggregate",
                pipeline,
                None,
                None,
                None,
            )
            timed = mongo.run_aggregate_timed(q.database, q.source, pipeline, max_ms)
            findings = analyze_aggregate(pipeline, truth, src, settings.max_pipeline_stages_warn)
            index_recs = index_recommendations_from_pipeline(pipeline, truth, findings)
            original_summary = json.dumps({"mode": "aggregate", "pipeline": pipeline}, default=str)[:8000]
            _pipeline_for_literals = pipeline
        else:
            filt = q.filter or {}
            sort_pairs = q.sort
            explain = mongo.run_explain(
                q.database,
                q.source,
                "find",
                None,
                filt,
                q.projection,
                q.limit,
                sort_pairs,
            )
            timed = mongo.run_find_timed(
                q.database,
                q.source,
                filt,
                q.projection,
                sort_pairs,
                q.limit,
                max_ms,
            )
            findings = analyze_find(filt, truth, src)
            sk = [s[0] for s in (sort_pairs or [])]
            index_recs = index_recommendations_from_find(filt, sk, truth)
            original_summary = json.dumps(
                {
                    "mode": "find",
                    "filter": filt,
                    "projection": q.projection,
                    "sort": sort_pairs,
                    "limit": q.limit,
                },
                default=str,
            )[:8000]
            _pipeline_for_literals = [{"$match": filt or {}}]

        if (
            timed.ok
            and (timed.returned_count == 0)
            and truth.sample_doc_count > 0
        ):
            sample_n = min(100, max(settings.sample_size, 50))
            sample_docs = mongo.sample_documents(q.database, q.source, sample_n)
            sch04_paths = {
                str(f.evidence["path"])
                for f in findings
                if f.rule_id == "SCH-04"
                and isinstance(f.evidence, dict)
                and f.evidence.get("path") is not None
            }
            literals = extract_match_equality_literals(_pipeline_for_literals)
            findings.extend(
                findings_for_zero_timed_run(
                    literals,
                    sample_docs,
                    truth.sample_doc_count,
                    sch04_paths,
                )
            )

        stats = explain.execution_stats
        interpret = _interpret_explain(stats, timed.duration_ms if timed.ok else None, timed.returned_count)
        bottlenecks = _bottlenecks_from(findings, stats)

        suggestion = suggest_rewrite(
            settings,
            q,
            src,
            truth,
            findings,
            stats,
            settings.env,
        )

        issue = "Diagnostics complete. Review rule findings and explain stats."
        prefix = ""
        if not truth.namespace_found:
            prefix = (
                f"**Namespace not found:** `{q.source}` is not listed in database `{q.database}`. "
                "If the CLI connects without error, this is usually a wrong **database name**, **collection/view name**, or **cluster/URI** — not a generic “connection failed.” "
                "Confirm names with `mongosh` (`show collections`) or Compass.\n\n"
            )
        elif truth.sample_doc_count == 0 and truth.approximate_document_count == 0:
            prefix = (
                "**Empty namespace:** sampled **0** documents and **estimated_document_count** is **0**. "
                "The collection exists but has no documents (or the estimate is unavailable). "
                "Pipelines will read zero rows until data is inserted.\n\n"
            )
        if timed.ok is False:
            issue = f"Timed execution failed or hit an error: {timed.error}"
        elif isinstance(stats.get("totalDocsExamined"), int) and stats["totalDocsExamined"] > 50_000:
            issue = "Heavy read workload detected; prioritize early filtering and index alignment."
        issue = prefix + issue

        risks: list[str] = []
        if src.kind == "view":
            risks.append("Standard views execute pipelines at read time — validate against base collections when optimizing.")
        if suggestion.risks:
            risks.extend(suggestion.risks[:8])
        conf = "medium"
        if suggestion.confidence >= 0.75:
            conf = "high"
        elif suggestion.confidence <= 0.35 and not suggestion.skipped_reason:
            conf = "low"

        metadata = {
            "explain_raw": explain.raw,
            "timed_run": timed.model_dump(),
            "source": src.model_dump(),
            "namespace": {
                "database": q.database,
                "source": q.source,
                "namespace_found": truth.namespace_found,
                "sample_doc_count": truth.sample_doc_count,
                "approximate_document_count": truth.approximate_document_count,
            },
        }

        report = build_v1_report(
            issue_summary=issue,
            bottlenecks=bottlenecks,
            findings=findings,
            explain_interpretation=interpret,
            suggestion=suggestion,
            index_recs=index_recs,
            risk_notes=risks,
            confidence_rating=conf,
            environment=settings.env,
            original_query_summary=original_summary,
            metadata=metadata,
        )
        md = report_to_markdown(report)
        return report, md
    finally:
        mongo.close()
