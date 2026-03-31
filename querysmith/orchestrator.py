from __future__ import annotations

import json
from typing import Any

from querysmith.config import Settings, load_settings
from querysmith.llm_planner import suggest_rewrite, suggest_view_flatten
from querysmith.models import QueryInput, RuleFinding, V1Report, ViewFlattenSuggestion
from querysmith.mongo_client import MongoService, extract_lookup_stage_stats
from querysmith.pipeline_parse import extract_match_equality_literals, normalize_aggregate
from querysmith.report_builder import build_v1_report, report_to_markdown
from querysmith.rules_engine import (
    analyze_aggregate,
    analyze_find,
    findings_for_zero_timed_run,
    index_recommendations_from_find,
    index_recommendations_from_pipeline,
)

_SLOW_LOOKUP_TIME_MS = 5_000
_SLOW_LOOKUP_DOCS_EXAMINED = 100_000
_MAX_LOOKUP_FLATTEN = 2


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


def _is_lookup_slow(
    from_coll: str,
    lookup_stats: list[dict[str, Any]],
    timed_duration_ms: float | None,
    overall_exec_ms: Any,
) -> bool:
    """Determine if a $lookup is slow from per-stage explain stats or overall timing."""
    for ls in lookup_stats:
        if ls.get("from") != from_coll:
            continue
        est = ls.get("executionTimeMillisEstimate")
        docs = ls.get("totalDocsExamined")
        if isinstance(est, (int, float)) and est > _SLOW_LOOKUP_TIME_MS:
            return True
        if isinstance(docs, int) and docs > _SLOW_LOOKUP_DOCS_EXAMINED:
            return True
        return False
    if timed_duration_ms is not None and timed_duration_ms > 30_000:
        return True
    if isinstance(overall_exec_ms, (int, float)) and overall_exec_ms > 10_000:
        return True
    return False


def _attempt_source_view_flatten(
    mongo: MongoService,
    settings: Settings,
    q: QueryInput,
    src: Any,
    pipeline: list[dict[str, Any]],
    truth: Any,
    findings: list[Any],
    stats: dict[str, Any],
) -> ViewFlattenSuggestion | None:
    """Branch 1: source is a view and query timed out — flatten and prune."""
    base_coll, chain, view_pipe = mongo.resolve_source_deep(q.database, q.source)
    if base_coll == q.source or not view_pipe:
        return None
    flattened = view_pipe + pipeline
    return suggest_view_flatten(
        settings,
        trigger="source_timeout",
        original_source=q.source,
        base_collection=base_coll,
        view_chain=chain,
        view_pipeline=view_pipe,
        user_pipeline=pipeline,
        flattened_pipeline=flattened,
        truth=truth,
        findings=findings,
        explain_stats=stats,
    )


def _attempt_lookup_view_flatten(
    mongo: MongoService,
    settings: Settings,
    q: QueryInput,
    pipeline: list[dict[str, Any]],
    raw_explain: dict[str, Any],
    timed_duration_ms: float | None,
    truth: Any,
    findings: list[Any],
    stats: dict[str, Any],
) -> list[ViewFlattenSuggestion]:
    """Branch 2: slow $lookup targets a view — inline view pipeline."""
    lookup_stats = extract_lookup_stage_stats(raw_explain)
    overall_exec_ms = stats.get("executionTimeMillis")
    results: list[ViewFlattenSuggestion] = []
    flattened_count = 0
    for i, stage in enumerate(pipeline):
        if flattened_count >= _MAX_LOOKUP_FLATTEN:
            break
        op = next(iter(stage.keys()))
        if op != "$lookup":
            continue
        body = stage[op]
        from_coll = body.get("from")
        if not isinstance(from_coll, str):
            continue
        if not _is_lookup_slow(from_coll, lookup_stats, timed_duration_ms, overall_exec_ms):
            continue
        target_src = mongo.resolve_source(q.database, from_coll)
        if target_src.kind != "view":
            continue
        base_coll, chain, view_pipe = mongo.resolve_source_deep(q.database, from_coll)
        if base_coll == from_coll or not view_pipe:
            continue
        flattened_count += 1
        vf = suggest_view_flatten(
            settings,
            trigger="slow_lookup",
            original_source=from_coll,
            base_collection=base_coll,
            view_chain=chain,
            view_pipeline=view_pipe,
            user_pipeline=pipeline,
            flattened_pipeline=view_pipe + pipeline,
            truth=truth,
            findings=findings,
            explain_stats=stats,
            lookup_stage_index=i,
        )
        results.append(vf)
    return results


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

        # ── View flattening (Branch 1 + Branch 2) ──────────────────────
        vf_suggestions: list[ViewFlattenSuggestion] = []

        if q.mode == "aggregate":
            # Branch 1: source is a view and query was slow / timed out
            source_is_slow = (
                (not timed.ok)
                or (timed.duration_ms is not None and timed.duration_ms > settings.view_flatten_timeout_ms)
            )
            if source_is_slow and src.kind == "view":
                vf = _attempt_source_view_flatten(mongo, settings, q, src, pipeline, truth, findings, stats)
                if vf is not None:
                    vf_suggestions.append(vf)
                    findings.append(
                        RuleFinding(
                            rule_id="VF-01",
                            category="view_flatten",
                            severity="warn",
                            message=(
                                f"Source view '{q.source}' timed out or exceeded {settings.view_flatten_timeout_ms}ms. "
                                f"Flattened pipeline targeting base collection '{vf.base_collection}' generated "
                                f"({vf.view_stage_count} view stages + {vf.user_stage_count} user stages "
                                f"= {len(vf.flattened_pipeline)} total). "
                                "Review the pruned suggestion — verify result equivalence before adopting."
                            ),
                            evidence={
                                "base_collection": vf.base_collection,
                                "view_chain": vf.view_chain,
                                "view_stage_count": vf.view_stage_count,
                                "flattened_stage_count": len(vf.flattened_pipeline),
                            },
                        )
                    )

            # Branch 2: slow $lookup targeting a view
            lookup_vfs = _attempt_lookup_view_flatten(
                mongo, settings, q, pipeline, explain.raw,
                timed.duration_ms if timed.ok else None,
                truth, findings, stats,
            )
            for lvf in lookup_vfs:
                vf_suggestions.append(lvf)
                findings.append(
                    RuleFinding(
                        rule_id="VF-02",
                        category="view_flatten",
                        severity="warn",
                        message=(
                            f"$lookup at stage {lvf.lookup_stage_index} targets view '{lvf.original_source}' "
                            f"which is slow in the explain plan. View resolves to base collection "
                            f"'{lvf.base_collection}' via chain {lvf.view_chain}. "
                            "Consider inlining only the necessary view stages into the $lookup sub-pipeline. "
                            "Verify result equivalence before adopting."
                        ),
                        evidence={
                            "lookup_stage_index": lvf.lookup_stage_index,
                            "lookup_from": lvf.original_source,
                            "base_collection": lvf.base_collection,
                            "view_chain": lvf.view_chain,
                            "view_stage_count": lvf.view_stage_count,
                        },
                    )
                )

        risks: list[str] = []
        if src.kind == "view":
            risks.append("Standard views execute pipelines at read time — validate against base collections when optimizing.")
        if vf_suggestions:
            risks.append(
                "View-flattened pipelines MUST be verified for result equivalence (row count + sample row comparison) "
                "before being adopted in production."
            )
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
        if vf_suggestions:
            metadata["view_flatten"] = [vf.model_dump() for vf in vf_suggestions]

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
            view_flatten_suggestions=vf_suggestions,
        )
        md = report_to_markdown(report)
        return report, md
    finally:
        mongo.close()
