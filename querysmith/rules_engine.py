from __future__ import annotations

from typing import Any

from querysmith.models import FieldTypeInfo, IndexRecommendation, RuleFinding, SourceInfo, TruthBundle
from querysmith.pipeline_parse import (
    extract_match_equality_literals,
    extract_referenced_paths,
    first_stage_index,
    literal_compatible_with_sampled_types,
    literal_type_name,
    normalize_aggregate,
    stage_order_summary,
    validate_paths_against_schema,
)


def analyze_aggregate(
    pipeline: list[dict[str, Any]],
    truth: TruthBundle,
    source: SourceInfo,
    max_stage_warn: int = 25,
) -> list[RuleFinding]:
    pipeline = normalize_aggregate(pipeline)
    findings: list[RuleFinding] = []

    if source.kind == "view":
        findings.append(
            RuleFinding(
                rule_id="SRC-05",
                category="source",
                severity="warn",
                message="Source is a standard view; performance-sensitive paths should optimize on base collections when possible.",
                evidence={"view_pipeline_length": len(source.view_pipeline or [])},
            )
        )
        if not source.view_pipeline:
            findings.append(
                RuleFinding(
                    rule_id="SRC-06",
                    category="source",
                    severity="error",
                    message="View definition could not be loaded; do not optimize blindly on top of this view.",
                    evidence={},
                )
            )

    schema_paths = {f.path for f in truth.field_types}
    ref_paths = extract_referenced_paths(pipeline)
    missing = validate_paths_against_schema(ref_paths, schema_paths)
    for m in missing[:40]:
        findings.append(
            RuleFinding(
                rule_id="SCH-01",
                category="schema",
                severity="warn",
                message=f"Field '{m}' was referenced but not observed in sampled schema (may be sparse or typo).",
                evidence={"field": m},
            )
        )

    for ft in truth.field_types:
        tset = set(ft.types)
        mixed_string_int = {"string", "int"}.issubset(tset)
        mixed_int_double = {"int", "double"}.issubset(tset)
        if mixed_string_int or mixed_int_double:
            findings.append(
                RuleFinding(
                    rule_id="SCH-03",
                    category="schema",
                    severity="warn",
                    message=f"Field '{ft.path}' shows mixed types in samples: {ft.types}.",
                    evidence={"path": ft.path, "types": ft.types},
                )
            )

    literals = extract_match_equality_literals(pipeline)
    findings.extend(_literal_type_mismatch_findings(literals, truth.field_types))

    order = stage_order_summary(pipeline)

    # --- Rule 4: join-key type cross-check (SCH-04b) ---
    path_to_types = {ft.path: set(ft.types) for ft in truth.field_types}
    for i, stage in enumerate(pipeline):
        op = next(iter(stage.keys()))
        body = stage[op]
        if op == "$lookup" and isinstance(body, dict):
            lf = body.get("localField")
            ff = body.get("foreignField")
            if isinstance(lf, str) and isinstance(ff, str):
                lt = path_to_types.get(lf)
                if lt and "string" in lt and "objectId" not in lt:
                    findings.append(
                        RuleFinding(
                            rule_id="SCH-04b",
                            category="schema",
                            severity="warn",
                            message=(
                                f"$lookup join key '{lf}' appears to be string in samples; if the foreign collection stores "
                                f"'{ff}' as ObjectId (or int), the join will silently return 0 matches."
                            ),
                            evidence={"stage_index": i, "localField": lf, "foreignField": ff, "local_types": sorted(lt)},
                        )
                    )

    # --- PIPE-01: stage count ---
    if len(pipeline) > max_stage_warn:
        findings.append(
            RuleFinding(
                rule_id="PIPE-01",
                category="pipeline",
                severity="info",
                message=f"Pipeline has {len(pipeline)} stages; consider breaking into smaller units for maintainability.",
                evidence={"stage_count": len(pipeline)},
            )
        )

    # --- C. Pipeline ordering ---
    im = first_stage_index(pipeline, "$match")
    ilook = first_stage_index(pipeline, "$lookup")
    igroup = first_stage_index(pipeline, "$group")

    # Rule 8: $match after $lookup
    if im is not None and ilook is not None and im > ilook:
        findings.append(
            RuleFinding(
                rule_id="ORD-08",
                category="ordering",
                severity="warn",
                message="$match appears after $lookup; push selective $match earlier when semantics allow.",
                evidence={"match_index": im, "lookup_index": ilook},
            )
        )
    if im is not None and igroup is not None and im > igroup:
        findings.append(
            RuleFinding(
                rule_id="ORD-08b",
                category="ordering",
                severity="info",
                message="$match appears after $group; verify intentional late filtering.",
                evidence={"match_index": im, "group_index": igroup},
            )
        )

    # Rule 9: adjacent $match stages that could be merged
    for i in range(len(pipeline) - 1):
        if _op(pipeline[i]) == "$match" and _op(pipeline[i + 1]) == "$match":
            findings.append(
                RuleFinding(
                    rule_id="ORD-09",
                    category="ordering",
                    severity="info",
                    message=f"Adjacent $match stages at positions {i} and {i + 1} can be merged into one.",
                    evidence={"stage_a": i, "stage_b": i + 1},
                )
            )

    # Rule 10: expensive stages before cardinality reduction
    _EXPENSIVE = {"$lookup", "$graphLookup", "$unwind", "$group", "$facet", "$unionWith", "$setWindowFields"}
    first_match_idx = first_stage_index(pipeline, "$match")
    for i, stage in enumerate(pipeline):
        sop = _op(stage)
        if sop in _EXPENSIVE and (first_match_idx is None or i < first_match_idx):
            findings.append(
                RuleFinding(
                    rule_id="ORD-10",
                    category="ordering",
                    severity="warn",
                    message=(
                        f"Expensive stage `{sop}` at position {i} runs before any `$match` that could reduce cardinality. "
                        "Move selective filters earlier if semantics allow."
                    ),
                    evidence={"stage_index": i, "stage_op": sop, "first_match": first_match_idx},
                )
            )

    # --- D. Lookup rules ---
    for i, stage in enumerate(pipeline):
        sop = _op(stage)
        body = stage[sop]
        if sop != "$lookup" or not isinstance(body, dict):
            continue

        # Rule 11 already emits LKP-11 for pipeline lookups; extend with Rule 12
        has_pipeline = "pipeline" in body
        has_let = "let" in body

        if has_pipeline and not has_let:
            # Rule 12: uncorrelated pipeline $lookup → simplify to localField/foreignField
            findings.append(
                RuleFinding(
                    rule_id="LKP-12",
                    category="lookup",
                    severity="warn",
                    message=(
                        f"Pipeline $lookup at stage {i} has no `let` (uncorrelated). "
                        "Convert to localField/foreignField for better performance unless the sub-pipeline has complex logic."
                    ),
                    evidence={"stage_index": i},
                )
            )
        elif has_pipeline and has_let:
            findings.append(
                RuleFinding(
                    rule_id="LKP-11",
                    category="lookup",
                    severity="info",
                    message=(
                        f"Correlated pipeline $lookup at stage {i} (uses `let`). "
                        "Verify the correlation is necessary; simple equality joins are cheaper with localField/foreignField."
                    ),
                    evidence={"stage_index": i},
                )
            )

        # Rule 14: fan-out risk
        as_field = body.get("as")
        from_coll = body.get("from")
        if isinstance(from_coll, str) and isinstance(as_field, str):
            coll_stat = truth.collection_stats or {}
            est_count = truth.approximate_document_count or coll_stat.get("count", 0)
            if isinstance(est_count, int) and est_count > 100_000:
                findings.append(
                    RuleFinding(
                        rule_id="LKP-14",
                        category="lookup",
                        severity="warn",
                        message=(
                            f"$lookup at stage {i} joins to '{from_coll}' (source collection ~{est_count:,} docs). "
                            "High-cardinality targets risk fan-out; ensure the foreign side has an index on the join key "
                            "and consider limiting results with a sub-pipeline $match or $limit."
                        ),
                        evidence={"stage_index": i, "from": from_coll, "source_estimated_count": est_count},
                    )
                )

    # --- E. Projection rules ---
    _NEEDS_NARROW = {"$lookup", "$group", "$facet", "$graphLookup"}
    for i, stage in enumerate(pipeline):
        sop = _op(stage)
        if sop not in _NEEDS_NARROW:
            continue
        has_project_before = any(
            _op(pipeline[j]) in ("$project", "$addFields", "$set", "$unset")
            for j in range(max(0, i - 3), i)
        )
        if not has_project_before and i > 0:
            findings.append(
                RuleFinding(
                    rule_id="PRJ-15",
                    category="projection",
                    severity="info",
                    message=(
                        f"No `$project` / `$unset` before expensive stage `{sop}` at position {i}. "
                        "Wide documents entering joins/groups carry unnecessary data; narrow the projection first."
                    ),
                    evidence={"stage_index": i, "stage_op": sop},
                )
            )

    # --- F. Unwind / group rules ---
    unwind_positions: list[int] = []
    for i, stage in enumerate(pipeline):
        sop = _op(stage)
        body = stage[sop]
        if sop == "$unwind":
            unwind_positions.append(i)
            findings.append(
                RuleFinding(
                    rule_id="UW-19",
                    category="unwind",
                    severity="info",
                    message=f"$unwind at stage {i} multiplies documents; verify the array is bounded and filtered early.",
                    evidence={"stage_index": i},
                )
            )
        if sop == "$group":
            if unwind_positions and i > 0 and _op(pipeline[i - 1]) == "$unwind":
                findings.append(
                    RuleFinding(
                        rule_id="UW-20",
                        category="unwind",
                        severity="warn",
                        message=(
                            f"$group at stage {i} immediately follows $unwind — this often re-aggregates an earlier explosion. "
                            "Consider rewriting with `$reduce`, `$map`, or restructuring the pipeline."
                        ),
                        evidence={"group_index": i, "preceding_unwind": i - 1},
                    )
                )

    # Rule 18: cascaded unwinds
    for idx in range(len(unwind_positions) - 1):
        a, b = unwind_positions[idx], unwind_positions[idx + 1]
        if b == a + 1:
            findings.append(
                RuleFinding(
                    rule_id="UW-18b",
                    category="unwind",
                    severity="warn",
                    message=(
                        f"Cascaded $unwind stages at positions {a} and {b}. "
                        "Back-to-back unwinds cause multiplicative document explosion — restructure if possible."
                    ),
                    evidence={"positions": [a, b]},
                )
            )

    # --- G. Pagination and boundedness ---
    has_limit = any(_op(s) == "$limit" for s in pipeline)
    has_sample = any(_op(s) == "$sample" for s in pipeline)
    has_skip = any(_op(s) == "$skip" for s in pipeline)
    est_count = truth.approximate_document_count

    if not has_limit and not has_sample:
        findings.append(
            RuleFinding(
                rule_id="BD-21",
                category="boundedness",
                severity="info",
                message="No $limit or $sample; confirm result cardinality is acceptable for this workload.",
                evidence={"stage_order": order[:15]},
            )
        )
        # Rule 23: suggest cursor pagination on indexed keys
        sort_fields_in_pipe = []
        for s in pipeline:
            if _op(s) == "$sort" and isinstance(s["$sort"], dict):
                sort_fields_in_pipe = list(s["$sort"].keys())
                break
        idx_key_sets = [set(ix.get("keys", {}).keys()) for ix in truth.indexes]
        has_sort_index = any(
            sort_fields_in_pipe and sort_fields_in_pipe[0] in ks for ks in idx_key_sets
        )
        if sort_fields_in_pipe and has_sort_index:
            findings.append(
                RuleFinding(
                    rule_id="BD-23",
                    category="boundedness",
                    severity="info",
                    message=(
                        f"Unbounded result with $sort on {sort_fields_in_pipe} which aligns with an index. "
                        "Consider cursor-based pagination (range query on the sort key + $limit) for large result sets."
                    ),
                    evidence={"sort_fields": sort_fields_in_pipe},
                )
            )

    if has_skip and not has_limit:
        findings.append(
            RuleFinding(
                rule_id="BD-22",
                category="boundedness",
                severity="warn",
                message=(
                    "$skip without $limit: MongoDB still scans and skips documents. "
                    "Prefer range-based (keyset) pagination on a stable indexed field."
                ),
                evidence={},
            )
        )

    # --- H. Index rules ---
    non_id_indexes = [ix for ix in truth.indexes if ix.get("name") != "_id_"]
    index_names = [ix.get("name") for ix in truth.indexes]
    idx_key_lists = [list(ix.get("keys", {}).keys()) for ix in non_id_indexes]

    # Rule 24: compare filter/sort pattern vs existing indexes
    match_fields_all: list[str] = []
    sort_fields_all: list[str] = []
    for stage in pipeline:
        sop = _op(stage)
        body = stage[sop]
        if sop == "$match" and isinstance(body, dict):
            for k in body.keys():
                if not str(k).startswith("$") and k not in match_fields_all:
                    match_fields_all.append(str(k))
        if sop == "$sort" and isinstance(body, dict):
            for k in body.keys():
                if k not in sort_fields_all:
                    sort_fields_all.append(str(k))

    desired_prefix = (match_fields_all + sort_fields_all)[:4]
    if desired_prefix and idx_key_lists:
        best_overlap = max(
            sum(1 for j, f in enumerate(desired_prefix) if j < len(ks) and ks[j] == f)
            for ks in idx_key_lists
        )
        if best_overlap == 0:
            findings.append(
                RuleFinding(
                    rule_id="IDX-24b",
                    category="index",
                    severity="warn",
                    message=(
                        f"Filter/sort fields {desired_prefix} share no leading prefix with any existing index. "
                        "The query likely requires a full collection scan."
                    ),
                    evidence={"desired_prefix": desired_prefix, "existing_indexes": idx_key_lists},
                )
            )
        elif best_overlap < len(desired_prefix):
            findings.append(
                RuleFinding(
                    rule_id="IDX-25",
                    category="index",
                    severity="info",
                    message=(
                        f"Filter/sort fields {desired_prefix} partially overlap with existing indexes (best prefix match: "
                        f"{best_overlap}/{len(desired_prefix)}). A compound index covering more of these fields may help."
                    ),
                    evidence={"desired_prefix": desired_prefix, "best_overlap": best_overlap},
                )
            )

    if ref_paths and not non_id_indexes:
        if source.kind == "view":
            findings.append(
                RuleFinding(
                    rule_id="IDX-24",
                    category="index",
                    severity="info",
                    message=(
                        "Standard views do not own indexes: MongoDB lists indexes on collections, not on view "
                        "namespaces, so this check is often empty even when underlying collections are indexed. "
                        "Validate indexes on the base collections this view reads from."
                    ),
                    evidence={"source_kind": "view", "indexes_listed_on_namespace": index_names},
                )
            )
        elif not truth.indexes:
            findings.append(
                RuleFinding(
                    rule_id="IDX-24",
                    category="index",
                    severity="warn",
                    message=(
                        "listIndexes returned no indexes for this namespace (unexpected for a collection). "
                        "Check permissions, spelling of database/source, or connectivity. "
                        "IDX-24 only reflects this one namespace, not $lookup targets."
                    ),
                    evidence={"indexes_listed_on_namespace": index_names},
                )
            )
        else:
            findings.append(
                RuleFinding(
                    rule_id="IDX-24",
                    category="index",
                    severity="info",
                    message=(
                        "Only the default _id index is reported on this pipeline source namespace. "
                        "If you have compound indexes, they apply to this collection name only — not to other "
                        "collections joined via $lookup."
                    ),
                    evidence={"indexes_listed_on_namespace": index_names},
                )
            )

    return findings


def _op(stage: dict[str, Any]) -> str:
    return next(iter(stage.keys()))


def analyze_find(
    filter_q: dict[str, Any],
    truth: TruthBundle,
    source: SourceInfo,
) -> list[RuleFinding]:
    paths = extract_referenced_paths([{"$match": filter_q or {}}])
    findings: list[RuleFinding] = []
    if source.kind == "view":
        findings.append(
            RuleFinding(
                rule_id="SRC-05",
                category="source",
                severity="warn",
                message="Source is a standard view; consider optimizing underlying pipeline/collections.",
                evidence={},
            )
        )
    schema_paths = {f.path for f in truth.field_types}
    missing = validate_paths_against_schema(paths, schema_paths)
    for m in missing[:40]:
        findings.append(
            RuleFinding(
                rule_id="SCH-01",
                category="schema",
                severity="warn",
                message=f"Field '{m}' was referenced but not observed in sampled schema.",
                evidence={"field": m},
            )
        )
    literals = extract_match_equality_literals([{"$match": filter_q or {}}])
    findings.extend(_literal_type_mismatch_findings(literals, truth.field_types))
    return findings


def findings_for_zero_timed_run(
    literals: list[tuple[str, Any]],
    sample_docs: list[dict[str, Any]],
    sample_doc_count: int,
    sch04_paths: set[str],
) -> list[RuleFinding]:
    """
    When the timed run returns 0 rows but we have sample documents, compare $match equality literals
    to distinct values observed in those samples (RUN-01).
    """
    from querysmith.pipeline_parse import collect_distinct_exemplars

    findings: list[RuleFinding] = []
    if sample_doc_count <= 0 or not sample_docs:
        return findings

    if not literals:
        findings.append(
            RuleFinding(
                rule_id="RUN-01",
                category="runtime",
                severity="info",
                message=(
                    "Timed run returned **0** documents. No `$match` equality literals were extracted to compare "
                    "with samples — check later stages (`$group`, `$lookup`, etc.) or filters inside sub-pipelines."
                ),
                evidence={"sample_doc_count": sample_doc_count},
            )
        )
        return findings

    first_literal: dict[str, Any] = {}
    for path, lit in literals:
        if path not in first_literal:
            first_literal[path] = lit

    paths = list(first_literal.keys())
    exemplars = collect_distinct_exemplars(sample_docs, paths)

    def _preview(vals: list[Any]) -> str:
        out: list[str] = []
        for v in vals[:8]:
            r = repr(v)
            out.append(r if len(r) <= 56 else r[:53] + "...")
        return ", ".join(out) if out else "(none)"

    for path, lit in first_literal.items():
        if path in sch04_paths:
            continue
        ex = exemplars.get(path) or []
        if not ex:
            findings.append(
                RuleFinding(
                    rule_id="RUN-01",
                    category="runtime",
                    severity="warn",
                    message=(
                        f"Timed run returned 0 documents. Samples had **no** usable values for `{path}` "
                        f"(missing or null in the sample set). Filter literal was {_short_repr(lit)}."
                    ),
                    evidence={
                        "path": path,
                        "literal": lit,
                        "sample_doc_count": sample_doc_count,
                    },
                )
            )
            continue
        if any(lit == x for x in ex):
            findings.append(
                RuleFinding(
                    rule_id="RUN-01",
                    category="runtime",
                    severity="info",
                    message=(
                        f"Timed run returned 0 documents, but `{path}` = {_short_repr(lit)} **appears among sampled values** "
                        f"({_preview(ex)}). The empty result is likely from **later** stages, not this equality."
                    ),
                    evidence={
                        "path": path,
                        "literal": lit,
                        "sample_values": ex[:10],
                    },
                )
            )
        else:
            findings.append(
                RuleFinding(
                    rule_id="RUN-01",
                    category="runtime",
                    severity="warn",
                    message=(
                        f"Timed run returned 0 documents. Your filter uses `{path}` = {_short_repr(lit)}. "
                        f"**Sampled values** for this field include: {_preview(ex)} — use a value that exists in data "
                        f"(or fix type/semantics; see SCH-04 if types conflict)."
                    ),
                    evidence={
                        "path": path,
                        "literal": lit,
                        "sample_values": ex[:10],
                    },
                )
            )
    return findings


def _short_repr(v: Any, n: int = 100) -> str:
    r = repr(v)
    return r if len(r) <= n else r[: n - 3] + "..."


def _literal_type_mismatch_findings(
    literals: list[tuple[str, Any]],
    field_types: list[FieldTypeInfo],
) -> list[RuleFinding]:
    """SCH-04: equality literals that cannot match sampled BSON types (e.g. string vs int field)."""
    path_to_types = {ft.path: set(ft.types) for ft in field_types}
    findings: list[RuleFinding] = []
    seen: set[tuple[str, str]] = set()
    for path, value in literals:
        lit = literal_type_name(value)
        if lit is None:
            continue
        st = path_to_types.get(path)
        if st is None:
            continue
        if literal_compatible_with_sampled_types(lit, st):
            continue
        dedup = (path, lit)
        if dedup in seen:
            continue
        seen.add(dedup)
        findings.append(
            RuleFinding(
                rule_id="SCH-04",
                category="schema",
                severity="error",
                message=(
                    f"Type mismatch on '{path}': filter uses a {lit} literal, but sampled documents have types "
                    f"{sorted(st)}. This predicate will not match numeric (or other) values stored in that field "
                    f"unless you use the correct type (e.g. `1` not `\"1\"`)."
                ),
                evidence={
                    "path": path,
                    "literal_type": lit,
                    "sampled_types": sorted(st),
                    "value_preview": str(value)[:200],
                },
            )
        )
    return findings


def index_recommendations_from_pipeline(
    pipeline: list[dict[str, Any]],
    truth: TruthBundle,
    findings: list[RuleFinding] | None = None,
) -> list[IndexRecommendation]:
    recs: list[IndexRecommendation] = []
    match_fields: list[str] = []
    sort_fields: list[str] = []
    for stage in pipeline:
        op = _op(stage)
        body = stage[op]
        if op == "$match" and isinstance(body, dict):
            for k in body.keys():
                if not str(k).startswith("$"):
                    match_fields.append(str(k))
        if op == "$sort" and isinstance(body, dict):
            sort_fields.extend([str(x) for x in body.keys()])

    existing = [set(ix.get("keys", {}).keys()) for ix in truth.indexes]
    compound: list[str] = []
    for f in match_fields + sort_fields:
        if f and f not in compound:
            compound.append(f)
    compound = compound[:4]

    # Rule 26: avoid recommending new indexes if structural rewrites can solve the issue first
    structural_issues = False
    if findings:
        structural_ids = {"ORD-08", "ORD-09", "ORD-10", "LKP-12", "PRJ-15", "UW-18b", "UW-20", "SCH-04"}
        structural_issues = any(f.rule_id in structural_ids for f in findings)

    if compound:
        covered = any(compound[0] in ex for ex in existing)
        if not covered:
            keys = {k: 1 for k in compound}
            rationale = "Compound index aligned to early $match/$sort fields (verify with real cardinality and write load)."
            if structural_issues:
                rationale = (
                    "Compound index aligned to early $match/$sort fields — BUT structural rewrites "
                    "(ordering, projection, lookup simplification) should be applied first. "
                    "An index does not fix a badly shaped pipeline."
                )
            recs.append(
                IndexRecommendation(
                    keys=keys,
                    options={},
                    rationale=rationale,
                    alignment=compound,
                )
            )
    return recs


def index_recommendations_from_find(
    filter_q: dict[str, Any],
    sort_keys: list[str] | None,
    truth: TruthBundle,
) -> list[IndexRecommendation]:
    paths = extract_referenced_paths([{"$match": filter_q or {}}])
    fields = list(dict.fromkeys([p.split(".")[0] for p in sorted(paths) if p]))
    if sort_keys:
        for s in sort_keys:
            if s not in fields:
                fields.append(s)
    existing = [set(ix.get("keys", {}).keys()) for ix in truth.indexes]
    compound = fields[:4]
    recs: list[IndexRecommendation] = []
    if compound:
        covered = any(compound[0] in ex for ex in existing)
        if not covered:
            keys = {k: 1 for k in compound}
            recs.append(
                IndexRecommendation(
                    keys=keys,
                    options={},
                    rationale="Compound index aligned to filter/sort keys (verify selectivity and write impact).",
                    alignment=compound,
                )
            )
    return recs
