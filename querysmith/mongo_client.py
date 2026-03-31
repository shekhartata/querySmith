from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from bson import ObjectId
from bson.decimal128 import Decimal128
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

from querysmith.config import Settings
from querysmith.models import ExplainSnapshot, FieldTypeInfo, SourceInfo, TimedRunResult, TruthBundle


def _type_name(val: Any) -> str:
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "bool"
    if isinstance(val, int):
        return "int"
    if isinstance(val, float):
        return "double"
    if isinstance(val, str):
        return "string"
    if isinstance(val, ObjectId):
        return "objectId"
    if isinstance(val, Decimal128):
        return "decimal"
    if isinstance(val, bytes):
        return "binData"
    if isinstance(val, dict):
        return "object"
    if isinstance(val, list):
        return "array"
    return type(val).__name__


def _merge_path_types(acc: dict[str, set[str]], prefix: str, doc: Any) -> None:
    if doc is None:
        acc[prefix].add("null")
        return
    if isinstance(doc, dict):
        for k, v in doc.items():
            path = f"{prefix}.{k}" if prefix else k
            acc[path].add(_type_name(v))
            if isinstance(v, dict):
                _merge_path_types(acc, path, v)
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                for item in v[:3]:
                    if isinstance(item, dict):
                        _merge_path_types(acc, path, item)
    elif isinstance(doc, list) and doc:
        for item in doc[:5]:
            _merge_path_types(acc, prefix, item)


def _paths_from_doc(doc: dict[str, Any]) -> dict[str, set[str]]:
    acc: dict[str, set[str]] = defaultdict(set)
    _merge_path_types(acc, "", doc)
    return acc


class MongoService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = MongoClient(settings.mongodb_uri, serverSelectionTimeoutMS=10_000)

    def close(self) -> None:
        self._client.close()

    def get_database(self, name: str):
        return self._client[name]

    def resolve_source(self, database: str, source: str) -> SourceInfo:
        db = self._client[database]
        for coll in db.list_collections(filter={"name": source}):
            if coll.get("name") != source:
                continue
            ctype = coll.get("type", "collection")
            if ctype == "view":
                opts = coll.get("options") or {}
                pipeline = list(opts.get("pipeline") or [])
                view_on = opts.get("viewOn") or None
                return SourceInfo(name=source, kind="view", view_on=view_on, view_pipeline=pipeline)
            return SourceInfo(name=source, kind="collection", view_pipeline=None)
        return SourceInfo(name=source, kind="collection", view_pipeline=None)

    def resolve_source_deep(
        self, database: str, source: str, max_depth: int = 10,
    ) -> tuple[str, list[str], list[dict[str, Any]]]:
        """Recursively resolve a view chain to the base collection.

        Returns (base_collection, chain, combined_view_pipeline) where the
        combined pipeline is ordered deepest-first so that prepending the
        user's stages produces the correct execution order.
        """
        chain: list[str] = [source]
        pipelines: list[list[dict[str, Any]]] = []
        current = source
        for _ in range(max_depth):
            src = self.resolve_source(database, current)
            if src.kind != "view" or not src.view_on:
                break
            pipelines.append(src.view_pipeline or [])
            current = src.view_on
            chain.append(current)
        combined: list[dict[str, Any]] = []
        for p in reversed(pipelines):
            combined.extend(p)
        return current, chain, combined

    def namespace_diagnostics(self, database: str, source: str) -> tuple[bool, int | None]:
        """Whether the name exists in the DB, and estimated_document_count when applicable."""
        db = self._client[database]
        found = source in db.list_collection_names()
        approx: int | None = None
        if found:
            try:
                approx = int(db[source].estimated_document_count())
            except (PyMongoError, TypeError, ValueError):
                approx = None
        return found, approx

    def get_collection_schema(self, database: str, collection: str) -> tuple[list[FieldTypeInfo], int]:
        coll = self._client[database][collection]
        n = self._settings.sample_size
        if not n:
            return [], 0
        try:
            docs = list(coll.aggregate([{"$sample": {"size": n}}]))
        except OperationFailure:
            docs = list(coll.find().limit(min(n, 50)))
        merged: dict[str, set[str]] = defaultdict(set)
        for d in docs:
            if not isinstance(d, dict):
                continue
            for path, types in _paths_from_doc(d).items():
                merged[path].update(types)
        fields = [
            FieldTypeInfo(path=p, types=sorted(t), nullable=("null" in t))
            for p, t in sorted(merged.items())
        ]
        return fields, len(docs)

    def list_indexes(self, database: str, collection: str) -> list[dict[str, Any]]:
        """Return index specs. Raises OperationFailure on auth/permission errors (callers may catch)."""
        coll = self._client[database][collection]
        out: list[dict[str, Any]] = []
        idx_info = coll.index_information()
        for name, spec in idx_info.items():
            keys = spec.get("key")
            key_d: dict[str, Any] = dict(keys) if hasattr(keys, "items") else dict(keys)
            out.append({"name": name, "keys": key_d, "unique": spec.get("unique", False)})
        return out

    def get_collection_stats(self, database: str, collection: str) -> dict[str, Any] | None:
        db = self._client[database]
        try:
            return dict(db.command("collStats", collection))
        except PyMongoError:
            return None

    def sample_documents(self, database: str, collection: str, n: int) -> list[dict[str, Any]]:
        coll = self._client[database][collection]
        try:
            return list(coll.aggregate([{"$sample": {"size": n}}]))
        except OperationFailure:
            return list(coll.find().limit(n))

    def run_explain(
        self,
        database: str,
        source: str,
        mode: str,
        pipeline: list[dict[str, Any]] | None,
        filter_q: dict[str, Any] | None,
        projection: dict[str, Any] | None,
        projection_limit: int | None,
        sort: list[tuple[str, int]] | None = None,
    ) -> ExplainSnapshot:
        db = self._client[database]
        if mode == "aggregate":
            pipeline = pipeline or []
            cmd = {
                "explain": {"aggregate": source, "pipeline": pipeline, "cursor": {}},
                "verbosity": "executionStats",
            }
        else:
            fq = filter_q or {}
            find_doc: dict[str, Any] = {"find": source, "filter": fq}
            if projection is not None:
                find_doc["projection"] = projection
            if projection_limit is not None:
                find_doc["limit"] = projection_limit
            if sort:
                find_doc["sort"] = {k: v for k, v in sort}
            cmd = {"explain": find_doc, "verbosity": "executionStats"}
        raw = db.command(cmd)
        stats = _extract_execution_stats(raw)
        summary = _summarize_plan(raw)
        return ExplainSnapshot(raw=raw, winning_plan_summary=summary, execution_stats=stats)

    def run_aggregate_timed(
        self,
        database: str,
        source: str,
        pipeline: list[dict[str, Any]],
        max_time_ms: int,
    ) -> TimedRunResult:
        coll = self._client[database][source]
        start = time.perf_counter()
        try:
            cur = coll.aggregate(pipeline, maxTimeMS=max_time_ms, allowDiskUse=True)
            rows = list(cur)
            dur = (time.perf_counter() - start) * 1000
            return TimedRunResult(ok=True, duration_ms=dur, returned_count=len(rows))
        except PyMongoError as e:
            dur = (time.perf_counter() - start) * 1000
            return TimedRunResult(ok=False, duration_ms=dur, error=str(e))

    def run_find_timed(
        self,
        database: str,
        source: str,
        filter_q: dict[str, Any],
        projection: dict[str, Any] | None,
        sort: list[tuple[str, int]] | None,
        limit: int | None,
        max_time_ms: int,
    ) -> TimedRunResult:
        coll = self._client[database][source]
        start = time.perf_counter()
        try:
            cur = coll.find(filter_q or {}, projection=projection)
            if sort:
                cur = cur.sort(sort)
            if limit is not None:
                cur = cur.limit(limit)
            cur = cur.max_time_ms(max_time_ms)
            rows = list(cur)
            dur = (time.perf_counter() - start) * 1000
            return TimedRunResult(ok=True, duration_ms=dur, returned_count=len(rows))
        except PyMongoError as e:
            dur = (time.perf_counter() - start) * 1000
            return TimedRunResult(ok=False, duration_ms=dur, error=str(e))

    def build_truth_bundle(self, database: str, source: str, src: SourceInfo) -> TruthBundle:
        namespace_found, approx = self.namespace_diagnostics(database, source)
        fields, n = self.get_collection_schema(database, source)
        try:
            idx = self.list_indexes(database, source)
        except OperationFailure:
            idx = []
        stats = self.get_collection_stats(database, source)
        return TruthBundle(
            source=src,
            field_types=fields,
            indexes=idx,
            collection_stats=stats,
            sample_doc_count=n,
            namespace_found=namespace_found,
            approximate_document_count=approx,
        )


def _extract_execution_stats(raw: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        cursor = raw.get("cursor") or {}
        first = cursor.get("firstBatch") or []
        if not first:
            return out
        root = first[0]
        stats = root.get("executionStats") or {}
        if not stats:
            stages = root.get("stages") or []
            if stages:
                last = stages[-1]
                if "$cursor" in last:
                    stats = last["$cursor"].get("executionStats") or {}
                else:
                    first_val = next(iter(last.values()))
                    if isinstance(first_val, dict):
                        stats = first_val.get("executionStats") or {}
        for k in (
            "executionTimeMillis",
            "totalDocsExamined",
            "totalKeysExamined",
            "nReturned",
        ):
            if k in stats:
                out[k] = stats[k]
    except (KeyError, TypeError, IndexError, StopIteration):
        pass
    return out


def extract_lookup_stage_stats(raw_explain: dict[str, Any]) -> list[dict[str, Any]]:
    """Best-effort extraction of per-$lookup performance stats from raw explain output.

    Returns a list of dicts with keys: from, executionTimeMillisEstimate, totalDocsExamined.
    """
    results: list[dict[str, Any]] = []
    try:
        cursor = raw_explain.get("cursor") or {}
        first = cursor.get("firstBatch") or []
        if not first:
            return results
        root = first[0] if isinstance(first, list) and first else {}
        stages = root.get("stages") or []
        for stage in stages:
            op = next(iter(stage.keys()), "")
            if op != "$lookup":
                continue
            body = stage[op]
            if not isinstance(body, dict):
                continue
            results.append({
                "from": body.get("from", ""),
                "executionTimeMillisEstimate": body.get("executionTimeMillisEstimate"),
                "totalDocsExamined": body.get("totalDocsExamined"),
                "nReturned": body.get("nReturned"),
            })
    except (KeyError, TypeError, IndexError):
        pass
    return results


def _summarize_plan(raw: dict[str, Any]) -> str | None:
    try:
        cursor = raw.get("cursor") or {}
        first = cursor.get("firstBatch") or []
        if not first:
            return None
        root = first[0]
        stages = root.get("stages") or []
        if not stages:
            return str(root.get("queryPlanner", {}).get("winningPlan", {}).get("stage", ""))
        names = []
        for s in stages[:12]:
            names.extend(list(s.keys()))
        return " → ".join(names)
    except (TypeError, AttributeError):
        return None
