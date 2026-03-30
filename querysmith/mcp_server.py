from __future__ import annotations

import json
from typing import Any

from mcp.server.fastmcp import FastMCP
from pymongo.errors import OperationFailure, PyMongoError

from querysmith.config import load_settings
from querysmith.models import QueryInput
from querysmith.mongo_client import MongoService
from querysmith.orchestrator import run_v1
from querysmith.pipeline_parse import normalize_aggregate, parse_query_payload

mcp = FastMCP("querysmith")


def _json(obj: Any) -> str:
    return json.dumps(obj, default=str, indent=2)


@mcp.tool()
def parse_query(payload: str) -> str:
    """Parse a JSON query payload (pipeline array or find filter object) and return normalized JSON."""
    data = parse_query_payload(payload)
    if isinstance(data, list):
        data = normalize_aggregate(data)
    return _json(data)


@mcp.tool()
def get_collection_schema(database: str, collection: str) -> str:
    """Return inferred field paths, observed BSON types, and sample counts for a collection or view."""
    settings = load_settings()
    svc = MongoService(settings)
    try:
        found, approx = svc.namespace_diagnostics(database, collection)
        fields, n = svc.get_collection_schema(database, collection)
        return _json(
            {
                "fields": [f.model_dump() for f in fields],
                "sample_count": n,
                "namespace_found": found,
                "approximate_document_count": approx,
            }
        )
    finally:
        svc.close()


@mcp.tool()
def get_view_definition(database: str, view: str) -> str:
    """Return view metadata including the backing aggregation pipeline when the source is a view."""
    settings = load_settings()
    svc = MongoService(settings)
    try:
        src = svc.resolve_source(database, view)
        return _json(src.model_dump())
    finally:
        svc.close()


@mcp.tool()
def get_field_types(database: str, collection: str) -> str:
    """Infer field paths and observed BSON types from sampled documents."""
    settings = load_settings()
    svc = MongoService(settings)
    try:
        found, approx = svc.namespace_diagnostics(database, collection)
        fields, n = svc.get_collection_schema(database, collection)
        return _json(
            {
                "fields": [f.model_dump() for f in fields],
                "sample_count": n,
                "namespace_found": found,
                "approximate_document_count": approx,
            }
        )
    finally:
        svc.close()


@mcp.tool()
def list_indexes(database: str, collection: str) -> str:
    """List indexes for a namespace."""
    settings = load_settings()
    svc = MongoService(settings)
    try:
        return _json(svc.list_indexes(database, collection))
    except OperationFailure as e:
        return _json(
            {
                "error": str(e),
                "code": getattr(e, "code", None),
                "details": getattr(e, "details", None),
                "indexes": [],
                "hint": "Often auth, wrong cluster in MONGODB_URI, or missing listIndexes privilege. Cursor Rules do not block this.",
            }
        )
    except PyMongoError as e:
        return _json({"error": str(e), "indexes": []})
    finally:
        svc.close()


@mcp.tool()
def get_collection_stats(database: str, collection: str) -> str:
    """Return collStats for a namespace (may be limited on views)."""
    settings = load_settings()
    svc = MongoService(settings)
    try:
        stats = svc.get_collection_stats(database, collection)
        return _json(stats or {})
    finally:
        svc.close()


@mcp.tool()
def run_explain(
    database: str,
    source: str,
    mode: str,
    query_json: str,
) -> str:
    """Run explain with executionStats. query_json must be a pipeline array (aggregate) or filter object (find)."""
    settings = load_settings()
    svc = MongoService(settings)
    try:
        data = parse_query_payload(query_json)
        if mode == "aggregate":
            pipeline = normalize_aggregate(data)
            snap = svc.run_explain(database, source, "aggregate", pipeline, None, None, None)
        else:
            filt = data if isinstance(data, dict) else {}
            snap = svc.run_explain(database, source, "find", None, filt, None, None)
        return _json({"execution_stats": snap.execution_stats, "summary": snap.winning_plan_summary, "raw": snap.raw})
    finally:
        svc.close()


@mcp.tool()
def run_query_with_timeout(
    database: str,
    source: str,
    mode: str,
    query_json: str,
    max_time_ms: int,
) -> str:
    """Execute aggregate or find with maxTimeMS guardrail."""
    settings = load_settings()
    svc = MongoService(settings)
    try:
        data = parse_query_payload(query_json)
        if mode == "aggregate":
            pipeline = normalize_aggregate(data)
            res = svc.run_aggregate_timed(database, source, pipeline, max_time_ms)
        else:
            filt = data if isinstance(data, dict) else {}
            res = svc.run_find_timed(database, source, filt, None, None, None, max_time_ms)
        return _json(res.model_dump())
    finally:
        svc.close()


@mcp.tool()
def sample_documents(database: str, collection: str, n: int) -> str:
    """Return up to n sampled documents."""
    settings = load_settings()
    svc = MongoService(settings)
    try:
        docs = svc.sample_documents(database, collection, n)
        return _json(docs)
    finally:
        svc.close()


@mcp.tool()
def run_v1_optimization(
    database: str,
    source: str,
    mode: str,
    query_json: str,
    max_time_ms: int | None = None,
) -> str:
    """End-to-end V1: schema truth, rules, explain, timed run, optional LLM rewrite, index ideas, full report."""
    data = parse_query_payload(query_json)
    if mode == "aggregate":
        pipeline = normalize_aggregate(data)
        q = QueryInput(
            database=database,
            source=source,
            mode="aggregate",
            pipeline=pipeline,
            max_time_ms=max_time_ms,
        )
    else:
        filt = data if isinstance(data, dict) else {}
        q = QueryInput(
            database=database,
            source=source,
            mode="find",
            filter=filt,
            max_time_ms=max_time_ms,
        )
    report, md = run_v1(q, load_settings())
    return _json({"report": report.model_dump(), "markdown": md})


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
