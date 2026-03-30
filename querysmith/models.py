from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class QueryInput(BaseModel):
    database: str
    source: str
    mode: Literal["aggregate", "find"]
    pipeline: list[dict[str, Any]] | None = None
    filter: dict[str, Any] | None = None
    projection: dict[str, Any] | None = None
    sort: list[tuple[str, int]] | None = None
    limit: int | None = None
    max_time_ms: int | None = None


class SourceInfo(BaseModel):
    name: str
    kind: Literal["collection", "view"]
    view_pipeline: list[dict[str, Any]] | None = None


class FieldTypeInfo(BaseModel):
    path: str
    types: list[str] = Field(default_factory=list)
    nullable: bool = False


class TruthBundle(BaseModel):
    source: SourceInfo
    field_types: list[FieldTypeInfo] = Field(default_factory=list)
    indexes: list[dict[str, Any]] = Field(default_factory=list)
    collection_stats: dict[str, Any] | None = None
    sample_doc_count: int = 0
    #: True if this name appears in list_collection_names (wrong name / wrong DB => False).
    namespace_found: bool = True
    #: estimated_document_count when available; helps interpret 0 sampled docs.
    approximate_document_count: int | None = None


class RuleFinding(BaseModel):
    rule_id: str
    category: str
    severity: Literal["info", "warn", "error"]
    message: str
    evidence: dict[str, Any] = Field(default_factory=dict)


class ExplainSnapshot(BaseModel):
    raw: dict[str, Any]
    winning_plan_summary: str | None = None
    execution_stats: dict[str, Any] = Field(default_factory=dict)


class TimedRunResult(BaseModel):
    ok: bool
    duration_ms: float
    returned_count: int | None = None
    error: str | None = None
    truncated: bool = False


class LLMSuggestion(BaseModel):
    suggested_pipeline: list[dict[str, Any]] | None = None
    suggested_find: dict[str, Any] | None = None
    rationale: str = ""
    confidence: float = 0.0
    risks: list[str] = Field(default_factory=list)
    expected_gain: str = ""
    skipped_reason: str | None = None


class IndexRecommendation(BaseModel):
    keys: dict[str, Any]
    options: dict[str, Any] = Field(default_factory=dict)
    rationale: str
    alignment: list[str] = Field(default_factory=list)


class V1Report(BaseModel):
    issue_summary: str
    suspected_bottlenecks: list[str]
    rule_violations: list[RuleFinding]
    explain_interpretation: str
    optimized_candidate: LLMSuggestion | None = None
    index_recommendations: list[IndexRecommendation]
    risk_notes: list[str]
    confidence_rating: str
    environment: str
    original_query_summary: str
    metadata: dict[str, Any] = Field(default_factory=dict)
