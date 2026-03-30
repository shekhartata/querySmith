from __future__ import annotations

import json
from typing import Any

def parse_query_payload(text: str) -> Any:
    text = text.strip()
    if not text:
        raise ValueError("Empty query payload")
    return json.loads(text)


def normalize_aggregate(pipeline: list[dict[str, Any]] | Any) -> list[dict[str, Any]]:
    if not isinstance(pipeline, list):
        raise ValueError("Aggregate pipeline must be a JSON array of stages")
    out: list[dict[str, Any]] = []
    for i, stage in enumerate(pipeline):
        if not isinstance(stage, dict) or len(stage) != 1:
            raise ValueError(f"Stage {i} must be a single-key object (e.g. {{'$match': ...}})")
        out.append(stage)
    return out


def _collect_keys_from_filter(obj: Any, acc: set[str], prefix: str = "") -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.startswith("$") and k not in ("$and", "$or", "$nor", "$not", "$in", "$nin", "$elemMatch"):
                if k in ("$and", "$or", "$nor") and isinstance(v, list):
                    for item in v:
                        _collect_keys_from_filter(item, acc, prefix)
                continue
            if k in ("$and", "$or", "$nor") and isinstance(v, list):
                for item in v:
                    _collect_keys_from_filter(item, acc, prefix)
                continue
            if k.startswith("$"):
                _collect_keys_from_filter(v, acc, prefix)
                continue
            path = f"{prefix}.{k}" if prefix else k
            acc.add(path)
            if isinstance(v, (dict, list)):
                _collect_keys_from_filter(v, acc, path)
    elif isinstance(obj, list):
        for item in obj:
            _collect_keys_from_filter(item, acc, prefix)


def extract_referenced_paths(pipeline: list[dict[str, Any]]) -> set[str]:
    paths: set[str] = set()
    for stage in pipeline:
        op = next(iter(stage.keys()))
        body = stage[op]
        if op == "$match":
            _collect_keys_from_filter(body, paths)
        elif op == "$lookup":
            if isinstance(body, dict):
                for key in ("localField", "foreignField", "as"):
                    if key in body and isinstance(body[key], str):
                        paths.add(body[key])
                if "pipeline" in body and isinstance(body["pipeline"], list):
                    paths |= extract_referenced_paths(body["pipeline"])
        elif op in ("$project", "$addFields", "$set"):
            if isinstance(body, dict):
                for k, v in body.items():
                    if not str(k).startswith("$"):
                        paths.add(str(k))
                    if isinstance(v, dict):
                        _collect_keys_from_filter(v, paths)
        elif op == "$group":
            if isinstance(body, dict):
                if "_id" in body:
                    _collect_keys_from_filter(body["_id"], paths)
                for k, v in body.items():
                    if k == "_id":
                        continue
                    _collect_keys_from_filter(v, paths)
        elif op == "$sort" and isinstance(body, dict):
            paths.update(body.keys())
        elif op == "$unwind" and isinstance(body, dict):
            p = body.get("path")
            if isinstance(p, str) and p.startswith("$"):
                paths.add(p[1:])
        elif op == "$replaceRoot" and isinstance(body, dict):
            _collect_keys_from_filter(body.get("newRoot"), paths)
    return paths


def stage_order_summary(pipeline: list[dict[str, Any]]) -> list[str]:
    return [next(iter(s.keys())) for s in pipeline]


def first_stage_index(pipeline: list[dict[str, Any]], op: str) -> int | None:
    for i, s in enumerate(pipeline):
        if next(iter(s.keys())) == op:
            return i
    return None


def _path_exists_in_schema(path: str, schema_paths: set[str]) -> bool:
    if path in schema_paths:
        return True
    for k in schema_paths:
        if k.startswith(path + "."):
            return True
    parts = path.split(".")
    for i in range(len(parts) - 1):
        parent = ".".join(parts[: i + 1])
        if parent in schema_paths:
            return True
    return False


def validate_paths_against_schema(paths: set[str], schema_paths: set[str]) -> list[str]:
    missing: list[str] = []
    for p in sorted(paths):
        if not p or p.startswith("$"):
            continue
        if _path_exists_in_schema(p, schema_paths):
            continue
        missing.append(p)
    return missing


def schema_path_set(field_infos: list[Any]) -> set[str]:
    return {f.path for f in field_infos}


def literal_type_name(value: Any) -> str | None:
    """BSON-style type label for a JSON/Python literal used in a query."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return None


def _walk_match_equality_literals(obj: Any, out: list[tuple[str, Any]], prefix: str = "") -> None:
    """Collect (field_path, value) for equality predicates in a match filter."""
    if not isinstance(obj, dict):
        return
    for k, v in obj.items():
        if k in ("$and", "$or", "$nor"):
            if isinstance(v, list):
                for item in v:
                    _walk_match_equality_literals(item, out, prefix)
            continue
        if k.startswith("$"):
            continue
        path = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict) and v:
            keys = {str(x) for x in v.keys()}
            if keys == {"$eq"}:
                out.append((path, v["$eq"]))
            elif keys == {"$in"} and isinstance(v.get("$in"), list) and v["$in"]:
                out.append((path, v["$in"][0]))
            elif all(str(x).startswith("$") for x in v.keys()):
                continue
            else:
                _walk_match_equality_literals(v, out, path)
        else:
            out.append((path, v))


def extract_match_equality_literals(pipeline: list[dict[str, Any]]) -> list[tuple[str, Any]]:
    """Equality literals from all $match stages (top-level and nested $lookup pipelines omitted for v1)."""
    out: list[tuple[str, Any]] = []
    for stage in pipeline:
        op = next(iter(stage.keys()))
        if op != "$match":
            continue
        body = stage.get(op)
        if isinstance(body, dict):
            _walk_match_equality_literals(body, out, "")
    return out


def get_nested_value(doc: dict[str, Any], path: str) -> Any:
    """Read a dotted field path from a document (missing path -> None)."""
    cur: Any = doc
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def collect_distinct_exemplars(
    docs: list[dict[str, Any]],
    paths: list[str],
    *,
    max_per_path: int = 10,
) -> dict[str, list[Any]]:
    """Up to `max_per_path` distinct example values per path from sample documents."""
    out: dict[str, list[Any]] = {p: [] for p in paths}
    seen: dict[str, set[str]] = {p: set() for p in paths}

    def _key(v: Any) -> str:
        try:
            return json.dumps(v, sort_keys=True, default=str)
        except (TypeError, ValueError):
            return repr(v)

    for doc in docs:
        for p in paths:
            if len(out[p]) >= max_per_path:
                continue
            v = get_nested_value(doc, p)
            k = _key(v)
            if k in seen[p]:
                continue
            seen[p].add(k)
            out[p].append(v)
    return out


def literal_compatible_with_sampled_types(literal_type: str, schema_types: set[str]) -> bool:
    """Whether a query literal type can plausibly match documents given sampled BSON types."""
    if not schema_types:
        return True
    if literal_type in schema_types:
        return True
    nums = {"int", "long", "double", "decimal"}
    if literal_type == "int" and schema_types & nums:
        return True
    if literal_type == "double" and schema_types & nums:
        return True
    if literal_type == "bool" and "bool" in schema_types:
        return True
    if literal_type == "null" and "null" in schema_types:
        return True
    if literal_type == "string" and schema_types <= {"objectId"}:
        return True
    return False
