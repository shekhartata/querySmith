# QuerySmith

An MCP-powered AI system for validating, profiling, optimizing, and benchmarking MongoDB queries using a **deterministic rule engine** + **LLM planner** + **safe execution controls**.

QuerySmith is not "a chatbot that gives query tips." It is a controlled optimization workbench that:

1. Establishes **schema and execution truth** from real data
2. Applies a **codified optimization playbook** (26 deterministic rules)
3. Generates **candidate rewrites** via an optional LLM
4. Produces a **structured recommendation** with evidence

---

## Table of contents

- [Setup](#setup)
- [Environment (.env)](#environment-env)
- [CLI usage](#cli-usage)
- [MCP server](#mcp-server)
- [MCP tools reference](#mcp-tools-reference)
- [Rule engine](#rule-engine)
- [V1 workflow (end-to-end)](#v1-workflow-end-to-end)
- [Architecture](#architecture)
- [V1 vs V2 checklist](#v1-vs-v2-checklist)

---

## Setup

```bash
cd querySmithProj
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
```

Requires **Python 3.11+** and a reachable **MongoDB** instance.

---

## Environment (`.env`)

1. Copy the template: `cp .env.example .env`
2. Edit `.env` and set at least **`MONGODB_URI`**.

Settings are loaded from **`.env` in the project root** (next to `pyproject.toml`), not from the shell's current directory. Both CLI and MCP read the same file.

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `MONGODB_URI` | **Yes** | `mongodb://localhost:27017` | Connection string for all DB operations. |
| `OPENAI_API_KEY` | No | — | Enables LLM-assisted rewrite suggestions. |
| `QUERYSMITH_OPENAI_BASE_URL` | No | `https://api.openai.com/v1` | OpenAI-compatible base URL (Azure, Ollama, LiteLLM, etc.). |
| `QUERYSMITH_LLM_MODEL` | No | `gpt-4o-mini` | Model identifier for the LLM planner. |
| `QUERYSMITH_ENV` | No | `dev` | Environment label on reports: `dev` \| `uat` \| `prod`. |
| `QUERYSMITH_DEFAULT_TIMEOUT_MS` | No | `30000` | Default `maxTimeMS` for bounded execution. |
| `QUERYSMITH_SAMPLE_SIZE` | No | `80` | Documents sampled for schema inference. |
| `QUERYSMITH_MAX_PIPELINE_STAGES_WARN` | No | `25` | Stage count threshold for PIPE-01 warning. |
| `QUERYSMITH_VIEW_FLATTEN_TIMEOUT_MS` | No | `60000` | If a query exceeds this (ms) and the source is a view, triggers view flattening. |

**Cursor MCP note:** The MCP server process does not inherit Cursor's UI-level settings. Duplicate `MONGODB_URI` / `OPENAI_API_KEY` into **Settings → MCP → QuerySmith → Environment**, or ensure the `.env` file is populated before starting the server.

---

## CLI usage

### Full V1 optimization (aggregate)

```bash
querysmith run \
  --database mydb \
  --source orders \
  --mode aggregate \
  --pipeline '[{"$match": {"status": "A"}}, {"$limit": 10}]'
```

### Full V1 optimization (find)

```bash
querysmith run \
  --database mydb \
  --source orders \
  --mode find \
  --filter '{"status": "A"}' \
  --sort '[["createdAt", -1]]' \
  --limit 50
```

### JSON output

```bash
querysmith run --database mydb --source orders --mode aggregate \
  --pipeline '[{"$match": {"status": "A"}}]' --json
```

### All CLI flags

| Flag | Description |
|------|-------------|
| `--database` | Database name (required). |
| `--source` | Collection or view name (required). |
| `--mode` | `aggregate` or `find` (default `aggregate`). |
| `--pipeline` | JSON array of aggregation stages (required for aggregate). |
| `--filter` | JSON filter object (find mode). |
| `--projection` | JSON projection object (find mode). |
| `--sort` | JSON array of `[field, direction]` pairs (find mode). |
| `--limit` | Result limit (find mode). |
| `--max-time-ms` | Override default timeout. |
| `--json` | Emit structured JSON report instead of markdown. |

**Important:** `--pipeline` must be **valid JSON** (all keys in double quotes). MongoDB shell syntax like `{ $match: ... }` is not JSON. Always use `'[{"$match": ...}]'` with single quotes around the argument.

---

## MCP server

### Start (stdio transport)

```bash
python -m querysmith.mcp_server
```

### Register in Cursor

- **Command:** `/path/to/querySmithProj/.venv/bin/python`
- **Args:** `-m`, `querysmith.mcp_server`
- **Environment:** set `MONGODB_URI`, optionally `OPENAI_API_KEY`

### Smoke-test

```bash
# List tools (no MongoDB needed)
querysmith mcp-test --list-only

# Call parse_query (no MongoDB needed)
querysmith mcp-test

# Call a DB tool
querysmith mcp-test --tool list_indexes \
  --arguments '{"database":"mydb","collection":"orders"}'

# Call full V1 optimization
querysmith mcp-test --tool run_v1_optimization \
  --arguments '{"database":"mydb","source":"orders","mode":"aggregate","query_json":"[{\"$match\":{\"status\":\"A\"}}]"}'
```

Diagnostics go to stderr; the tool response body is printed on stdout.

---

## MCP tools reference

| Tool | Parameters | Description |
|------|-----------|-------------|
| `parse_query` | `payload` (JSON string) | Parse and normalize a pipeline or filter. |
| `get_collection_schema` | `database`, `collection` | Sampled field paths, BSON types, namespace check, estimated count. |
| `get_view_definition` | `database`, `view` | Resolve collection vs view; return view pipeline if applicable. |
| `get_field_types` | `database`, `collection` | Inferred field types from sampled documents. |
| `list_indexes` | `database`, `collection` | Index list for a namespace (surfaces errors instead of empty `[]`). |
| `get_collection_stats` | `database`, `collection` | `collStats` for a namespace. |
| `run_explain` | `database`, `source`, `mode`, `query_json` | `explain` with `executionStats` verbosity. |
| `run_query_with_timeout` | `database`, `source`, `mode`, `query_json`, `max_time_ms` | Bounded aggregate or find with `maxTimeMS`. |
| `sample_documents` | `database`, `collection`, `n` | Return up to n sampled documents. |
| **`run_v1_optimization`** | `database`, `source`, `mode`, `query_json`, `max_time_ms?` | **Full V1 pipeline**: truth → rules → explain → timed run → LLM → report. |

---

## Rule engine

The rule engine implements **26 deterministic rules** derived from the optimization playbook (design doc §8–9). Rules fire before the LLM and constrain its suggestions.

### A. Schema truth

| Rule | ID | Severity | What it checks |
|------|-----|----------|----------------|
| 1 | **SCH-01** | warn | Referenced field not found in sampled schema. |
| 2 | **SCH-03** | warn | Mixed types on a field (e.g. string + int). |
| 3 | **SCH-04** | error | Filter literal type ≠ sampled field type (e.g. `"1"` vs `int`). |
| 4 | **SCH-04b** | warn | `$lookup` join-key type mismatch (localField vs foreignField). |

### B. Source selection

| Rule | ID | Severity | What it checks |
|------|-----|----------|----------------|
| 5 | **SRC-05** | warn | Source is a standard view; optimize on base collections. |
| 6 | **SRC-06** | error | View definition could not be loaded. |

### C. Pipeline ordering

| Rule | ID | Severity | What it checks |
|------|-----|----------|----------------|
| 8 | **ORD-08** | warn | `$match` appears after `$lookup`; push earlier. |
| 8b | **ORD-08b** | info | `$match` appears after `$group`. |
| 9 | **ORD-09** | info | Adjacent `$match` stages that could be merged. |
| 10 | **ORD-10** | warn | Expensive stage runs before any `$match` (no cardinality reduction). |

### D. Lookup

| Rule | ID | Severity | What it checks |
|------|-----|----------|----------------|
| 11 | **LKP-11** | info | Correlated pipeline `$lookup` (has `let`); check if equality join suffices. |
| 12 | **LKP-12** | warn | Uncorrelated pipeline `$lookup` (no `let`); convert to localField/foreignField. |
| 14 | **LKP-14** | warn | Fan-out risk: `$lookup` target has high estimated document count. |

### E. Projection

| Rule | ID | Severity | What it checks |
|------|-----|----------|----------------|
| 15–16 | **PRJ-15** | info | No `$project`/`$unset` before expensive stage; wide documents in pipeline. |

### F. Unwind / group

| Rule | ID | Severity | What it checks |
|------|-----|----------|----------------|
| 18 | **UW-18b** | warn | Cascaded (back-to-back) `$unwind` stages. |
| 19 | **UW-19** | info | `$unwind` multiplies documents; verify array is bounded. |
| 20 | **UW-20** | warn | `$group` immediately after `$unwind` (re-aggregation pattern). |

### G. Pagination / boundedness

| Rule | ID | Severity | What it checks |
|------|-----|----------|----------------|
| 21 | **BD-21** | info | No `$limit` or `$sample`; unbounded result. |
| 22 | **BD-22** | warn | `$skip` without `$limit`; prefer keyset pagination. |
| 23 | **BD-23** | info | Unbounded + sorted on indexed key → suggest cursor pagination. |

### H. Index

| Rule | ID | Severity | What it checks |
|------|-----|----------|----------------|
| 24 | **IDX-24** | info/warn | No user indexes on namespace (with view-aware messaging). |
| 24b | **IDX-24b** | warn | Filter/sort fields share no leading prefix with any existing index. |
| 25 | **IDX-25** | info | Partial index prefix overlap; compound index may help. |
| 26 | **IDX-26** | — | Index recommendations deprioritized when structural rewrites pending. |

### Runtime (post-execution)

| Rule | ID | Severity | What it checks |
|------|-----|----------|----------------|
| — | **RUN-01** | warn/info | Timed run returned 0 docs; compares filter literals to sampled values. |

### I. Safety (by design)

| Rule | Status | Notes |
|------|--------|-------|
| 27 | Enforced | No automatic prod index creation in V1. |
| 28 | Enforced | No autonomous writes on prod. |
| 29 | Enforced | No dropping indexes. |
| 30 | Planned (V2) | Every rewrite benchmarked before recommendation. |

---

## V1 workflow (end-to-end)

```
Input (query + db + source + mode)
  │
  ├─ Phase 1: Ingest — parse, normalize, validate JSON
  ├─ Phase 2: Truth — resolve source (coll/view), sample schema, indexes, stats
  ├─ Phase 3: Static analysis — 26 deterministic rules
  ├─ Phase 4: Explain + timed run — executionStats + bounded execution
  ├─ Phase 4b: Zero-result diagnostics — compare literals to sampled values
  ├─ Phase 5: LLM planner — optional rewrite (constrained by rule findings)
  ├─ Phase 6: Index recommendations — aligned to filter/sort, deprioritized if rewrites pending
  └─ Phase 7: Report — structured JSON + markdown memo
```

The report includes: executive summary, namespace/sampling info, suspected bottlenecks, rule findings, explain/runtime stats, suggested candidate (with rationale, confidence, risks), index recommendations, risk notes.

---

## Architecture

```
┌──────────────────────────────────────────────┐
│                   Transport                   │
│   MCP Server (FastMCP, stdio)  ·  CLI (argparse) │
└────────────────────┬─────────────────────────┘
                     │
┌────────────────────▼─────────────────────────┐
│               Orchestrator                    │
│            run_v1() — end-to-end              │
└──┬─────┬─────┬──────┬──────┬─────┬───────────┘
   │     │     │      │      │     │
   ▼     ▼     ▼      ▼      ▼     ▼
 Parse  Mongo  Rules  LLM  Index  Report
       Client  Engine Plan  Recs   Builder
```

| Module | File | Responsibility |
|--------|------|----------------|
| Config | `config.py` | `.env` loading, Pydantic settings |
| Models | `models.py` | Pydantic models (input, truth, findings, explain, report) |
| Mongo client | `mongo_client.py` | PyMongo: schema, indexes, stats, explain, timed runs |
| Parser | `pipeline_parse.py` | JSON parse, field extraction, literal analysis |
| Rules | `rules_engine.py` | 26 deterministic playbook rules |
| LLM | `llm_planner.py` | OpenAI-compatible chat completions (optional) |
| Report | `report_builder.py` | V1Report model + markdown rendering |
| Orchestrator | `orchestrator.py` | Wires all phases into `run_v1()` |
| MCP server | `mcp_server.py` | 10 MCP tools (stdio transport) |
| CLI | `cli.py` | `querysmith run` + `querysmith mcp-test` |
| MCP test | `mcp_test.py` | Stdio client for smoke-testing the MCP server |

---

## View flattening

QuerySmith automatically detects when views are causing performance problems and generates optimized alternatives.

### Branch 1: Source view timed out

When a query against a view exceeds `QUERYSMITH_VIEW_FLATTEN_TIMEOUT_MS` (default 60s) or times out:

1. Recursively resolves the view chain to the base collection (handles nested views)
2. Concatenates the view pipeline + user query pipeline into a single flattened pipeline
3. Asks the LLM to prune unnecessary view stages (e.g. a 17-stage view might only need 10-12 stages for this specific query)
4. Reports the pruned pipeline as an alternative that targets the base collection directly

**Rule finding:** `VF-01` (severity: warn)

### Branch 2: Slow `$lookup` targets a view

When the explain plan identifies a slow `$lookup` stage (high `executionTimeMillisEstimate` or `totalDocsExamined`) and that lookup's target collection is actually a view:

1. Resolves the view to its base collection
2. Generates a rewritten pipeline where the `$lookup` targets the base collection with only the necessary view stages inlined as a sub-pipeline
3. Fast lookups on views are left untouched — only slow ones trigger flattening

**Rule finding:** `VF-02` (severity: warn)

### Safety

All view-flattened suggestions include a safety warning: verify result equivalence (row count + sample row comparison) before adopting in production.

---

## V1 vs V2 checklist

### V1 — Diagnostic + Assisted Optimization (current)

| Feature | Status |
|---------|--------|
| Query / pipeline ingestion (aggregate + find) | ✅ Implemented |
| Source detection: collection vs view | ✅ Implemented |
| View pipeline fetch + warning | ✅ Implemented |
| Schema inference from sampled documents | ✅ Implemented |
| Field type validation | ✅ Implemented |
| Literal-vs-schema type mismatch (SCH-04) | ✅ Implemented |
| Join-key type cross-check (SCH-04b) | ✅ Implemented |
| 26 deterministic playbook rules | ✅ Implemented |
| Explain plan capture (executionStats) | ✅ Implemented |
| Bounded timed execution (maxTimeMS) | ✅ Implemented |
| Zero-result diagnostics with sample comparison (RUN-01) | ✅ Implemented |
| Namespace existence + estimated document count | ✅ Implemented |
| Anti-pattern detection (ordering, projection, unwind, lookup) | ✅ Implemented |
| Single LLM rewrite suggestion (constrained by rules) | ✅ Implemented |
| Index recommendations (with rewrite-first deprioritization) | ✅ Implemented |
| Structured report (JSON + markdown) | ✅ Implemented |
| MCP server (10 tools, stdio transport) | ✅ Implemented |
| CLI (`querysmith run` + `querysmith mcp-test`) | ✅ Implemented |
| No automatic prod writes | ✅ Enforced by design |
| View flattening: source view timed out → prune & target base collection (VF-01) | ✅ Implemented |
| View flattening: slow `$lookup` on view → inline view stages (VF-02) | ✅ Implemented |
| Recursive view chain resolution (view → view → base) | ✅ Implemented |
| Per-lookup explain stats extraction | ✅ Implemented |
| Explain on **suggested** (rewritten) pipeline | ❌ Not yet |
| Audit log (persist runs) | ❌ Not yet |
| MCP resources (schema://, indexes://, playbook://) | ❌ Not yet |
| MCP prompts (optimize_aggregation, etc.) | ❌ Not yet |

### V2 — Benchmarking + Candidate Ranking (planned)

| Feature | Status |
|---------|--------|
| Multi-candidate rewrite generation (2–5 variants) | 🔲 Planned |
| Benchmark harness (explain + timed run per candidate) | 🔲 Planned |
| Before/after metrics comparison table | 🔲 Planned |
| Result verification (row count, sample row compare, hash) | 🔲 Planned |
| Candidate scoring model (perf 35%, safety 25%, ops 15%, simplicity 15%, index dep 10%) | 🔲 Planned |
| Candidate ranking (best + runners-up) | 🔲 Planned |
| Non-prod test index creation (`create_test_index`) | 🔲 Planned |
| Non-prod test index cleanup (`drop_test_index`) | 🔲 Planned |
| Explain-plan delta (original vs each candidate) | 🔲 Planned |
| Index delta (what each candidate needs) | 🔲 Planned |
| Semantic-risk delta per candidate | 🔲 Planned |
| Final recommendation pack | 🔲 Planned |

### Phase 3 — Advanced (planned)

| Feature | Status |
|---------|--------|
| Base collection flipping engine (alternate root suggestion) | 🔲 Planned |
| Collection relationship / join graph | 🔲 Planned |
| View taxonomy (thin, join-heavy, nested chain, materialization candidate) | 🔲 Planned |
| Materialized view recommendations | 🔲 Planned |
| Learned heuristics from past optimization wins | 🔲 Planned |
| Recurring-query memory (normalized signatures) | 🔲 Planned |
| App-integration suggestions | 🔲 Planned |

---

## License

Internal tool. Not published.
