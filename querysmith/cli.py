from __future__ import annotations

import argparse
import json
import sys

from querysmith.config import load_settings
from querysmith.models import QueryInput
from querysmith.orchestrator import run_v1
from querysmith.pipeline_parse import parse_query_payload


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="querysmith")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="Run full V1 optimization workflow")
    run.add_argument("--database", required=True)
    run.add_argument("--source", required=True, help="Collection or view name")
    run.add_argument("--mode", choices=("aggregate", "find"), default="aggregate")
    run.add_argument("--pipeline", default=None, help="JSON array of aggregation stages")
    run.add_argument("--filter", default=None, help="JSON object for find mode")
    run.add_argument("--projection", default=None, help="JSON projection for find mode")
    run.add_argument(
        "--sort",
        default=None,
        help='JSON array of [field, direction] pairs for find mode, e.g. \'[["createdAt", -1]]\'',
    )
    run.add_argument("--limit", type=int, default=None)
    run.add_argument("--max-time-ms", type=int, default=None)
    run.add_argument("--json", action="store_true", help="Emit JSON report to stdout")

    mcp_test = sub.add_parser(
        "mcp-test",
        help="Spawn MCP server (stdio), list tools, and optionally call one tool",
    )
    mcp_test.add_argument(
        "--python",
        default=None,
        help="Python executable for the server (default: same as this CLI)",
    )
    mcp_test.add_argument("--cwd", default=None, help="Server process working directory")
    mcp_test.add_argument(
        "--list-only",
        action="store_true",
        help="Only initialize + list_tools",
    )
    mcp_test.add_argument(
        "--tool",
        default="parse_query",
        help="Tool to call (default: parse_query, no MongoDB)",
    )
    mcp_test.add_argument(
        "--arguments",
        default=None,
        help="JSON object of tool arguments (required for tools other than default parse_query)",
    )

    args = p.parse_args(argv)
    if args.cmd == "mcp-test":
        from querysmith import mcp_test as mcp_test_mod

        return mcp_test_mod.main(
            [
                *(["--python", args.python] if args.python else []),
                *(["--cwd", args.cwd] if args.cwd else []),
                *(["--list-only"] if args.list_only else []),
                "--tool",
                args.tool,
                *(["--arguments", args.arguments] if args.arguments else []),
            ]
        )
    if args.cmd == "run":
        pipeline = None
        filt = None
        projection = None
        sort_pairs = None
        if args.mode == "aggregate":
            if not args.pipeline:
                print("aggregate mode requires --pipeline", file=sys.stderr)
                return 2
            pipeline = parse_query_payload(args.pipeline)
            if not isinstance(pipeline, list):
                print("pipeline must be a JSON array", file=sys.stderr)
                return 2
        else:
            if args.filter:
                filt = parse_query_payload(args.filter)
                if not isinstance(filt, dict):
                    print("filter must be a JSON object", file=sys.stderr)
                    return 2
            if args.projection:
                projection = parse_query_payload(args.projection)
                if not isinstance(projection, dict):
                    print("projection must be a JSON object", file=sys.stderr)
                    return 2
            if args.sort:
                raw_sort = parse_query_payload(args.sort)
                if not isinstance(raw_sort, list):
                    print("sort must be a JSON array", file=sys.stderr)
                    return 2
                sort_pairs = []
                for item in raw_sort:
                    if not isinstance(item, (list, tuple)) or len(item) != 2:
                        print("each sort entry must be [field, direction]", file=sys.stderr)
                        return 2
                    sort_pairs.append((str(item[0]), int(item[1])))

        q = QueryInput(
            database=args.database,
            source=args.source,
            mode=args.mode,
            pipeline=pipeline,
            filter=filt,
            projection=projection,
            sort=sort_pairs,
            limit=args.limit,
            max_time_ms=args.max_time_ms,
        )
        report, md = run_v1(q, load_settings())
        if args.json:
            print(json.dumps(report.model_dump(), default=str, indent=2))
        else:
            print(md)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
