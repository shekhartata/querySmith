"""
Smoke-test the QuerySmith MCP server over stdio (same transport Cursor uses).

Does not replace the official MCP Inspector; it is a quick local check that the
server starts, lists tools, and returns a tool response.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

from mcp import ClientSession, StdioServerParameters, stdio_client
from mcp.types import TextContent


def _err(*args: Any, **kwargs: Any) -> None:
    print(*args, file=sys.stderr, **kwargs)


def _format_tool_result(result: Any) -> str:
    parts: list[str] = []
    if getattr(result, "isError", False):
        parts.append("isError: true")
    for block in getattr(result, "content", []) or []:
        if isinstance(block, TextContent):
            parts.append(block.text)
        else:
            parts.append(str(block))
    if not parts and getattr(result, "structuredContent", None):
        parts.append(json.dumps(result.structuredContent, indent=2, default=str))
    return "\n".join(parts) if parts else "(empty content)"


async def _run(
    *,
    python_exe: str,
    cwd: str | None,
    list_only: bool,
    tool: str,
    arguments_json: str | None,
) -> int:
    params = StdioServerParameters(
        command=python_exe,
        args=["-m", "querysmith.mcp_server"],
        env=os.environ.copy(),
        cwd=cwd or None,
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            _err("=== initialize ===")
            _err(json.dumps(init.model_dump(mode="json"), indent=2, default=str))

            lt = await session.list_tools()
            _err("\n=== tools/list (names) ===")
            names = [t.name for t in lt.tools]
            _err(json.dumps(names, indent=2))

            if list_only:
                _err("\nOK (list-only)")
                return 0

            if arguments_json is not None:
                args_dict: dict[str, Any] = json.loads(arguments_json)
            elif tool == "parse_query":
                args_dict = {"payload": '[{"$limit": 1}]'}
            else:
                _err(f"Error: --tool {tool!r} requires --arguments <json>")
                return 2

            _err(f"\n=== tools/call: {tool} === (stdout below)")
            result = await session.call_tool(tool, args_dict)
            print(_format_tool_result(result))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Spawn QuerySmith MCP over stdio, list tools, and optionally call one tool.",
    )
    p.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable used to run python -m querysmith.mcp_server",
    )
    p.add_argument(
        "--cwd",
        default=None,
        help="Working directory for the server process (defaults to current directory)",
    )
    p.add_argument(
        "--list-only",
        action="store_true",
        help="Only initialize + list_tools (no tools/call)",
    )
    p.add_argument(
        "--tool",
        default="parse_query",
        help="Tool to call (default: parse_query — works without MongoDB)",
    )
    p.add_argument(
        "--arguments",
        default=None,
        help='JSON object of arguments, e.g. \'{"database":"mydb","collection":"c"}\'. '
        "Omitted for parse_query uses a minimal pipeline.",
    )
    ns = p.parse_args(argv)
    try:
        return asyncio.run(
            _run(
                python_exe=ns.python,
                cwd=ns.cwd,
                list_only=ns.list_only,
                tool=ns.tool,
                arguments_json=ns.arguments,
            )
        )
    except KeyboardInterrupt:
        return 130
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in --arguments: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
