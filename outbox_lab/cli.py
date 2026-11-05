"""Command-line interface with payload-free operational diagnostics."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence

from outbox_lab.demo import format_demo_text, run_demo_scenario
from outbox_lab.errors import OutboxError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="outbox_lab", description="Deterministic SQLite Transactional Outbox laboratory."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Run the offline multi-worker scenario.")
    demo.add_argument("--format", choices=("json", "text"), default="json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:
        status = exc.code if isinstance(exc.code, int) else 2
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except OSError:
                _retire_failed_writer(stream)
                return 1
        return status
    try:
        report = run_demo_scenario()
        print(format_demo_text(report) if args.format == "text" else json.dumps(report, indent=2), flush=True)
        return 0
    except OutboxError as exc:
        error = exc.error_code
    except OSError:
        _retire_failed_writer(sys.stdout)
        error = "io_error"
    except AssertionError:
        error = "invariant_failed"
    try:
        print(json.dumps({"error": error, "message": "Demo could not complete."}), file=sys.stderr, flush=True)
    except OSError:
        _retire_failed_writer(sys.stderr)
    return 1


def _retire_failed_writer(writer: object) -> None:
    """Keep shutdown from flushing the same failed OS buffer a second time."""
    try:
        with open(os.devnull, "wb") as sink:
            os.dup2(sink.fileno(), writer.fileno())
    except (AttributeError, OSError, ValueError):
        # In-memory writers belong to the caller and have no OS buffer to retire.
        pass
