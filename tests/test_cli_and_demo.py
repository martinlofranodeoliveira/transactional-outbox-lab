"""Offline demo and CLI regressions, including diagnostic redaction."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from outbox_lab import OutboxConstraintError
from outbox_lab.cli import main
from outbox_lab.demo import format_demo_text, run_demo_scenario


class CliAndDemoTests(unittest.TestCase):
    @unittest.skipUnless(os.path.exists("/dev/full"), "requires a failing POSIX output device")
    def test_real_output_and_diagnostic_failures_remain_operational_errors(self) -> None:
        command = [sys.executable, "-m", "outbox_lab"]
        with open("/dev/full", "wb") as full:
            for arguments in (["demo"], ["--help"]):
                with self.subTest(arguments=arguments):
                    result = subprocess.run(command + arguments, stdout=full, stderr=subprocess.PIPE,
                                            timeout=5, check=False)
                    self.assertEqual(result.returncode, 1)
                    self.assertNotIn(b"Traceback", result.stderr)
                    self.assertNotIn(b"Exception ignored", result.stderr)
            result = subprocess.run(command + ["invalid-subcommand"], stdout=subprocess.PIPE, stderr=full,
                                    timeout=5, check=False)
            self.assertEqual((result.returncode, result.stdout), (1, b""))

    def test_demo_scenario_is_deterministic_and_verifies_all_invariants(self) -> None:
        first = run_demo_scenario()
        second = run_demo_scenario()
        self.assertEqual(first, second)
        self.assertTrue(first["invariants"]["all_invariants_passed"])
        self.assertEqual(first["final_clock_ms"], 2300)
        self.assertEqual(first["last_fencing_token"], 6)
        deliveries = [d for d in first["broker_deliveries"] if d["message_id"] == "msg-invoice-202"]
        self.assertEqual(len(deliveries), 2)
        self.assertEqual(deliveries[0]["payload_json"], deliveries[1]["payload_json"])
        self.assertEqual([d["fencing_token"] for d in deliveries], [2, 4])
        inbox = next(r for r in first["consumer_inbox"] if r["message_id"] == "msg-invoice-202")
        self.assertEqual(inbox["raw_delivery_count"], 2)
        self.assertEqual(first["invariants"]["invoice_deduplicated_count"], 1)
        self.assertEqual(
            [(r["message_id"], r["state"], r["attempt_count"]) for r in first["outbox_records"]],
            [("msg-order-101", "published", 2), ("msg-invoice-202", "published", 1), ("msg-webhook-303", "dead_letter", 2)],
        )

    def test_cli_demo_json_and_text_modes(self) -> None:
        default_output = io.StringIO()
        with redirect_stdout(default_output):
            self.assertEqual(main(["demo"]), 0)
        stdout_json = io.StringIO()
        with redirect_stdout(stdout_json):
            self.assertEqual(main(["demo", "--format", "json"]), 0)
        self.assertEqual(default_output.getvalue(), stdout_json.getvalue())
        parsed = json.loads(stdout_json.getvalue())
        stdout_text = io.StringIO()
        with redirect_stdout(stdout_text):
            self.assertEqual(main(["demo", "--format", "text"]), 0)
        self.assertEqual(stdout_text.getvalue().strip(), format_demo_text(parsed).strip())
        self.assertIn("All Invariants Passed:    True", stdout_text.getvalue())

    def test_cli_rejects_invalid_arguments(self) -> None:
        for arguments in ([], ["invalid-subcommand"], ["demo", "--format", "yaml"]):
            with redirect_stderr(io.StringIO()):
                self.assertEqual(main(arguments), 2)

    def test_cli_does_not_print_exception_content_or_payloads(self) -> None:
        for exception, code in (
            (OutboxConstraintError("PRIVATE_SYNTHETIC_PAYLOAD"), "constraint_violation"),
            (OSError("PRIVATE_SYNTHETIC_PATH"), "io_error"),
            (AssertionError("PRIVATE_SYNTHETIC_TRACE"), "invariant_failed"),
        ):
            stderr = io.StringIO()
            stdout = io.StringIO()
            with patch("outbox_lab.cli.run_demo_scenario", side_effect=exception):
                with redirect_stderr(stderr), redirect_stdout(stdout):
                    self.assertEqual(main(["demo"]), 1)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(json.loads(stderr.getvalue()), {"error": code, "message": "Demo could not complete."})
            self.assertNotIn("PRIVATE_SYNTHETIC", stderr.getvalue())
