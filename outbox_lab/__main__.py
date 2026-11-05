"""Module entrypoint for `python3 -m outbox_lab`."""

from __future__ import annotations

import sys

from outbox_lab.cli import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
