"""Convenience entry point equivalent to python -m quaketrack.cli backfill."""
import sys
from quaketrack.cli import main

if __name__ == "__main__":
    raise SystemExit(main(["backfill", *sys.argv[1:]]))
