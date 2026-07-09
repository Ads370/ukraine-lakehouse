"""
pipeline.py: single entrypoint the scheduler (cron/Prefect) calls.
For now it just runs the bronze ingestors. Silver/gold get wired in later.
    python src/pipeline.py            # run all
    python src/pipeline.py acled      # run one
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from ingest import acled, firms  # noqa: E402
from transform import gold, silver  # noqa: E402

STEPS = {
    "acled": acled.run,
    "firms": firms.run,
    "silver": silver.run,
    "gold": gold.run,
}


def main(argv: list[str]) -> None:
    steps = argv or list(STEPS)
    for name in steps:
        if name not in STEPS:
            raise SystemExit(f"Unknown step '{name}'. Options: {', '.join(STEPS)}")
        print(f"\n=== running: {name} ===")
        STEPS[name]()


if __name__ == "__main__":
    main(sys.argv[1:])
