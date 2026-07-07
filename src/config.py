"""
config.py — loads sources.yaml and secrets from the environment (.env).

Never commit real credentials. Copy .env.example to .env and fill it in.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

with open(ROOT / "config" / "sources.yaml") as f:
    SOURCES = yaml.safe_load(f)

# --- secrets (from environment) ---
ACLED_EMAIL = os.getenv("ACLED_EMAIL", "")
ACLED_PASSWORD = os.getenv("ACLED_PASSWORD", "")
FIRMS_MAP_KEY = os.getenv("FIRMS_MAP_KEY", "")


def require(*names: str) -> None:
    """Fail early with a clear message if a needed secret is missing."""
    missing = [n for n in names if not globals().get(n)]
    if missing:
        raise SystemExit(
            f"Missing required secrets: {', '.join(missing)}. "
            f"Set them in {ROOT / '.env'} (see .env.example)."
        )
