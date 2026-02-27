"""
Path helpers — resolve data directories relative to the repo root
regardless of whether the caller is a notebook or the Streamlit app.
"""
from pathlib import Path

# Repo root = parent of this file's parent  (src/ → Project-CS-570/)
REPO_ROOT = Path(__file__).resolve().parent.parent

DATA_RAW = REPO_ROOT / "data" / "raw"
DATA_PROCESSED = REPO_ROOT / "data" / "processed"


def raw_path(filename: str) -> str:
    """Return the absolute path string for a raw data file."""
    return str(DATA_RAW / filename)


def processed_path(filename: str) -> str:
    """Return the absolute path string for a processed data file."""
    return str(DATA_PROCESSED / filename)
