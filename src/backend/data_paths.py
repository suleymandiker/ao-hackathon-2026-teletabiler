"""Repository-anchored locations for mutable AIOps runtime data."""

import os
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def data_root():
    """Resolve AIOPS_DATA_DIR relative to the repository, never the process cwd."""
    configured = os.environ.get("AIOPS_DATA_DIR") or "data"
    return (REPOSITORY_ROOT / configured).resolve()


def monitoring_data_dir():
    return data_root() / "monitoring"


def policy_data_dir():
    return data_root() / "policy"


def template_data_dir():
    return data_root() / "templates"
