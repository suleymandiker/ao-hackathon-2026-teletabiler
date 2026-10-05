"""Explicit entrypoint configuration bootstrap; process settings take priority."""
from pathlib import Path
from dotenv import load_dotenv


def load_environment(path=None):
    load_dotenv(path or Path(__file__).resolve().parents[2] / '.env', override=False)
