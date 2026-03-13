"""Shared pytest fixtures."""

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to the directory containing DwC-A test archives."""
    return FIXTURES_DIR


def fixture_path(name: str) -> Path:
    """Return the absolute path to a named test fixture."""
    return FIXTURES_DIR / name
