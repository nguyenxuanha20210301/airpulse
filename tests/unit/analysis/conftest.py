import os
from pathlib import Path

import pytest

from airpulse.common.config import DEFAULT_CONFIG_DIR, load_config

# matplotlib writes a font cache when it is imported, keep it inside the repo like the others.
os.environ.setdefault(
    "MPLCONFIGDIR", str(Path(__file__).resolve().parents[3] / ".cache" / "matplotlib")
)


@pytest.fixture
def config():
    return load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={})
