import pytest

from airpulse.common.config import DEFAULT_CONFIG_DIR, load_config


@pytest.fixture
def config():
    return load_config(DEFAULT_CONFIG_DIR / "dev.yaml", environ={})
