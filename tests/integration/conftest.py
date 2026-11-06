import os
import socket

import pytest

from airpulse.common.config import load_config


def _reachable(host_port: str) -> bool:
    host, _, port = host_port.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=2):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def live_config():
    """the dev profile with the endpoints overridden from the environment, skipped when absent."""
    if not os.environ.get("AIRPULSE_KAFKA__BOOTSTRAP_SERVERS"):
        pytest.skip("set AIRPULSE_KAFKA__BOOTSTRAP_SERVERS, for example through port forwarding")
    cfg = load_config("configs/dev.yaml")
    if not _reachable(cfg.kafka.bootstrap_servers.split(",")[0]):
        pytest.skip(f"kafka at {cfg.kafka.bootstrap_servers} is not reachable")
    return cfg
