from importlib.metadata import version

import airpulse


def test_package_version_matches_metadata() -> None:
    assert airpulse.__version__ == version("airpulse")
