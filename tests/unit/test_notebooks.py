import json
import re
from pathlib import Path

import pytest

NOTEBOOK_DIR = Path(__file__).resolve().parents[2] / "notebooks"
NOTEBOOKS = sorted(NOTEBOOK_DIR.glob("*.ipynb"))


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda path: path.name)
def test_notebook_is_numbered_and_carries_no_output(path) -> None:
    # output can hold paths and host names of the machine that ran the notebook.
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    code = [cell for cell in cells if cell["cell_type"] == "code"]

    assert re.fullmatch(r"\d{2}_[a-z0-9_]+\.ipynb", path.name)
    assert code
    assert all(not cell["outputs"] for cell in code)
    assert all(cell["execution_count"] is None for cell in code)
