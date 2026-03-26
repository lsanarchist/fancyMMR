from __future__ import annotations

import os
from pathlib import Path
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PYTEST_TEMP_ROOT = ROOT / ".tmp" / "pytest-tmp"
PYTEST_TEMP_ROOT.mkdir(parents=True, exist_ok=True)

# Keep pytest temp files on the repo filesystem so a full /tmp does not break
# otherwise-valid local verification runs.
os.environ["TMPDIR"] = str(PYTEST_TEMP_ROOT)
os.environ["TEMP"] = str(PYTEST_TEMP_ROOT)
os.environ["TMP"] = str(PYTEST_TEMP_ROOT)
tempfile.tempdir = str(PYTEST_TEMP_ROOT)
