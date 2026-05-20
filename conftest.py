"""Root conftest.py — ensures the src/ layout is on sys.path.

Required because hatchling's editable-install .pth file is missing a trailing
newline, so Python's site module silently skips the last line.  This file is
the standard pytest workaround for src-layout projects.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))
