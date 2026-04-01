"""Ensure pipelines/ml is on sys.path so pipeline modules can be imported."""

import sys
from pathlib import Path

# Add pipelines/ml/ to the front of sys.path so that
# `from components.preprocess import preprocess` works inside pipeline modules.
_ML_DIR = str(Path(__file__).resolve().parent.parent)
if _ML_DIR not in sys.path:
    sys.path.insert(0, _ML_DIR)
