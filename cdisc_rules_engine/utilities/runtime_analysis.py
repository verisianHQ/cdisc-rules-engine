"""
Per-rule execution times for a validation run (core.py validate -ra), and the CSV they're written to.
The CSV has one row per rule: its CORE ID and the time the rule took to execute, in milliseconds.
scripts/compare_runtime_analysis.py compares two of these files.
"""

import csv
from pathlib import Path
from typing import Dict

# the engine's runtime_analysis folder, where all runtime analysis files go
RUNTIME_ANALYSIS_DIR = str(Path(__file__).resolve().parents[2] / "runtime_analysis")

CORE_ID_COLUMN = "core_id"
RUNTIME_COLUMN = "runtime_ms"


def add_rule_runtime(runtimes: Dict[str, float], core_id: str, runtime_ms: float):
    # a CORE ID that comes up more than once in a run is counted once, with its runtimes added up
    runtimes[core_id] = runtimes.get(core_id, 0.0) + runtime_ms


def write_runtime_analysis(path: str, runtimes: Dict[str, float]):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([CORE_ID_COLUMN, RUNTIME_COLUMN])
        for core_id, runtime_ms in runtimes.items():
            writer.writerow([core_id, f"{runtime_ms:.3f}"])


def read_runtime_analysis(path: str) -> Dict[str, float]:
    runtimes: Dict[str, float] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            add_rule_runtime(runtimes, row[CORE_ID_COLUMN], float(row[RUNTIME_COLUMN]))
    return runtimes
