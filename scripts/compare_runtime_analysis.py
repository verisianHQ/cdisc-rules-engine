"""
Compares two runtime analysis CSV files (written by `core.py validate -ra`) and prints the rules whose runtime
went up or down by more than the given percentage between the two runs. The change is between the rule's runtimes
in milliseconds: a rule going from 400ms to 500ms has changed by +25%, one going from 400ms to 300ms by -25%.

Each rule's share of the total runtime (the sum of all its rules' runtimes) is printed alongside, and rules taking
a small share in both runs can be left out. Shares aren't compared, as one rule's change would move all the others'.

BEFORE and AFTER are file names in the engine's runtime_analysis folder, wherever the script is run from.
Example, from the engine's folder (from any other folder, give the path to this script instead):

python scripts/compare_runtime_analysis.py CORE-runtime-analysis_before.csv CORE-runtime-analysis_after.csv -pc 25
"""

import math
import sys
from pathlib import Path
from typing import Dict

import click

# run as a file, the script can't import the engine unless the engine's folder is on the path
ENGINE_ROOT = str(Path(__file__).resolve().parents[1])
if ENGINE_ROOT not in sys.path:
    sys.path.insert(0, ENGINE_ROOT)

from cdisc_rules_engine.utilities.runtime_analysis import (  # noqa: E402
    RUNTIME_ANALYSIS_DIR,
    read_runtime_analysis,
)


def runtime_analysis_file(ctx, param, value: str) -> str:
    """Resolves a file name against the runtime_analysis folder."""
    path = Path(RUNTIME_ANALYSIS_DIR) / value
    if not path.is_file():
        raise click.BadParameter(f"no file named '{value}' in {RUNTIME_ANALYSIS_DIR}")
    return str(path)


def runtime_shares(runtimes: Dict[str, float]) -> Dict[str, float]:
    """Maps CORE ID -> the rule's runtime as a percentage of the run's total runtime."""
    total = sum(runtimes.values())
    if not total:
        return {core_id: 0.0 for core_id in runtimes}
    return {core_id: runtime / total * 100 for core_id, runtime in runtimes.items()}


def format_runtime(runtime_ms: float) -> str:
    return f"{runtime_ms / 1000:.1f}s" if runtime_ms >= 1000 else f"{runtime_ms:.1f}ms"


def percent_change(before: float, after: float) -> float:
    """Relative change from one runtime to the other, in percent."""
    if before == 0:
        return 0.0 if after == 0 else math.inf
    return (after - before) / before * 100


@click.command(help=__doc__)
@click.argument("before", callback=runtime_analysis_file)
@click.argument("after", callback=runtime_analysis_file)
@click.option(
    "-pc",
    "--percentage-change",
    required=True,
    type=click.FloatRange(min=0),
    help="Print the rules whose runtime in milliseconds went up or down by more than this percentage",
)
@click.option(
    "-mpr",
    "--minimum-percentage-runtime",
    default=0,
    show_default=True,
    type=click.FloatRange(min=0),
    help="Leave out the rules that take less than this percentage of the total runtime in both runs",
)
def compare_runtime_analysis(before: str, after: str, percentage_change: float, minimum_percentage_runtime: float):
    before_runtimes = read_runtime_analysis(before)
    after_runtimes = read_runtime_analysis(after)
    before_total = sum(before_runtimes.values())
    after_total = sum(after_runtimes.values())
    total_change = f" ({percent_change(before_total, after_total):+.0f}%)" if before_total else ""
    click.echo(f"Total runtime: {format_runtime(before_total)} -> {format_runtime(after_total)}{total_change}")

    before_shares = runtime_shares(before_runtimes)
    after_shares = runtime_shares(after_runtimes)
    compared = [
        core_id
        for core_id in before_shares.keys() & after_shares.keys()
        if max(before_shares[core_id], after_shares[core_id]) >= minimum_percentage_runtime
    ]
    changed = sorted(
        (
            (core_id, change)
            for core_id in compared
            if abs(change := percent_change(before_runtimes[core_id], after_runtimes[core_id])) > percentage_change
        ),
        key=lambda item: (-abs(item[1]), item[0]),
    )
    compared_description = f"{len(compared)} rules"
    if minimum_percentage_runtime:
        compared_description += f" taking at least {minimum_percentage_runtime:g}% of the total runtime in either run"
    click.echo(f"{len(changed)} of {compared_description} changed by more than {percentage_change:g}%")
    for core_id, change in changed:
        click.echo(
            f"{core_id} runtime: {change:+.0f}%, "
            f"{format_runtime(before_runtimes[core_id])} -> {format_runtime(after_runtimes[core_id])} "
            f"({before_shares[core_id]:.2f}% -> {after_shares[core_id]:.2f}% of total runtime)"
        )

    # with the same study and ruleset these should be empty, so they're only listed when they're not
    for label, core_ids in (
        ("Rules only in the first run", before_shares.keys() - after_shares.keys()),
        ("Rules only in the second run", after_shares.keys() - before_shares.keys()),
    ):
        if core_ids:
            click.echo(f"{label}: {', '.join(sorted(core_ids))}")


if __name__ == "__main__":
    compare_runtime_analysis()
