import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

import core
from cdisc_rules_engine.utilities.runtime_analysis import (
    add_rule_runtime,
    read_runtime_analysis,
    write_runtime_analysis,
)
from cdisc_rules_engine.utilities.utils import (
    generate_report_filename,
    generate_runtime_analysis_filename,
)
import scripts.compare_runtime_analysis as compare_script
from scripts.compare_runtime_analysis import compare_runtime_analysis

RUN_TIME = "2026-10-29T12:13:39.123456"
VALIDATE_ARGS = ["-s", "sdtmig", "-v", "3-4", "-dp", "tests/resources/test_dataset.xpt", "-mem"]
COMPARE_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "compare_runtime_analysis.py"


def test_report_filename_without_tag_is_unchanged():
    assert generate_report_filename(RUN_TIME) == "CORE-Report-2026-10-29T12-13-39"


def test_report_filename_with_tag():
    assert generate_report_filename(RUN_TIME, "mBigNexara1") == "CORE-Report_mBigNexara1_2026-10-29T12-13-39"


def test_runtime_analysis_filename():
    assert generate_runtime_analysis_filename(RUN_TIME) == "CORE-runtime-analysis_2026-10-29T12-13-39.csv"
    assert (
        generate_runtime_analysis_filename(RUN_TIME, "mBigNexara1")
        == "CORE-runtime-analysis_mBigNexara1_2026-10-29T12-13-39.csv"
    )


def test_repeated_core_id_runtimes_are_added_up():
    runtimes = {}
    add_rule_runtime(runtimes, "CORE-000001", 10.0)
    add_rule_runtime(runtimes, "CORE-000002", 5.0)
    add_rule_runtime(runtimes, "CORE-000001", 2.5)
    assert runtimes == {"CORE-000001": 12.5, "CORE-000002": 5.0}


def test_write_then_read_runtime_analysis(tmp_path):
    path = tmp_path / "runtime_analysis" / "run.csv"
    write_runtime_analysis(str(path), {"CORE-000001": 12.3456, "CORE-000002": 0.5})
    assert path.read_text().splitlines() == ["core_id,runtime_ms", "CORE-000001,12.346", "CORE-000002,0.500"]
    assert read_runtime_analysis(str(path)) == {"CORE-000001": 12.346, "CORE-000002": 0.5}


def run_validate(extra_args):
    with patch.object(core, "run_sql_validation") as run:
        result = CliRunner().invoke(core.validate, VALIDATE_ARGS + extra_args)
    return result, run


def test_valid_custom_run_tag_is_accepted():
    # also shows VALIDATE_ARGS are valid, so the rejections below are down to the tag alone
    result, run = run_validate(["-crt", "mStudy1_v2-final.run"])
    assert result.exit_code == 0, result.output
    assert os.path.basename(run.call_args.args[0].output).startswith("CORE-Report_mStudy1_v2-final.run_")


@pytest.mark.parametrize(
    "tag", ["", " ", "a/b", "a\\b", "a:b", "a?b", "a*b", 'a"b', "a<b", "a>b", "a|b", "a\tb", "a\x00b", "a\x1fb"]
)
def test_custom_run_tag_that_is_empty_or_not_valid_in_a_file_name_is_rejected(tag):
    result, run = run_validate(["-crt", tag])
    assert result.exit_code == 2
    run.assert_not_called()


def write_before_and_after(folder: Path):
    before = folder / "before.csv"
    after = folder / "after.csv"
    # runtime changes: 0%, +50%, -40%, -20%; shares before: 50%, 20%, 20%, 10%; after: 50%, 30%, 12%, 8%
    write_runtime_analysis(
        str(before), {"CORE-000001": 500.0, "CORE-000002": 200.0, "CORE-000003": 200.0, "CORE-000004": 100.0}
    )
    write_runtime_analysis(
        str(after),
        {"CORE-000001": 500.0, "CORE-000002": 300.0, "CORE-000003": 120.0, "CORE-000004": 80.0, "CORE-000005": 0.0},
    )
    return str(before), str(after)


@pytest.fixture
def runtime_analysis_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(compare_script, "RUNTIME_ANALYSIS_DIR", str(tmp_path))
    write_before_and_after(tmp_path)
    return tmp_path


def compare(folder: Path, before: dict, after: dict, *options: str) -> list:
    write_runtime_analysis(str(folder / "before.csv"), before)
    write_runtime_analysis(str(folder / "after.csv"), after)
    result = CliRunner().invoke(compare_runtime_analysis, ["before.csv", "after.csv", *options])
    assert result.exit_code == 0, result.output
    return result.output.splitlines()


def test_compare_runtime_analysis(runtime_analysis_dir):
    result = CliRunner().invoke(compare_runtime_analysis, ["before.csv", "after.csv", "-pc", "25"])
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "Total runtime: 1.0s -> 1.0s (+0%)",
        "2 of 4 rules changed by more than 25%",
        "CORE-000002 runtime: +50%, 200.0ms -> 300.0ms (20.00% -> 30.00% of total runtime)",
        "CORE-000003 runtime: -40%, 200.0ms -> 120.0ms (20.00% -> 12.00% of total runtime)",
        "Rules only in the second run: CORE-000005",
    ]


def test_compare_runtime_analysis_only_flags_the_rule_that_changed(runtime_analysis_dir):
    # the other two rules' shares go from 25% to 45%, but their runtimes don't change
    before = {"CORE-000001": 500.0, "CORE-000002": 250.0, "CORE-000003": 250.0}
    after = {"CORE-000001": 50.0, "CORE-000002": 250.0, "CORE-000003": 250.0}
    assert compare(runtime_analysis_dir, before, after, "-pc", "25") == [
        "Total runtime: 1.0s -> 550.0ms (-45%)",
        "1 of 3 rules changed by more than 25%",
        "CORE-000001 runtime: -90%, 500.0ms -> 50.0ms (50.00% -> 9.09% of total runtime)",
    ]


def test_compare_runtime_analysis_flags_rules_that_all_slowed_down_the_same(runtime_analysis_dir):
    # all the shares stay the same, but every rule takes twice as long
    before = {"CORE-000001": 500.0, "CORE-000002": 250.0, "CORE-000003": 250.0}
    after = {"CORE-000001": 1000.0, "CORE-000002": 500.0, "CORE-000003": 500.0}
    assert compare(runtime_analysis_dir, before, after, "-pc", "25") == [
        "Total runtime: 1.0s -> 2.0s (+100%)",
        "3 of 3 rules changed by more than 25%",
        "CORE-000001 runtime: +100%, 500.0ms -> 1.0s (50.00% -> 50.00% of total runtime)",
        "CORE-000002 runtime: +100%, 250.0ms -> 500.0ms (25.00% -> 25.00% of total runtime)",
        "CORE-000003 runtime: +100%, 250.0ms -> 500.0ms (25.00% -> 25.00% of total runtime)",
    ]


def test_compare_runtime_analysis_flags_changes_beyond_the_threshold_either_way(runtime_analysis_dir):
    # +25% and -25% only reach the threshold, +26% and -26% go beyond it
    before = {"CORE-000001": 400.0, "CORE-000002": 400.0, "CORE-000003": 400.0, "CORE-000004": 400.0}
    after = {"CORE-000001": 500.0, "CORE-000002": 300.0, "CORE-000003": 504.0, "CORE-000004": 296.0}
    assert compare(runtime_analysis_dir, before, after, "-pc", "25")[1:] == [
        "2 of 4 rules changed by more than 25%",
        "CORE-000003 runtime: +26%, 400.0ms -> 504.0ms (25.00% -> 31.50% of total runtime)",
        "CORE-000004 runtime: -26%, 400.0ms -> 296.0ms (25.00% -> 18.50% of total runtime)",
    ]


def test_compare_runtime_analysis_leaves_out_rules_below_minimum_runtime_in_both_runs(runtime_analysis_dir):
    before, after = "before.csv", "after.csv"
    without_minimum = CliRunner().invoke(compare_runtime_analysis, [before, after, "-pc", "15"])
    assert (
        "CORE-000004 runtime: -20%, 100.0ms -> 80.0ms (10.00% -> 8.00% of total runtime)"
        in without_minimum.output.splitlines()
    )

    # CORE-000004 is under 15% of the total in both runs, CORE-000003 only in the second one
    result = CliRunner().invoke(compare_runtime_analysis, [before, after, "-pc", "15", "-mpr", "15"])
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "Total runtime: 1.0s -> 1.0s (+0%)",
        "2 of 3 rules taking at least 15% of the total runtime in either run changed by more than 15%",
        "CORE-000002 runtime: +50%, 200.0ms -> 300.0ms (20.00% -> 30.00% of total runtime)",
        "CORE-000003 runtime: -40%, 200.0ms -> 120.0ms (20.00% -> 12.00% of total runtime)",
        "Rules only in the second run: CORE-000005",
    ]


def test_compare_runtime_analysis_reads_files_from_the_runtime_analysis_folder(runtime_analysis_dir, monkeypatch):
    # the folder the script is run from doesn't matter
    other_folder = runtime_analysis_dir / "other"
    other_folder.mkdir()
    monkeypatch.chdir(other_folder)
    result = CliRunner().invoke(compare_runtime_analysis, ["before.csv", "after.csv", "-pc", "25"])
    assert result.exit_code == 0, result.output

    missing = CliRunner().invoke(compare_runtime_analysis, ["missing.csv", "after.csv", "-pc", "25"])
    assert missing.exit_code != 0
    assert f"no file named 'missing.csv' in {runtime_analysis_dir}" in missing.output


def test_compare_runtime_analysis_runs_as_a_file_from_another_folder(tmp_path):
    # run as a separate process, the script reads the real runtime_analysis folder: absolute paths still work there
    before, after = write_before_and_after(tmp_path)
    result = subprocess.run(
        [sys.executable, str(COMPARE_SCRIPT), before, after, "-pc", "25"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines()[1] == "2 of 4 rules changed by more than 25%"
