import math

import pandas as pd
import pyreadstat
import pytest

from cdisc_rules_engine.readers.data_readers.xpt_data_reader import XptDataReader


def _write_xpt(path, df):
    pyreadstat.write_xport(df, str(path), table_name="SUPPXX", file_format_version=5)


def _read_records(path):
    _, tables = XptDataReader(str(path)).read()
    return [record for table in tables for record in table.to_pylist()]


@pytest.mark.parametrize(
    "qeval_values",
    [
        ["INVESTIG", ""],
        ["INVESTIG", "", "", "", ""],
        ["INVESTIG", "SPONSOR"],
        ["", ""],
        ["INVESTIG"],
    ],
)
def test_read_keeps_last_record_with_blank_values(tmp_path, qeval_values):
    # Two 8-byte character variables give a 16-byte record, so several records share the final 80-byte card
    path = tmp_path / "suppxx.xpt"
    _write_xpt(path, pd.DataFrame({"QNAM": ["ENTCRITX"] * len(qeval_values), "QEVAL": qeval_values}))

    records = _read_records(path)

    assert [record["qeval"] for record in records] == qeval_values
    assert all(record["qnam"] == "ENTCRITX" for record in records)


@pytest.mark.parametrize(
    "columns",
    [
        {"A": ["X", "Y", "Q"], "B": ["", "", ""], "C": ["Z", "", ""]},
        {"A": ["XXXXXXXXXX", "Y         ", "Z"]},
        {"A": ["X" * 100, "Y"]},
    ],
)
def test_read_keeps_all_records_for_any_record_length(tmp_path, columns):
    path = tmp_path / "suppxx.xpt"
    df = pd.DataFrame(columns)
    _write_xpt(path, df)

    records = _read_records(path)

    assert records == [{name.lower(): value.rstrip() for name, value in row.items()} for row in df.to_dict("records")]


def test_read_keeps_last_record_with_missing_numeric(tmp_path):
    path = tmp_path / "suppxx.xpt"
    _write_xpt(path, pd.DataFrame({"A": ["X", ""], "N": [1.0, math.nan]}))

    records = _read_records(path)

    assert [record["a"] for record in records] == ["X", ""]
    assert records[0]["n"] == 1.0
    assert records[1]["n"] is None


def test_read_drops_fully_blank_trailing_record(tmp_path):
    path = tmp_path / "suppxx.xpt"
    _write_xpt(path, pd.DataFrame({"A": ["X", ""], "B": ["Y", ""]}))

    records = _read_records(path)

    assert records == [{"a": "X", "b": "Y"}]


def test_read_warns_when_data_is_not_card_aligned(tmp_path):
    path = tmp_path / "suppxx.xpt"
    _write_xpt(path, pd.DataFrame({"A": ["X"]}))
    with open(path, "ab") as handle:
        handle.write(b"  ")

    with pytest.warns(UserWarning, match="xport file may be corrupted"):
        XptDataReader(str(path)).read()
