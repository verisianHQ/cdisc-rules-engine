import pandas as pd
import pyreadstat
import pytest

from cdisc_rules_engine.readers.data_readers.xpt_data_reader import XptDataReader


def _write_xpt(path, qeval_values):
    # Two 8-byte character variables give a 16-byte record, so several records share the final 80-byte card
    df = pd.DataFrame({"QNAM": ["ENTCRITX"] * len(qeval_values), "QEVAL": qeval_values})
    pyreadstat.write_xport(df, str(path), table_name="SUPPXX", file_format_version=5)


def _read_records(path):
    _, chunks = XptDataReader(str(path)).read()
    return [record for chunk in chunks for record in chunk]


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
    path = tmp_path / "suppxx.xpt"
    _write_xpt(path, qeval_values)

    records = _read_records(path)

    assert len(records) == len(pyreadstat.read_xport(str(path))[0])
    assert [record["QEVAL"] for record in records] == qeval_values
    assert all(record["QNAM"] == "ENTCRITX" for record in records)
