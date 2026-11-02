import pytest

from cdisc_rules_engine.readers.data_readers.data_reader_factory import DataReaderFactory
from cdisc_rules_engine.readers.data_readers.sas7_data_reader import Sas7DataReader
from cdisc_rules_engine.readers.data_readers.xpt_data_reader import XptDataReader


@pytest.mark.parametrize(
    "file_path, reader_class",
    [
        ("dm.xpt", XptDataReader),
        ("DM.XPT", XptDataReader),
        ("dm.sas7bdat", Sas7DataReader),
        ("DM.SAS7BDAT", Sas7DataReader),
    ],
)
def test_get_data_reader_ignores_extension_case(file_path, reader_class):
    assert isinstance(DataReaderFactory.get_data_reader(file_path), reader_class)
