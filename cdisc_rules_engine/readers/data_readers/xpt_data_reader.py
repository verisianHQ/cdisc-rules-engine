from typing import Any, Dict, Iterable, List, Tuple

from pandas.io.sas.sas_xport import XportReader

from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)
from cdisc_rules_engine.readers.data_readers.base_data_reader import BaseDataReader

XPT_CARD_LENGTH = 80


class _XportReader(XportReader):
    """
    pandas XportReader with a corrected record count.

    XPT data is padded with blanks to a multiple of 80 bytes. For records of 80 bytes or less, pandas
    treats every blank 8-byte word in the final 80-byte card as padding, not just the trailing ones,
    so a last record with blank character values (e.g. an empty QEVAL) is silently dropped.
    """

    def _record_count(self) -> int:
        buffer = self.filepath_or_buffer
        buffer.seek(0, 2)
        total_length = buffer.tell() - self.record_start
        count = total_length // self.record_length
        if self.record_length <= XPT_CARD_LENGTH:
            # Only whole records made entirely of blanks and lying within the final padding (less than one
            # card) can be padding
            while count > 0 and total_length - (count - 1) * self.record_length < XPT_CARD_LENGTH:
                buffer.seek(self.record_start + (count - 1) * self.record_length)
                if buffer.read(self.record_length).strip(b" "):
                    break
                count -= 1
        buffer.seek(self.record_start)
        return count


class XptDataReader(BaseDataReader):
    """
    XPT Reader for datasets
    """

    FILE_EXTENSION = ".xpt"
    CHUNKSIZE = 10000

    def __init__(self, file_path: str):
        super().__init__(file_path)

    def read(self) -> Tuple[DatasetMetadata2, Iterable[List[Dict[str, Any]]]]:
        reader = _XportReader(self.file_path, encoding=None, chunksize=self.CHUNKSIZE)
        metadata = self._extract_metadata(reader)
        chunk_stream = self._read_chunks(reader, metadata)
        return metadata, chunk_stream

    def _get_extension(self):
        return self.FILE_EXTENSION

    def _extract_variable_metadata(self, reader: XportReader) -> List[VariableMetadata]:
        """Extract variable-level metadata from SAS reader."""
        variables = []
        for i, field in enumerate(reader.fields):
            type_field = self._decode(field.get("ntype"))
            # Only two types in XPT: numeric or string
            type_value = "Num" if self._is_numeric_type(type_field) else "Char"
            var_info = VariableMetadata(
                name=self._decode(field.get("name")).strip(),
                label=self._decode(field.get("label", "")),
                format=self._decode(field.get("nform", "")),
                type=type_value,
                length=field.get("field_length", 0),
                order=i + 1,
            )
            variables.append(var_info)
        return variables
