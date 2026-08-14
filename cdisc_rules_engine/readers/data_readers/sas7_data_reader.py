from typing import Any, Dict, Iterable, List, Optional, Tuple

from pandas.io.sas.sas7bdat import SAS7BDATReader

from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)
from cdisc_rules_engine.readers.data_readers.base_data_reader import BaseDataReader


class Sas7DataReader(BaseDataReader):
    """
    Sas7bdat Reader for datasets
    """

    FILE_EXTENSION = ".sas7bdat"
    CHUNKSIZE = 200000

    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._reader = None

    def read(self) -> Tuple[DatasetMetadata2, Iterable[List[Dict[str, Any]]]]:
        self._reader = SAS7BDATReader(self.file_path, encoding=None, chunksize=self.CHUNKSIZE, convert_dates=False)
        metadata = self._extract_metadata(self._reader)
        chunk_stream = self._read_chunks(self._reader, metadata)
        return metadata, chunk_stream

    def _get_extension(self):
        return self.FILE_EXTENSION

    def _extract_variable_metadata(self, reader: SAS7BDATReader) -> List[VariableMetadata]:
        """Extract variable-level metadata from SAS reader."""
        variables = []
        for i, col in enumerate(reader.columns):
            type_field = self._decode(col.ctype)
            # Only two types in SAS7bdat: numeric or string
            type_value = "Num" if self._is_numeric_type(type_field) else "Char"
            var_info = VariableMetadata(
                name=self._decode(col.name).strip(),
                label=self._decode(col.label),
                format=self._decode(col.format),
                type=type_value,
                length=col.length,
                order=i + 1,
            )
            variables.append(var_info)
        return variables

    def _get_total_rows(self, reader: SAS7BDATReader = None) -> Optional[int]:
        """
        SAS7BDATReader does not consistently expose a public row count.
        Return None so ingestion falls back to byte-based progress estimates.
        """
        reader = reader or self._reader
        if reader is None:
            return None
        try:
            row_count = getattr(reader, "row_count", None) or getattr(reader, "_row_count", None)
            if row_count is not None:
                return int(row_count)
        except Exception:
            pass
        return None
