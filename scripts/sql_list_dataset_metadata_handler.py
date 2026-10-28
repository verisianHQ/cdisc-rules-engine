from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa

from cdisc_rules_engine.readers.data_readers.data_reader_factory import (
    DataReaderFactory,
)


def sql_list_dataset_metadata_handler(dataset_paths: Tuple[str]) -> List[dict]:
    """
    SQL engine version of list_dataset_metadata_handler.
    Reads metadata with the readers the SQL engine loads datasets with.
    Every format it can validate (XPT, SAS7BDAT) is listed.
    Output has the same shape as list_dataset_metadata_handler.
    """
    return [_dataset_metadata(path) for path in dataset_paths]


def _dataset_metadata(path: str) -> dict:
    reader = DataReaderFactory.get_data_reader(path)
    reader.CHUNKSIZE = 1
    metadata, chunk_stream = reader.read()
    return {
        "domain": _first_domain(chunk_stream),
        "filename": Path(metadata.full_path).name,
        "full_path": metadata.full_path,
        "file_size": metadata.file_size,
        "label": metadata.label,
        "modification_date": metadata.modification_date,
    }


def _first_domain(chunk_stream: Iterable[Union[pd.DataFrame, pa.Table]]) -> Optional[str]:
    """DOMAIN of the first record or None when there is no DOMAIN column (e.g. ADaM) or no records"""
    first_chunk = next(iter(chunk_stream), None)
    if hasattr(chunk_stream, "close"):
        chunk_stream.close()
    if first_chunk is None or len(first_chunk) == 0:
        return None
    if isinstance(first_chunk, pa.Table):
        if "domain" not in first_chunk.column_names:
            return None
        return first_chunk.column("domain")[0].as_py()
    if "domain" not in first_chunk.columns:
        return None
    return first_chunk["domain"].iloc[0]
