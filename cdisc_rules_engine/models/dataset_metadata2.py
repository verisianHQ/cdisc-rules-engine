from dataclasses import dataclass, field
from typing import List, Optional

from cdisc_rules_engine.models.sql import DATASET_COLUMN_TYPES


@dataclass
class VariableMetadata:
    name: str
    label: str
    type: DATASET_COLUMN_TYPES
    length: int
    format: str
    order: int


@dataclass
class DatasetMetadata2:
    filename: str
    name: str
    label: str
    variables: List[VariableMetadata]
    file_size: Optional[int] = field(default=None, kw_only=True)
