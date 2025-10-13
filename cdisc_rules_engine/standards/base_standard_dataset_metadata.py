from dataclasses import dataclass

from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2


@dataclass
class BaseStandardDatasetMetadata(DatasetMetadata2):
    domain: str
    is_split: bool
