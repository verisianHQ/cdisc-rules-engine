from dataclasses import dataclass

from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2


@dataclass
class BaseDatasetMetadata(DatasetMetadata2):
    domain: str

    @property
    def variable_prefix(self) -> str:
        return getattr(self, "domain_code", None) or self.domain
