from abc import ABC, abstractmethod
from typing import List

from cdisc_rules_engine.data_service.postgresql_data_service import (
    BaseDatasetMetadata,
    PostgresQLDataService,
)
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext

DEFINE_VARIABLES_TYPE = {
    "define_variable_name": "Char",
    "define_variable_label": "Char",
    "define_variable_data_type": "Char",
    "define_variable_is_collected": "Bool",
    "define_variable_role": "Char",
    "define_variable_size": "Num",
    "define_variable_ccode": "Char",
    "define_variable_format": "Char",
    "define_variable_allowed_terms": "Char",
    "define_variable_origin_type": "Char",
    "define_variable_has_no_data": "Bool",
    "define_variable_order_number": "Num",
    "define_variable_length": "Num",
    "define_variable_has_codelist": "Bool",
    "define_variable_codelist_coded_values": "Char",
    "define_variable_mandatory": "Bool",
    "define_variable_has_comment": "Bool",
}
DEFINE_DATASETS_TYPE = {
    "define_dataset_name": "Char",
    "define_dataset_label": "Char",
    "define_dataset_location": "Char",
    "define_dataset_domain": "Char",
    "define_dataset_class": "Char",
    "define_dataset_structure": "Char",
    "define_dataset_is_non_standard": "Bool",
    "define_dataset_variables": "Char",
    "define_dataset_key_sequence": "Char",
}


class SqlBaseDatasetBuilder(ABC):
    """
    Base class for SQL dataset builders.
    """

    def __init__(
        self,
        rule: dict,
        data_service: PostgresQLDataService,
        dataset_metadata: BaseDatasetMetadata,
        standards_context: BaseStandardsContext,
        datasets: List[BaseDatasetMetadata] = None,
        **kwargs,
    ):
        self.rule = rule
        self.data_service = data_service
        self.dataset_metadata = dataset_metadata
        self.standards_context = standards_context
        self.datasets = datasets or []
        # Store any additional kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    def build(self) -> str:
        """
        Build and return the table/view name for this rule type.

        For mini tables: just return the pre-existing table name.
        Regular builders return DatasetInterface, we return table name string.
        """
        pass

    def get_dataset_id(self) -> str:
        """
        Main entrypoint - equivalent to get_dataset() in regular builders.
        Returns the table/view name to validate against.
        """
        return self.build()
