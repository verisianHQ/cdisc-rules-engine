from abc import ABC, abstractmethod
from typing import List

from cdisc_rules_engine.data_service.postgresql_data_service import (
    BaseDatasetMetadata,
    PostgresQLDataService,
)
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import DefineXMLReaderFactory
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext

LIBRARY_VARIABLES_TYPE = {
    "library_variable_name": "Char",
    "library_variable_label": "Char",
    "library_variable_data_type": "Char",
    "library_variable_role": "Char",
    "library_variable_core": "Char",
    "library_variable_order_number": "Num",
}

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
    "define_dataset_is_non_standard": "Char",
    "define_dataset_variables": "Char",
    "define_dataset_key_sequence": "Char",
}
DEFINE_VLM_TYPE = {
    "define_vlm_name": "Char",
    "define_vlm_label": "Char",
    "define_vlm_data_type": "Char",
    "define_vlm_is_collected": "Bool",
    "define_vlm_role": "Char",
    "define_vlm_size": "Num",
    "define_vlm_ccode": "Char",
    "define_vlm_format": "Char",
    "define_vlm_allowed_terms": "Char",
    "define_vlm_origin_type": "Char",
    "define_vlm_has_no_data": "Bool",
    "define_vlm_order_number": "Num",
    "define_vlm_length": "Num",
    "define_vlm_has_codelist": "Bool",
    "define_vlm_codelist_coded_values": "Char",
    "define_vlm_mandatory": "Bool",
    "define_vlm_has_comment": "Bool",
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

    def get_define_vars(self) -> List[dict]:
        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        domain = self.dataset_metadata.domain or self.dataset_metadata.name
        metadata = define_reader.extract_variables_metadata(domain)
        self.flatten_lists_in_dict(metadata)
        return metadata

    def get_define_datasets(self) -> List[dict]:
        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        metadata = define_reader.extract_dataset_metadata()
        self.flatten_lists_in_dict(metadata)
        return metadata

    def get_define_vlms(self) -> List[dict]:
        define_reader = DefineXMLReaderFactory.get_define_xml_reader(
            self.data_service.define_xml_path, self.data_service.define_xml_path, self.data_service, None
        )
        metadata = define_reader.extract_value_level_metadata()
        self.flatten_lists_in_dict(metadata)
        return metadata

    @staticmethod
    def flatten_lists_in_dict(metadata_list):
        for var in metadata_list:
            for k, v in var.items():
                if isinstance(v, list):
                    var[k] = ",".join(map(str, v))
