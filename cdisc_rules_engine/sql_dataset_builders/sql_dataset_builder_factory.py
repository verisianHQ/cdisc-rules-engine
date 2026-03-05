from typing import Type
from cdisc_rules_engine.enums.rule_types import RuleTypes
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_contents_dataset_builder import (
    SqlContentsDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_variables_metadata_builder import (
    SqlVariablesMetadataBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_domain_list_builder import (
    SqlDomainListDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_dataset_metadata_builder import (
    SqlDatasetMetadataBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_variables_metadata_with_library_builder import (
    SqlVariablesMetadataWithLibraryBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_value_check_with_dataset_metadata_builder import (
    SqlValueCheckWithDatasetMetadataBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_value_check_with_variable_metadata_builder import (
    SqlValueCheckWithVariableMetadataBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_contents_define_dataset_builder import (
    SqlContentsDefineDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_contents_define_variables_dataset_builder import (
    SqlContentsDefineVariablesDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_variables_metadata_with_define_dataset_builder import (
    SqlVariablesMetadataWithDefineDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_variables_metadata_with_define_and_library_dataset_builder import (
    SqlVariablesMetadataWithDefineAndLibraryDatasetBuilder,
)
from cdisc_rules_engine.sql_dataset_builders.sql_define_item_group_dataset_builder import (
    SqlDefineItemGroupDatasetBuilder,
)


class SqlDatasetBuilderFactory:
    """
    Factory to get the right SQL dataset builder based on rule type.
    """

    _builders_map = {
        RuleTypes.VARIABLE_METADATA_CHECK.value: SqlVariablesMetadataBuilder,
        RuleTypes.DOMAIN_PRESENCE_CHECK.value: SqlDomainListDatasetBuilder,
        RuleTypes.DATASET_METADATA_CHECK.value: SqlDatasetMetadataBuilder,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_LIBRARY.value: SqlVariablesMetadataWithLibraryBuilder,
        RuleTypes.VALUE_CHECK_WITH_DATASET_METADATA.value: SqlValueCheckWithDatasetMetadataBuilder,
        RuleTypes.VALUE_CHECK_WITH_VARIABLE_METADATA.value: SqlValueCheckWithVariableMetadataBuilder,
        RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE.value: SqlContentsDefineDatasetBuilder,
        RuleTypes.VALUE_CHECK_AGAINST_DEFINE_XML_VARIABLE.value: SqlContentsDefineVariablesDatasetBuilder,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE.value: SqlVariablesMetadataWithDefineDatasetBuilder,
        RuleTypes.VARIABLE_METADATA_CHECK_AGAINST_DEFINE_XML_AND_LIBRARY.value: SqlVariablesMetadataWithDefineAndLibraryDatasetBuilder,  # noqa: E501
        RuleTypes.DEFINE_ITEM_GROUP_METADATA_CHECK.value: SqlDefineItemGroupDatasetBuilder,
    }

    _unimplemented_types = {
        # define-XML dependent types (not implemented yet)
        RuleTypes.DATASET_CONTENTS_CHECK_AGAINST_DEFINE_AND_LIBRARY.value,
        RuleTypes.DATASET_METADATA_CHECK_AGAINST_DEFINE.value,
        RuleTypes.DEFINE_ITEM_METADATA_CHECK.value,
        RuleTypes.VALUE_LEVEL_METADATA_CHECK_AGAINST_DEFINE.value,
        RuleTypes.VALUE_CHECK_AGAINST_DEFINE_XML_VLM.value,
        RuleTypes.DEFINE_ITEM_METADATA_CHECK_AGAINST_LIBRARY.value,
    }

    @classmethod
    def register_service(cls, name: str, builder: Type[SqlBaseDatasetBuilder]) -> None:
        """
        Register a new SQL dataset builder for a specific rule type.
        """
        if not name:
            raise ValueError("Builder name must not be empty!")
        if not issubclass(builder, SqlBaseDatasetBuilder):
            raise TypeError("Implementation of SqlBaseDatasetBuilder required!")
        cls._builders_map[name] = builder

    def get_service(self, rule_type: str, **kwargs) -> SqlBaseDatasetBuilder:
        """
        Get instance of SQL dataset builder by rule type.

        Uses _builders_map to instantiate the appropriate builder.
        Falls back to SqlContentsDatasetBuilder (Record Data) as default.
        Raises NotImplementedError for explicitly unimplemented rule types.
        """

        # Get builder class from map, default to SqlContentsDatasetBuilder
        builder_class = self._builders_map.get(rule_type, SqlContentsDatasetBuilder)
        return builder_class(**kwargs)
