from typing import Tuple, Optional
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlContentsDatasetBuilder(SqlBaseDatasetBuilder):
    def build(self) -> Tuple[str, Optional[str]]:
        """
        Return the table name and None for the query.
        """
        dataset_id = self.data_service.get_dataset_for_rule(self.dataset_metadata, self.rule, self.standards_context)
        return dataset_id, None
