import re
from typing import List, Optional

from cdisc_rules_engine.constants import NULL_FLAVORS
from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.interfaces.condition_interface import ConditionInterface
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.models.validation_error_entity import ValidationErrorEntity
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata


class SqlVenmoResultHandler:
    def __init__(
        self,
        output_container: list,
        dataset_metadata: BaseDatasetMetadata,
        rule: dict,
        dataset_id: str,
        data_service: PostgresQLDataService,
        operation_variables: dict = None,
    ):
        self.output_container = output_container
        self.dataset_metadata = dataset_metadata
        self.rule = rule
        self.dataset_id = dataset_id
        self.data_service = data_service
        self.operation_variables = operation_variables or {}

    def evaluate_sql(self, where_clause: str, operations_query: str):
        schema = self.data_service.pgi.schema.get_table(self.dataset_id)

        target_columns = self._get_target_columns(self.rule, self.dataset_metadata, schema)
        select_cols = self._build_select_cols(target_columns, schema)
        meta_cols = self._build_meta_cols(schema)

        all_selects = ", ".join(meta_cols + select_cols)

        distinct_clause, order_clause = self._build_clauses(schema)
        dataset_filter = self._build_dataset_filter(schema)

        query = f"""
            SELECT {distinct_clause} {all_selects}
            FROM ({operations_query}) co
            WHERE {where_clause} {dataset_filter}
            {order_clause}
        """

        try:
            self.data_service.pgi.execute_sql(query)
            error_rows = self.data_service.pgi.fetch_all()
        except Exception as e:
            from cdisc_rules_engine.services import logger

            logger.error(f"Failed to execute compiled SQL: {e}\nQuery: {query}")
            raise e

        entities = self._build_validation_entities(error_rows, target_columns)

        message = self.rule.get("actions", [{}])[0].get("params", {}).get("message", "")
        if entities:
            error_obj = self._bundle_error_object(message, entities)
            self.output_container.append(error_obj.to_representation())

    def _build_select_cols(self, target_columns: dict[str, bool], schema: SqlTableSchema) -> list[str]:
        select_cols = []
        for col, present in target_columns.items():
            if not present:
                continue
            if col.startswith("$"):
                op = self.operation_variables.get(col)
                if op and op.type == "window":
                    col_name = op.params.get("column_name")
                    col_hash = schema.get_column_hash(col_name) or col_name
                    select_cols.append(f'co.{col_hash} AS "{col}"')
                elif op and op.type == "constant":
                    select_cols.append(f'({op.query}) AS "{col}"')
                elif op and op.type == "collection":
                    select_cols.append(f'ARRAY({op.query}) AS "{col}"')
                else:
                    select_cols.append(f'NULL AS "{col}"')
            else:
                if schema.has_column(col):
                    select_cols.append(f'co.{schema.get_column_hash(col)} AS "{col}"')
                else:
                    select_cols.append(f'NULL AS "{col}"')

        return select_cols

    def _build_meta_cols(self, schema: SqlTableSchema) -> list[str]:
        meta_cols = ["co.id AS __id"]
        for m_col in ["usubjid", f"{self.dataset_metadata.domain or ''}SEQ", SOURCE_ROW_NUMBER, "dataset_name"]:
            if schema.has_column(m_col):
                meta_cols.append(f'co.{schema.get_column_hash(m_col)} AS "__{m_col.lower()}"')
        return meta_cols

    def _build_clauses(self, schema: SqlTableSchema) -> tuple[str, str]:
        sensitivity = str(self.rule.get("sensitivity", "")).lower().strip()
        grouping_vars = self.rule.get("grouping_variables", self.rule.get("Grouping_Variables", []))
        actual_grouping_vars = []
        for value in grouping_vars:
            if value.lower() == "filter_by_dataset":
                continue
            actual_grouping_vars.append(value.replace("--", self.dataset_metadata.domain or "", 1).lower())

        if sensitivity == "group" or actual_grouping_vars:
            distinct_cols = []
            order_cols = []
            for g_var in actual_grouping_vars:
                if schema.has_column(g_var):
                    hash_name = schema.get_column_hash(g_var)
                    distinct_cols.append(f"co.{hash_name}")
                    order_cols.append(f"co.{hash_name} ASC")
            if distinct_cols:
                return f"DISTINCT ON ({', '.join(distinct_cols)})", f"ORDER BY {', '.join(order_cols)}, co.id ASC"
            return "", "ORDER BY co.id ASC"
        elif sensitivity in ["dataset", "study"]:
            return "", "ORDER BY co.id ASC LIMIT 1"

        return "", "ORDER BY co.id ASC"

    def _build_dataset_filter(self, schema: SqlTableSchema) -> str:
        if schema.has_column("dataset_name"):
            return f" AND co.{schema.get_column_hash('dataset_name')} = '{self.dataset_metadata.name}'"
        return ""

    def _build_validation_entities(
        self, error_rows: list[dict], target_columns: dict[str, bool]
    ) -> list[ValidationErrorEntity]:
        entities = []
        for row in error_rows:
            values = {}
            for col, present in target_columns.items():
                if not present:
                    values[col] = "Not in dataset"
                else:
                    val = row.get(col)
                    values[col] = None if val in NULL_FLAVORS else val

            entities.append(
                ValidationErrorEntity(
                    dataset=self.dataset_metadata.filename,
                    row=row.get(f"__{SOURCE_ROW_NUMBER.lower()}") or row.get("__id"),
                    usubjid=row.get("__usubjid"),
                    sequence=row.get(f"__{self.dataset_metadata.domain or ''}seq".lower()),
                    value=values,
                )
            )
        return entities

    def _bundle_error_object(self, message: str, error_rows: List[ValidationErrorEntity]) -> ValidationErrorContainer:
        original_schema = self.data_service.pgi.schema.get_table(self.dataset_metadata.name)
        return ValidationErrorContainer(
            domain=(self.dataset_metadata.domain),
            dataset=", ".join(sorted(set(error._dataset or "" for error in error_rows))),
            targets=SqlVenmoResultHandler._get_target_columns(self.rule, self.dataset_metadata, original_schema),
            errors=error_rows,
            message=message.replace("--", self.dataset_metadata.domain or ""),
        )

    @staticmethod
    def _get_target_columns(rule: dict, metadata: BaseDatasetMetadata, schema: SqlTableSchema) -> dict[str, bool]:
        target_columns = SqlVenmoResultHandler._extract_target_names_from_rule(rule, metadata, schema)
        target_columns_with_presence = {}

        for column in target_columns:
            if column.startswith("$"):
                target_columns_with_presence[column] = True
            else:
                target_columns_with_presence[column] = schema.has_column(column)

        return target_columns_with_presence

    @staticmethod
    def _extract_target_names_from_rule(rule: dict, metadata: BaseDatasetMetadata, schema: SqlTableSchema) -> List[str]:
        output_variables: List[str] = rule.get("output_variables", [])
        if output_variables:
            target_names: List[str] = [var.replace("--", metadata.domain or "", 1) for var in output_variables]
        else:
            target_names: List[str] = []
            conditions: ConditionInterface = rule["conditions"]

            for condition in conditions.values():
                if condition.get("operator") == "not_exists":
                    continue

                target: str = condition["value"].get("target")
                if target is None:
                    continue

                target = target.replace("--", metadata.domain or "")
                op_related_pattern: str = SqlVenmoResultHandler.get_operator_related_pattern(
                    condition.get("operator"), target
                )

                if op_related_pattern is not None:
                    columns = [col for col, _ in schema.get_columns()]
                    target_names.extend(
                        filter(
                            lambda name: re.match(op_related_pattern, name),
                            columns,
                        )
                    )
                else:
                    target_names.append(target)
        return target_names

    @staticmethod
    def get_operator_related_pattern(operator: str, target: str) -> Optional[str]:
        operator_related_patterns: dict = {
            "additional_columns_empty": rf"^{target}\d+$",
            "additional_columns_not_empty": rf"^{target}\d+$",
        }
        return operator_related_patterns.get(operator)
