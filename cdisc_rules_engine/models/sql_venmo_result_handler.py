import re
from typing import List, Optional

import pandas as pd
from business_rules.actions import BaseActions, rule_action
from business_rules.fields import FIELD_TEXT

from cdisc_rules_engine.constants import NULL_FLAVORS
from cdisc_rules_engine.constants.metadata_columns import SOURCE_ROW_NUMBER
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.enums.sensitivity import Sensitivity
from cdisc_rules_engine.interfaces.condition_interface import ConditionInterface
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.models.validation_error_container import (
    ValidationErrorContainer,
)
from cdisc_rules_engine.models.validation_error_entity import ValidationErrorEntity
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata


class SqlVenmoResultHandler(BaseActions):
    """
    This class maps the output of venmo (a truth series) to a list of error objects.
    It uses the rule 'Sensitivity' to determine whether to generate a single dataset-level error
    or multiple record-level errors.

    This is an example error:
    {
        "dataset": "ae.xpt",
        "domain": "AE",
        "variables": ["AESTDY", "DOMAIN"],
        "errors": [
            {
                "dataset": "ae.xpt",
                "row": 0,
                "value": {"STUDYID": "Not in dataset"},
                "uSubjId": "2",
                "seq": 1,
            },
            {
                "dataset": "ae.xpt",
                "row": 1,
                "value": {"AESTDY": "test", "DOMAIN": "test"},
                "uSubjId": 7,
                "seq": 2,
            },
            {
                "dataset": "ae.xpt",
                "row": 9,
                "value": {"AESTDY": "test", "DOMAIN": "test"},
                "uSubjId": 12,
                "seq": 10,
            },
        ],
        "message": "AESTDY and DOMAIN are equal to test",
    }
    """

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

    @rule_action(params={"message": FIELD_TEXT})
    def generate_dataset_error_objects(self, message: str, results: pd.Series):
        """
        This function maps the truth series from venmo to a list of error objects.

        For derived table validations (Domain Presence, Variable Metadata):
        - Broadcast single-value Series to match original dataset length
        """
        # Broadcast single-value Series to original dataset length
        if len(results) == 1 and self.dataset_id != self.dataset_metadata.name:
            # Get row count of original dataset
            original_table_hash = self.data_service.pgi.schema.get_table_hash(self.dataset_metadata.name)
            self.data_service.pgi.execute_sql(f"SELECT COUNT(*) as count FROM {original_table_hash};")
            count_result = self.data_service.pgi.fetch_all()
            original_row_count = count_result[0]["count"]

            single_value = results.iloc[0]
            results = pd.Series([single_value] * original_row_count, index=range(original_row_count))

        rows_with_error = self._get_error_rows(results)

        validation_schema = self.data_service.pgi.schema.get_table(self.dataset_id)
        target_columns = SqlVenmoResultHandler._get_target_columns(self.rule, self.dataset_metadata, validation_schema)

        self._evaluate_operations_in_bulk(rows_with_error, validation_schema, target_columns)

        errors_list = self._generate_errors_list(rows_with_error, target_columns, validation_schema)
        error_object = self._bundle_error_object(
            message=message,
            error_rows=errors_list,
        )
        self.output_container.append(error_object.to_representation())

    def _get_error_rows(self, truth_series) -> List[dict]:
        """
        Fetch the rows which returned TRUE.

        Query from the validation table (self.dataset_id) which contains all necessary columns:
        - For normal rules: same as original dataset
        - For cross-dataset rules: joined table with columns from multiple datasets
        - For metadata rules: metadata table
        """
        # Query from the validation table which has all the columns we need
        table_hash = self.data_service.pgi.schema.get_table_hash(self.dataset_id)

        # Get indices of TRUE values
        true_indicies = [str(i + 1) for i, x in enumerate(truth_series) if x]

        if not true_indicies:
            return []

        # Query the validation table
        self.data_service.pgi.execute_sql(
            f"""SELECT * FROM {table_hash}
                WHERE id IN ({', '.join(true_indicies)}) ORDER BY id ASC"""
        )

        results = self.data_service.pgi.fetch_all()
        return list(results)

    def _evaluate_operations_in_bulk(self, rows: List[dict], schema: SqlTableSchema, target_columns: dict):  # noqa
        """
        Constructs a single VALUES CTE containing all parameters for the failing rows.
        Binds the CTE to the operation query.
        """
        op_vars = [col for col, present in target_columns.items() if present and col.startswith("$")]
        if not op_vars or not rows:
            return

        for var_name in op_vars:
            if var_name not in self.operation_variables:
                for row in rows:
                    row[var_name] = "Operation variable not found"
                continue

            op_result = self.operation_variables[var_name]

            if op_result.type == "window":
                col_name = op_result.params.get("column_name")
                col_hash = schema.get_column_hash(col_name)
                for row in rows:
                    row[var_name] = row.get(col_hash)
                continue

            if not op_result.params:
                if op_result.type == "constant":
                    val = self._execute_query_for_single_value(op_result.query)
                elif op_result.type == "collection":
                    val = self._execute_query_for_collection_values(op_result.query)
                else:
                    val = "Unsupported operation type"

                for row in rows:
                    row[var_name] = val
                continue

            param_placeholders = sorted(list(op_result.params.keys()), key=len, reverse=True)
            param_columns = [op_result.params[p] for p in param_placeholders]

            chunk_size = 5000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                values_list = []

                for j, row in enumerate(chunk):
                    row_id = row.get("id")

                    row_vals = [f"CAST('{row_id}' AS INTEGER)" if j == 0 else f"'{row_id}'"]

                    for col in param_columns:
                        if col == "id":
                            col_type = "INTEGER"
                            val = row.get("id")
                        else:
                            col_schema = schema.get_column(col)
                            col_type = "NUMERIC" if col_schema and col_schema.type == "Num" else "TEXT"
                            val = row.get(schema.get_column_hash(col))

                        if val is None:
                            row_vals.append(f"CAST(NULL AS {col_type})" if j == 0 else "NULL")
                        elif col_type == "TEXT":
                            clean_val = str(val).replace("'", "''")
                            row_vals.append(f"CAST('{clean_val}' AS TEXT)" if j == 0 else f"'{clean_val}'")
                        else:
                            row_vals.append(f"CAST({val} AS {col_type})" if j == 0 else str(val))

                    values_list.append(f"({', '.join(row_vals)})")

                v_cols = ["v_id"] + [f"v_p{k}" for k in range(len(param_placeholders))]
                values_sql = ",\n".join(values_list)

                bulk_query_inner = op_result.query
                for k, placeholder in enumerate(param_placeholders):
                    bulk_query_inner = bulk_query_inner.replace(placeholder, f"v.v_p{k}")

                if op_result.type == "constant":
                    bulk_query = f"""
                        SELECT v.v_id, ({bulk_query_inner}) as op_value
                        FROM (VALUES {values_sql}) AS v({', '.join(v_cols)})
                    """
                elif op_result.type == "collection":
                    bulk_query = f"""
                        SELECT v.v_id, ARRAY({bulk_query_inner}) as op_value
                        FROM (VALUES {values_sql}) AS v({', '.join(v_cols)})
                    """
                else:
                    for row in chunk:
                        row[var_name] = "Unsupported operation variable type"
                    continue

                try:
                    self.data_service.pgi.execute_sql(bulk_query)
                    results = self.data_service.pgi.fetch_all()

                    if op_result.type == "collection":
                        val_map = {
                            str(r["v_id"]): list(r["op_value"]) if r["op_value"] is not None else [] for r in results
                        }
                    else:
                        val_map = {str(r["v_id"]): r["op_value"] for r in results}

                    for row in chunk:
                        row[var_name] = val_map.get(str(row.get("id")))
                except Exception as e:
                    for row in chunk:
                        row[var_name] = f"Query error: {str(e)}"

    def _execute_query_for_single_value(self, query: str):
        try:
            self.data_service.pgi.execute_sql(query)
            result_rows = self.data_service.pgi.fetch_all()
            if result_rows:
                result_keys = list(result_rows[0].keys())
                if result_keys:
                    return result_rows[0][result_keys[0]]
            return None
        except Exception as e:
            return f"Query error: {str(e)}"

    def _execute_query_for_collection_values(self, query: str):
        try:
            self.data_service.pgi.execute_sql(query)
            result_rows = self.data_service.pgi.fetch_all()
            if result_rows:
                return [row.get("value") for row in result_rows if row.get("value") is not None]
            return []
        except Exception as e:
            return f"Query error: {str(e)}"

    def _bundle_error_object(self, message: str, error_rows: List[ValidationErrorEntity]) -> ValidationErrorContainer:
        original_schema = self.data_service.pgi.schema.get_table(self.dataset_metadata.name)

        return ValidationErrorContainer(
            domain=(self.dataset_metadata.domain),
            dataset=", ".join(sorted(set(error._dataset or "" for error in error_rows))),
            targets=SqlVenmoResultHandler._get_target_columns(self.rule, self.dataset_metadata, original_schema),
            errors=error_rows,
            message=message.replace("--", self.dataset_metadata.domain or ""),
        )

    def _generate_errors_list(
        self, data: List[dict], target_columns: dict[str, bool], schema: SqlTableSchema
    ) -> List[ValidationErrorEntity]:
        match self.rule.get("sensitivity"):
            case Sensitivity.DATASET.value | Sensitivity.STUDY.value:
                return [self._build_dataset_error(data, target_columns, schema)]
            case Sensitivity.RECORD.value | None:
                return self._build_record_error_items(data, target_columns, schema)
            case Sensitivity.GROUP.value:
                return self._build_group_error_items(data, target_columns, schema)
            case _:
                raise ValueError(f"Invalid sensitivity value: {self.rule.get('sensitivity')}")

    def _build_dataset_error(
        self, data: List[dict], target_columns: dict[str, bool], schema: SqlTableSchema
    ) -> ValidationErrorEntity:
        """Only generate one error for rules with dataset sensitivity"""
        if len(data) == 0:
            value = {}
        else:
            value = self._create_error_for_row(data[0], schema, target_columns).value

        return ValidationErrorEntity(
            value=value,
            dataset=self.dataset_metadata.filename,
        )

    def _build_record_error_items(
        self, data: List[dict], target_columns: dict[str, bool], schema: SqlTableSchema
    ) -> List[ValidationErrorEntity]:
        """
        Build a list of ValidationErrorEntity objects for each error row in the data.
        """
        return [self._create_error_for_row(row, schema, target_columns) for row in data]

    def _build_group_error_items(
        self, data: List[dict], target_columns: dict[str, bool], schema: SqlTableSchema
    ) -> List[ValidationErrorEntity]:
        """
        Group error rows by the rule's grouping_variables and return one error per group.
        The first row encountered for each unique combination of grouping variable values is returned.
        Falls back to record-level errors if no grouping_variables are defined on the rule.
        """
        grouping_variables: List[str] = self.rule.get("grouping_variables") or []
        if not grouping_variables:
            return self._build_record_error_items(data, target_columns, schema)

        seen_groups: set = set()
        result: List[ValidationErrorEntity] = []
        for row in data:
            if "filter_by_dataset" in grouping_variables:
                dataset_name = row.get(schema.get_column_hash("dataset_name"))
                if dataset_name != self.dataset_metadata.name:
                    continue
            group_key = tuple(
                row.get(schema.get_column_hash(key)) for key in grouping_variables if key not in ["filter_by_dataset"]
            )
            if group_key not in seen_groups:
                seen_groups.add(group_key)
                result.append(self._create_error_for_row(row, schema, target_columns))
        return result

    def _create_error_for_row(
        self, row: dict, schema: SqlTableSchema, target_columns: dict[str, bool]
    ) -> ValidationErrorEntity:
        usubjid = str(row.get(schema.get_column_hash("usubjid")))

        sequence_column = f"{self.dataset_metadata.domain or ''}SEQ"
        sequence_value = row.get(schema.get_column_hash(sequence_column))
        sequence = int(sequence_value) if sequence_value is not None and sequence_value != "" else None

        source_row_hash = schema.get_column_hash(SOURCE_ROW_NUMBER)

        # Determine row_id based on table source type
        if schema.source == "data":
            # Original data tables MUST have source_row_number (enforced by PR #400)
            if not source_row_hash or source_row_hash not in row:
                raise ValueError(
                    f"source_row_number not found in row data for table {schema.name}. "
                    f"Data loading issue. All original data tables must have source_row_number."
                )
            row_id = row.get(source_row_hash)
        elif schema.source == "derived":
            if source_row_hash and source_row_hash in row:
                row_id = row.get(source_row_hash)
            else:
                row_id = row.get("id")
        else:  # schema.source == "static"
            row_id = row.get("id")

        values = {}
        for column in target_columns.keys():
            if not target_columns[column]:
                values[column] = "Not in dataset"
                continue

            if column.startswith("$"):
                value = row.get(column)
            else:
                value = row.get(schema.get_column_hash(column))

            if value is None or value in NULL_FLAVORS:
                values[column] = None
            else:
                values[column] = value

        return ValidationErrorEntity(
            dataset=self.dataset_metadata.filename,
            row=int(row_id),
            usubjid=usubjid,
            sequence=sequence,
            value=values,
        )

    @staticmethod
    def _get_target_columns(rule: dict, metadata: BaseDatasetMetadata, schema: SqlTableSchema) -> dict[str, bool]:
        """
        Returns the columns to display in the error object
        """
        target_columns = SqlVenmoResultHandler._extract_target_names_from_rule(rule, metadata, schema)
        target_columns_with_presence = {}

        for column in target_columns:
            if column.startswith("$"):
                # Operation variables always exist if they're in the rule
                target_columns_with_presence[column] = True
            else:
                # Regular columns need to be checked against the schema
                target_columns_with_presence[column] = schema.has_column(column)

        return target_columns_with_presence

    @staticmethod
    def _extract_target_names_from_rule(rule: dict, metadata: BaseDatasetMetadata, schema: SqlTableSchema) -> List[str]:
        r"""
        Extracts target from each item of condition list.

        Some operators require reporting additional column names when
        extracting target names. An operator has a certain pattern,
        to which these column names have to correspond. So we
        have a mapping like {operator: pattern} to find the
        necessary pattern and extract matching column names.
        Example:
            column: TSVAL
            operator: additional_columns_empty
            pattern: ^TSVAL\d+$ (starts with TSVAL and ends with number)
            additional columns: TSVAL1, TSVAL2, TSVAL3 etc.
        """

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
        # {operator: pattern} mapping
        operator_related_patterns: dict = {
            "additional_columns_empty": rf"^{target}\d+$",
            "additional_columns_not_empty": rf"^{target}\d+$",
        }
        return operator_related_patterns.get(operator)
