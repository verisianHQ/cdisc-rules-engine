from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
import json


class SqlGetModelFilteredVariables(SqlBaseOperation):
    def __init__(self, params: SqlOperationParams, data_service: PostgresQLDataService):
        super().__init__(params, data_service)

    def _execute_operation(self):
        dataset_id = self.data_service.pgi.schema.get_table_hash(self.params.domain)

        # Get filtered variable list as JSON
        variable_list_json = self._get_filtered_variables_json()

        # Construct the base WHERE clause from filters if any
        where_clause = self.construct_where_clause()

        # Create query that returns the filtered variable list for each row
        query = f"""SELECT '{variable_list_json}' AS value
                    FROM {dataset_id}"""

        if where_clause:
            query += f" {where_clause}"

        return SqlOperationResult(query=query, type="constant", subtype="text")

    def _get_filtered_variables_json(self):
        """
        Query the ig_variables table to get filtered variables and return as JSON array.
        """
        key_name = getattr(self.params, "key_name", None)
        key_value = getattr(self.params, "key_value", None)

        # Get standard info from data service
        standard = None
        standard_version = None

        if self.data_service.ig_specs:
            standard = self.data_service.ig_specs.get("standard")
            standard_version = self.data_service.ig_specs.get("standard_version")

        # Fallback to params if ig_specs not available
        if not standard:
            standard = getattr(self.params, "standard", None)
        if not standard_version:
            standard_version = getattr(self.params, "standard_version", None)

        if not key_name or not key_value or not standard or not standard_version:
            return "[]"

        # Query ig_variables table for filtered variables
        query = f"""
            SELECT variable_name, variable_order
            FROM ig_variables
            WHERE dataset_name = '{self.params.domain.upper()}'
              AND standard_type = '{standard.upper()}'
              AND version = '{standard_version}'
              AND {key_name} = '{key_value}'
            ORDER BY COALESCE(CAST(variable_order AS INTEGER), 0)
        """

        try:
            self.data_service.pgi.execute_sql(query)
            # After execute_sql, we need to fetch the results
            rows = self.data_service.pgi.fetch_all()
            variable_names = []

            for row in rows:
                var_name = row[0] if row[0] else ""
                # Replace wildcards like --TERM with domain-specific names
                if var_name.startswith("--"):
                    var_name = var_name.replace("--", self.params.domain.upper())
                variable_names.append(var_name)

            return json.dumps(variable_names)

        except Exception:
            # If query fails, return empty array
            return "[]"
