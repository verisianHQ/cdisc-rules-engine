from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlGetDefineVariablesMetadata(SqlBaseOperation):

    def _execute_operation(self):

        domain_name = self.params.domain.lower()
        try:
            define_variables_metadata = self.params.standards_context.get_define_xml_variables_metadata(
                self.data_service, domain_name
            )
        except Exception as e:
            raise ValueError(f"Error: Domain {domain_name.upper()} is not found in Define XML", e)

        value = next(
            i.get(self.params.define_variable_value)
            for i in define_variables_metadata
            if i.get("define_variable_name") == self.params.target
        )

        if not value:
            raise Exception(f"Metadata extraction of {self.params.target} failed - metadata not found")

        return SqlOperationResult(
            query=f"SELECT '{value.replace('\'', '\'\'')}' AS value", type="constant", subtype="Char"
        )
