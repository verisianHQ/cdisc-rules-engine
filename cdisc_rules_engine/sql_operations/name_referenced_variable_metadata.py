from cdisc_rules_engine.sql_operations.referenced_variable_metadata import SqlReferencedVariableMetadata


class SqlNameReferencedVariableMetadata(SqlReferencedVariableMetadata):
    key_attribute = "name"
    key_alias = "name"
