from cdisc_rules_engine.sql_operations.referenced_variable_metadata import SqlReferencedVariableMetadata


class SqlLabelReferencedVariableMetadata(SqlReferencedVariableMetadata):
    key_attribute = "label"
    key_alias = "lbl"
