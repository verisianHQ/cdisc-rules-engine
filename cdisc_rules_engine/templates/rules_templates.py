class SQLRuleTemplates:
    """ Extended SQL rule templates for various CDISC rule types. """

    @staticmethod
    def get_value_check_template() -> str:
        """ Template for value validation rules. """
        return """
               WITH rule_check AS (
                   SELECT
                       d.*,
                       CASE
                           WHEN {condition} THEN FALSE
                           ELSE TRUE
                           END as has_error
                   FROM {schema}.{dataset} d
                   WHERE d.study_id = %(study_id)s
               )
               SELECT
                   %(rule_id)s as rule_id,
                   '{dataset}' as dataset,
                   d.row_number,
                   d.usubjid,
                   d.{domain}seq as seq,
            jsonb_build_object(
                {target_columns}
            ) as error_values,
                   %(message)s as message
               FROM rule_check d
               WHERE d.has_error = TRUE
               LIMIT 1000; \
               """

    @staticmethod
    def get_codelist_check_template() -> str:
        """ Template for controlled terminology validation. """
        return """
               WITH codelist_values AS (
                   SELECT DISTINCT submission_value
                   FROM {schema}.ct_terms
                   WHERE codelist_id = %(codelist_id)s
                     AND ct_version = %(ct_version)s
               ),
                    rule_check AS (
                        SELECT
                            d.*,
                            CASE
                                WHEN d.{variable} IS NULL THEN FALSE
                    WHEN cv.submission_value IS NOT NULL THEN FALSE
                    ELSE TRUE
                END as has_error
                        FROM {schema}.{dataset} d
                            LEFT JOIN codelist_values cv
                        ON UPPER(d.{variable}) = UPPER(cv.submission_value)
                        WHERE d.study_id = %(study_id)s
                    )
               SELECT
                   %(rule_id)s as rule_id,
                   '{dataset}' as dataset,
                   d.row_number,
                   d.usubjid,
                   d.{domain}seq as seq,
            jsonb_build_object(
                '{variable}', d.{variable}
            ) as error_values,
                   %(message)s || ' - Value: ' || COALESCE(d.{variable}, 'null') as message
               FROM rule_check d
               WHERE d.has_error = TRUE
               LIMIT 1000; \
               """

    @staticmethod
    def get_consistency_check_template() -> str:
        """ Template for cross-dataset consistency checks. """
        return """
               WITH dataset1 AS (
                   SELECT * FROM {schema}.{dataset1}
                   WHERE study_id = %(study_id)s
               ),
                    dataset2 AS (
                        SELECT * FROM {schema}.{dataset2}
                        WHERE study_id = %(study_id)s
                    ),
                    rule_check AS (
                        SELECT
                            d1.*,
                            d2.{join_column2},
                CASE 
                    WHEN {consistency_condition} THEN FALSE
                    ELSE TRUE
                END as has_error
                        FROM dataset1 d1
                            {join_type} JOIN dataset2 d2
                        ON d1.{join_column1} = d2.{join_column2}
                            AND d1.usubjid = d2.usubjid
                    )
               SELECT
                   %(rule_id)s as rule_id,
                   '{dataset1}' as dataset,
                   d.row_number,
                   d.usubjid,
                   d.{domain}seq as seq,
            jsonb_build_object(
                {error_columns}
            ) as error_values,
                   %(message)s as message
               FROM rule_check d
               WHERE d.has_error = TRUE
               LIMIT 1000; \
               """

    @staticmethod
    def get_define_metadata_check_template() -> str:
        """ Template for Define XML metadata validation. """
        return """
               WITH define_metadata AS (
                   SELECT
                       dm.*,
                       dv.variable_name,
                       dv.variable_label,
                       dv.variable_type,
                       dv.variable_length,
                       dv.is_required
                   FROM {schema}.define_datasets dm
            JOIN {schema}.define_variables dv
                   ON dm.dataset_oid = dv.dataset_oid
                   WHERE dm.study_id = %(study_id)s
                     AND dm.dataset_name = %(dataset_name)s
               ),
                    actual_metadata AS (
                        SELECT
                            column_name as variable_name,
                            data_type,
                            character_maximum_length as max_length
                        FROM information_schema.columns
                        WHERE table_schema = %(schema)s
                          AND table_name = %(dataset_name)s
                    ),
                    rule_check AS (
                        SELECT
                            dm.*,
                            am.data_type as actual_type,
                            am.max_length as actual_length,
                            CASE
                                WHEN {metadata_condition} THEN FALSE
                                ELSE TRUE
                                END as has_error
                        FROM define_metadata dm
                                 LEFT JOIN actual_metadata am
                                           ON UPPER(dm.variable_name) = UPPER(am.variable_name)
                    )
               SELECT
                   %(rule_id)s as rule_id,
                   %(dataset_name)s as dataset,
                   NULL as row_number,
                   NULL as usubjid,
                   NULL as seq,
                   jsonb_build_object(
                           'variable', variable_name,
                           'define_type', variable_type,
                           'actual_type', actual_type,
                           'define_length', variable_length,
                           'actual_length', actual_length
                   ) as error_values,
                   %(message)s as message
               FROM rule_check
               WHERE has_error = TRUE; \
               """