-- Migration to SQL (just examples rn pls bare with)

-- Required variables check
INSERT INTO cdisc.rules (core_id, rule_type, description, sql_template)
VALUES (
    'CORE-000001',
    'Variable Existence Check',
    'Required variables must be present',
    $$
    SELECT
        NULL as row_number,
        ARRAY[missing_var] as variables,
        format('Required variable %s is missing', missing_var) as error_message
    FROM (
        SELECT unnest(ARRAY['STUDYID', 'DOMAIN', 'USUBJID']) as missing_var
        EXCEPT
        SELECT variable_name FROM cdisc.variable_metadata WHERE dataset_id = $1
    ) t
    $$
);

-- Empty values check
INSERT INTO cdisc.rules (core_id, rule_type, description, sql_template)
VALUES (
    'CORE-000002',
    'Empty Value Check',
    'STUDYID must not be empty',
    $$
    SELECT
        row_number,
        ARRAY['STUDYID'] as variables,
        'STUDYID must not be empty' as error_message
    FROM cdisc.dataset_contents
    WHERE dataset_id = $1
    AND (data->>'STUDYID' IS NULL OR data->>'STUDYID' = '')
    $$
);