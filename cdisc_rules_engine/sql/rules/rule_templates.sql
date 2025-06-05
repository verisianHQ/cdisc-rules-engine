-- Rule template functions

-- Check if a variable exists in dataset
CREATE OR REPLACE FUNCTION cdisc.check_variable_exists(
    p_dataset_id INTEGER,
    p_variable_name TEXT
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM cdisc.variable_metadata
        WHERE dataset_id = p_dataset_id
          AND variable_name = p_variable_name
    );
END;
$$ LANGUAGE plpgsql;

-- Check required variables
CREATE OR REPLACE FUNCTION cdisc.check_required_variables(
    p_dataset_id INTEGER,
    p_required_vars TEXT[]
) RETURNS TABLE(missing_variable TEXT) AS $$
BEGIN
    RETURN QUERY
        SELECT unnest(p_required_vars) AS missing_variable
        EXCEPT
        SELECT variable_name FROM cdisc.variable_metadata
        WHERE dataset_id = p_dataset_id;
END;
$$ LANGUAGE plpgsql;

-- Check for empty values
CREATE OR REPLACE FUNCTION cdisc.check_empty_values(
    p_dataset_id INTEGER,
    p_variable_name TEXT
) RETURNS TABLE(row_number INTEGER, value TEXT) AS $$
BEGIN
    RETURN QUERY
        SELECT dc.row_number, dc.data->>p_variable_name AS value
        FROM cdisc.dataset_contents dc
        WHERE dc.dataset_id = p_dataset_id
          AND (dc.data->>p_variable_name IS NULL
            OR dc.data->>p_variable_name = '');
END;
$$ LANGUAGE plpgsql;

-- Check value against allowed list
CREATE OR REPLACE FUNCTION cdisc.check_allowed_values(
    p_dataset_id INTEGER,
    p_variable_name TEXT,
    p_allowed_values TEXT[]
) RETURNS TABLE(row_number INTEGER, value TEXT) AS $$
BEGIN
    RETURN QUERY
        SELECT dc.row_number, dc.data->>p_variable_name AS value
        FROM cdisc.dataset_contents dc
        WHERE dc.dataset_id = p_dataset_id
          AND dc.data->>p_variable_name IS NOT NULL
          AND dc.data->>p_variable_name != ''
          AND NOT (dc.data->>p_variable_name = ANY(p_allowed_values));
END;
$$ LANGUAGE plpgsql;

-- Check relationship between variables
CREATE OR REPLACE FUNCTION cdisc.check_variable_relationship(
    p_dataset_id INTEGER,
    p_var1 TEXT,
    p_var2 TEXT,
    p_condition TEXT
) RETURNS TABLE(row_number INTEGER, var1_value TEXT, var2_value TEXT) AS $$
DECLARE
    v_query TEXT;
BEGIN
    -- Build dynamic query based on condition
    v_query := format(
            'SELECT row_number, data->>%L AS var1_value, data->>%L AS var2_value
             FROM cdisc.dataset_contents
             WHERE dataset_id = %L
             AND NOT (%s)',
            p_var1, p_var2, p_dataset_id, p_condition
               );

    RETURN QUERY EXECUTE v_query;
END;
$$ LANGUAGE plpgsql;