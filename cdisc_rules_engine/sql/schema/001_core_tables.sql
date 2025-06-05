-- core schema
CREATE SCHEMA IF NOT EXISTS cdisc;

-- study datasets table
CREATE TABLE IF NOT EXISTS cdisc.datasets (
    dataset_id SERIAL PRIMARY KEY,
    study_id VARCHAR(255) NOT NULL,
    dataset_name VARCHAR(64) NOT NULL,
    dataset_label VARCHAR(255),
    dataset_path TEXT NOT NULL,
    file_size BIGINT,
    record_count INTEGER,
    modification_date TIMESTAMP,
    domain VARCHAR(8),
    standard VARCHAR(32),
    standard_version VARCHAR(16),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(study_id, dataset_name)
);

-- dataset contents (actual data)
CREATE TABLE IF NOT EXISTS cdisc.dataset_contents (
    row_id BIGSERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES cdisc.datasets(dataset_id) ON DELETE CASCADE,
    row_number INTEGER NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- create dataset index
CREATE INDEX idx_dataset_contents_dataset_id ON cdisc.dataset_contents(dataset_id);
CREATE INDEX idx_dataset_contents_data ON cdisc.dataset_contents USING GIN(data);

-- variable metadata
CREATE TABLE IF NOT EXISTS cdisc.variable_metadata (
    variable_id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES cdisc.datasets(dataset_id) ON DELETE CASCADE,
    variable_name VARCHAR(64) NOT NULL,
    variable_label VARCHAR(255),
    variable_type VARCHAR(32),
    variable_length INTEGER,
    variable_format VARCHAR(64),
    variable_order INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id, variable_name)
);

-- rules metadata
CREATE TABLE IF NOT EXISTS cdisc.rules (
    rule_id SERIAL PRIMARY KEY,
    core_id VARCHAR(32) UNIQUE NOT NULL,
    rule_type VARCHAR(128) NOT NULL,
    description TEXT,
    severity VARCHAR(32),
    executability VARCHAR(64),
    sql_template TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- rule execution results
CREATE TABLE IF NOT EXISTS cdisc.validation_results (
    result_id BIGSERIAL PRIMARY KEY,
    rule_id INTEGER REFERENCES cdisc.rules(rule_id),
    dataset_id INTEGER REFERENCES cdisc.datasets(dataset_id),
    execution_id UUID NOT NULL,
    status VARCHAR(32) NOT NULL,
    row_number INTEGER,
    variables TEXT[],
    error_message TEXT,
    error_details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- create validation index
CREATE INDEX idx_validation_results_execution_id ON cdisc.validation_results(execution_id);
CREATE INDEX idx_validation_results_rule_dataset ON cdisc.validation_results(rule_id, dataset_id);

-- define XML metadata
CREATE TABLE IF NOT EXISTS cdisc.define_metadata (
    define_id SERIAL PRIMARY KEY,
    dataset_id INTEGER REFERENCES cdisc.datasets(dataset_id),
    metadata_type VARCHAR(64) NOT NULL,
    metadata JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- library metadata cache
CREATE TABLE IF NOT EXISTS cdisc.library_metadata (
    metadata_id SERIAL PRIMARY KEY,
    standard VARCHAR(32) NOT NULL,
    version VARCHAR(16) NOT NULL,
    metadata_type VARCHAR(64) NOT NULL,
    metadata JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(standard, version, metadata_type)
);