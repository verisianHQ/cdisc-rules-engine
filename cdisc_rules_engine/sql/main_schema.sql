-- core data tables with partitioning for large datasets
CREATE TABLE datasets (
    dataset_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_name VARCHAR(255) NOT NULL,
    dataset_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,
    UNIQUE(dataset_name, dataset_type)
);

-- main data table with dynamic columns
CREATE TABLE dataset_records (
    record_id BIGSERIAL,
    dataset_id UUID REFERENCES datasets(dataset_id),
    row_num BIGINT,
    data JSONB,
    PRIMARY KEY (dataset_id, record_id)
) PARTITION BY HASH (dataset_id);

-- create partitions for better performance
CREATE TABLE dataset_records_p0 PARTITION OF dataset_records
    FOR VALUES WITH (modulus 4, remainder 0);
CREATE TABLE dataset_records_p1 PARTITION OF dataset_records
    FOR VALUES WITH (modulus 4, remainder 1);
CREATE TABLE dataset_records_p2 PARTITION OF dataset_records
    FOR VALUES WITH (modulus 4, remainder 2);
CREATE TABLE dataset_records_p3 PARTITION OF dataset_records
    FOR VALUES WITH (modulus 4, remainder 3);

-- indexed column values for fast filtering
CREATE TABLE dataset_columns (
    dataset_id UUID REFERENCES datasets(dataset_id),
    column_name VARCHAR(255),
    column_type VARCHAR(50),
    column_index INTEGER,
    PRIMARY KEY (dataset_id, column_name)
);

-- materialised views for frequently accessed data
CREATE MATERIALISED VIEW dataset_stats AS
SELECT 
    dataset_id,
    COUNT(*) as row_count,
    pg_size_pretty(pg_total_relation_size('dataset_records')) as size
FROM dataset_records
GROUP BY dataset_id;

-- indexes for performance
CREATE INDEX idx_dataset_records_data_gin ON dataset_records USING GIN (data);
CREATE INDEX idx_dataset_records_row_num ON dataset_records (dataset_id, row_num);