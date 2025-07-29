CREATE TABLE data_metadata (
    -- general dataset metadata
    dataset_filename TEXT NOT NULL,
    dataset_filepath TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    dataset_label TEXT,
    dataset_domain TEXT NOT NULL,
    -- handle supplementary dataset information
    dataset_is_supp BOOLEAN,
    dataset_rdomain TEXT NOT NULL,
    -- handle split dataset information
    dataset_is_split BOOLEAN,
    dataset_unsplit_name TEXT NOT NULL,
    -- handle pre-processed dataset information
    dataset_preprocessed BOOLEAN,
    -- variable metadata
    var_name TEXT NOT NULL,
    var_label TEXT,
    var_type TEXT,
    var_length SMALLINT,
    var_format TEXT
);

CREATE INDEX idx_data_metadata_dataset_id ON data_metadata(dataset_id);
CREATE INDEX idx_data_metadata_dataset_name ON data_metadata(dataset_name);
CREATE INDEX idx_data_metadata_dataset_domain ON data_metadata(dataset_domain);
CREATE INDEX idx_data_metadata_var_name ON data_metadata(var_name);