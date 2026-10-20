## Dataset Contents Check against Define XML and Library Metadata

_Not supported by the SQL engine._ The SQL engine validates the rule as `Record Data`: the rule macro is not applied.

### Columns

Columns are the columns within the original dataset

### Rule Macro

Attach define xml metadata at variable level and library metadata at variable level

## Dataset Metadata Check

### Columns

- `dataset_label`
- `dataset_location`
- `dataset_name`
- `dataset_size`
- `record_count`

### Example

```yaml
all:
  - name: dataset_name
    operator: longer_than
    value: 6
```

## Dataset Contents Check against Define XML

### Columns

Columns are the columns within the original dataset along with the following columns:

- `dataset_label`
- `dataset_location`
- `dataset_name`
- `dataset_size`
- `dataset_domain`
- `define_dataset_class`
- `define_dataset_domain`
- `define_dataset_has_no_data`
- `define_dataset_is_non_standard`
- `define_dataset_key_sequence`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_name`
- `define_dataset_structure`
- `define_dataset_variables`
- `define_dataset_variable_order` _(SQL engine only)_
- `define_key_sequence_is_unique` _(SQL engine only)_

## Dataset Metadata Check against Define XML

### Columns

- `dataset_size`
- `dataset_location`
- `dataset_name`
- `dataset_label`
- `dataset_domain`
- `define_dataset_name`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_domain`
- `define_dataset_class`
- `define_dataset_structure`
- `define_dataset_is_non_standard`
- `define_dataset_has_no_data`
- `define_dataset_variables`
- `define_dataset_key_sequence`
- `define_dataset_variable_order` _(SQL engine only)_

### Rule Macro

Allows comparing content metadata to define xml metadata of the same name.

### Example

```yaml
all:
  - name: dataset_name
    operator: not_equal_to
    value: define_dataset_name
```

Or

```yaml
any:
  - name: dataset_name
    operator: empty
  - name: define_dataset_name
    operator: empty
```

## Define Item Group Metadata Check

### Columns

- `define_dataset_name`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_domain`
- `define_dataset_class`
- `define_dataset_structure`
- `define_dataset_is_non_standard`
- `define_dataset_has_no_data`
- `define_dataset_variables`
- `define_dataset_key_sequence`
- `define_dataset_variable_order` _(SQL engine only)_

## Define Item Metadata Check

### Columns

- `define_variable_name`
- `define_variable_label`
- `define_variable_data_type`
- `define_variable_role`
- `define_variable_length`
- `define_variable_ccode`
- `define_variable_format`
- `define_variable_allowed_terms`
- `define_variable_origin_type`
- `define_variable_source_type`
- `define_variable_is_collected`
- `define_variable_has_no_data`
- `define_variable_order_number`
- `define_variable_has_codelist`
- `define_variable_codelist_coded_values`
- `define_variable_codelist_coded_codes`
- `define_variable_mandatory`
- `define_variable_has_comment`

### Rule Macro

Attach variable codelist and codelist terms

**SQL engine:** the rule macro is not applied.

## Define Item Metadata Check against Library Metadata

### Columns

- `define_variable_name`
- `define_variable_label`
- `define_variable_data_type`
- `define_variable_role`
- `define_variable_length`
- `define_variable_ccode`
- `define_variable_format`
- `define_variable_allowed_terms`
- `define_variable_origin_type`
- `define_variable_source_type`
- `define_variable_is_collected`
- `define_variable_has_no_data`
- `define_variable_order_number`
- `define_variable_has_codelist`
- `define_variable_codelist_coded_values`
- `define_variable_codelist_coded_codes`
- `define_variable_mandatory`
- `define_variable_has_comment`
- `library_variable_name`
- `library_variable_order_number`
- `library_variable_label`
- `library_variable_data_type`
- `library_variable_role`
- `library_variable_core`
- `library_variable_ccode` _(SQL engine only)_

### Rule Macro

Attach variable codelist and codelist terms

**SQL engine:** the rule macro is not applied.

## Domain Presence Check

### Columns

Single row contains a column for each domain and the value of that column is the domain's file name

| AE     | EC     |
| ------ | ------ |
| ae.xpt | ec.xpt |

**SQL engine:** columns are named after the domain of each dataset, and datasets without a domain are left out. When several files have the same domain, the value is their file names separated by commas (e.g. `dm1.xpt,dm2.xpt`).

### Example

```yaml
all:
  - name: PP
    operator: exists
  - name: PC
    operator: not_exists
```

## Domain Presence Check against Define XML

_SQL engine only._

### Columns

One row per dataset (ItemGroupDef) in the Define-XML, with the following columns:

- `domain`
- `filename`
- `define_dataset_name`
- `define_dataset_label`
- `define_dataset_location`
- `define_dataset_domain`
- `define_dataset_class`
- `define_dataset_structure`
- `define_dataset_is_non_standard`
- `define_dataset_has_no_data`

`domain` is the Define-XML dataset name. `filename` is the file name of the submitted dataset whose domain is that name, and is empty when no such dataset was submitted.

### Example

Report the SE dataset when the Define-XML says it has data but it was not submitted:

```yaml
all:
  - name: define_dataset_name
    operator: equal_to
    value: SE
    value_is_literal: true
  - name: define_dataset_has_no_data
    operator: equal_to
    value: false
  - name: filename
    operator: empty
```

## Global Value Check with Variable Metadata

_SQL engine only._

### Columns

One row per value of each variable of each record, for every dataset in the study:

- `row_number`
- `dataset_name`
- `variable_name`
- `variable_value`
- `variable_label`
- `variable_data_type`
- `variable_length`
- `variable_order_number`
- `variable_format`
- `variable_value_length`

`row_number` is the position of the record within its dataset. The dataset is the same whichever dataset is being validated, so use `Sensitivity: Group` with `filter_by_dataset` in `Grouping_Variables` to report each issue against its own dataset (see [Sensitivity](Sensitivity.md)).

If the rule uses the `max_variable_value_length`, `global_max_variable_length` or `global_min_variable_length` operation, the dataset instead has one row per variable of each dataset, as for `Global Variable Metadata Check`.

### Example

Report variables whose length differs between datasets:

```yaml
Check:
  any:
    - name: $global_max_variable_length
      operator: not_equal_to
      value: $global_min_variable_length
    - name: variable_length
      operator: not_equal_to
      value: $global_max_variable_length
Operations:
  - id: $global_max_variable_length
    domain: global_value_check
    name: variable_length
    operator: max
    group:
      - variable_name
  - id: $global_min_variable_length
    domain: global_value_check
    name: variable_length
    operator: min
    group:
      - variable_name
Sensitivity: Group
Grouping_Variables:
  - variable_name
  - dataset_name
  - filter_by_dataset
```

## Global Variable Metadata Check

_SQL engine only._

### Columns

One row per variable of each dataset in the study (datasets without records are left out):

- `dataset_name`
- `variable_name`
- `variable_label`
- `variable_data_type`
- `variable_length`
- `variable_order_number`
- `variable_format`
- `variable_max_value_length`

As for `Global Value Check with Variable Metadata`, the dataset is the same whichever dataset is being validated, so use `Sensitivity: Group` with `filter_by_dataset` in `Grouping_Variables` to report each issue against its own dataset (see [Sensitivity](Sensitivity.md)).

### Example

Report variables whose length differs between datasets:

```yaml
Check:
  all:
    - name: $global_max_variable_length
      operator: not_equal_to
      value: $global_min_variable_length
Operations:
  - id: $global_max_variable_length
    operator: global_max_variable_length
  - id: $global_min_variable_length
    operator: global_min_variable_length
Sensitivity: Group
Grouping_Variables:
  - variable_name
  - dataset_name
  - filter_by_dataset
```

## Record Data

### Columns

Columns are the columns within the original dataset

### Example

```yaml
all:
  - name: --SCAT
    operator: non_empty
  - name: --SCAT
    operator: equal_to
    value: --CAT
```

## STF Metadata Check

_SQL engine only._

Checks the Study Tagging File (STF) given with `--stf-file-path` (`-stf`).

### Columns

One row per file tag of each document (`doc-content`) of the STF. A document without file tags has one row with empty `file_tag_name` and `file_tag_info_type`.

- `study_id`
- `study_title`
- `dtd_version`
- `language`
- `categories_summary`
- `doc_index`
- `document_title`
- `document_href`
- `document_href_path`
- `document_href_fragment`
- `document_properties_json`
- `document_properties_summary`
- `document_operation`
- `file_tag_name`
- `file_tag_info_type`

The dataset is the same whichever dataset is being validated, so use `Sensitivity: Study` to report an issue once.

### Example

Report a missing `data-tabulation-dataset-sdtm` file tag:

```yaml
all:
  - name: file_tag_name
    operator: not_contains_all
    value:
      - data-tabulation-dataset-sdtm
    value_is_literal: true
```

## Value Check with Dataset Metadata

### Columns

- `row_number`
- `variable_name`
- `variable_value`
- `dataset_label`
- `dataset_location`
- `dataset_name`
- `dataset_size`
- `record_count`

### Example

```yaml
all:
  - name: variable_name
    operator: starts_with
    value: "DM"
  - name: dataset_name
    operator: not_equal_to
    value: "DM"
```

## Value Check with Variable Metadata

### Columns

- `row_number`
- `variable_name`
- `variable_value`
- `variable_order_number`
- `variable_label`
- `variable_length`
- `variable_data_type`
- `variable_format`
- `variable_value_length`

**SQL engine:** if the rule uses the `max_variable_value_length`, `global_max_variable_length` or `global_min_variable_length` operation, the dataset instead has one row per variable of the dataset, with the columns `variable_name`, `variable_label`, `variable_data_type`, `variable_length`, `variable_order_number`, `variable_format` and `variable_max_value_length`.

### Example

```yaml
all:
  - name: variable_data_type
    operator: equal_to
    value: Char
  - name: variable_value
    operator: longer_than
    value: 200
```

## Value Check against Define XML Variable

### Columns

- `row_number`
- `variable_name`
- `variable_value`
- `define_variable_name`
- `define_variable_label`
- `define_variable_data_type`
- `define_variable_`...

### Example

```yaml
all:
  - name: define_variable_ccode
    operator: empty
  - name: variable_value
    operator: non_empty
  - name: define_variable_has_codelist
    operator: equal_to
    value: true
  - name: variable_value
    operator: is_not_contained_by
    value: define_variable_codelist_coded_values
```

**SQL engine:** `define_variable_codelist_coded_values` is comma-separated text in the SQL engine, so use a regular expression such as `(^|,)X(,|$)` instead of `is_not_contained_by` (see [MetaVariables](MetaVariables.md)).

## Value Check against Define XML VLM

### Columns

- `row_number`
- `variable_name`
- `variable_value`
- `define_vlm_name`
- `define_vlm_label`
- `define_vlm_data_type`
- `define_vlm_is_collected`
- `define_vlm_role`
- `define_vlm_ccode`
- `define_vlm_format`
- `define_vlm_allowed_terms`
- `define_vlm_origin_type`
- `define_vlm_has_no_data`
- `define_vlm_order_number`
- `define_vlm_length`
- `define_vlm_has_codelist`
- `define_vlm_codelist_coded_values`
- `define_vlm_mandatory`
- `define_vlm_has_comment`
- `define_variable_name`
- `type_check` _(not available in the SQL engine)_
- `length_check` _(not available in the SQL engine)_
- `variable_value_length`

**SQL engine:** the dataset also has every column of the record the value belongs to, so a condition can check other variables of that record (a record column with the same name as one of the columns above is left out). A VLM applies to the records that match every range check of its where clause; a VLM whose where clause uses a comparator other than `LT`, `LE`, `GT`, `GE`, `EQ`, `NE`, `IN` and `NOTIN`, or a variable that is not in the dataset, is skipped.

### Example

```yaml
all:
  - name: define_vlm_ccode
    operator: empty
  - name: variable_value
    operator: non_empty
  - name: define_vlm_has_codelist
    operator: equal_to
    value: true
  - name: variable_value
    operator: is_not_contained_by
    value: define_vlm_codelist_coded_values
```

**SQL engine:** `define_vlm_codelist_coded_values` is comma-separated text in the SQL engine, so use a regular expression such as `(^|,)X(,|$)` instead of `is_not_contained_by` (see [MetaVariables](MetaVariables.md)).

```yaml
all:
  - name: variable_value
    operator: empty
  - name: define_vlm_mandatory
    operator: equal_to
    value: "Yes"
```

```yaml
all:
  - name: variable_name
    operator: equal_to
    value: RSCAT
    value_is_literal: true
  - name: define_vlm_ccode
    operator: equal_to
    value: C118971
    value_is_literal: true
  - name: RSEVAL
    operator: non_empty
```

## Value Check against Library Metadata

_SQL engine only._

### Columns

One row per value of each variable of each record, with the CDISC Library metadata of the variable (empty when the variable is not in the Library):

- `row_number`
- `variable_name`
- `variable_value`
- `library_variable_name`
- `library_variable_label`
- `library_variable_data_type`
- `library_variable_role`
- `library_variable_core`
- `library_variable_ccode`
- `library_variable_order_number`
- `library_variable_ccode_values`
- `library_variable_codelist_name`
- `library_variable_codelist_value`

### Example

Report a value that is not a term of the variable's non-extensible codelist:

```yaml
Check:
  all:
    - name: library_variable_ccode
      operator: is_contained_by
      value: $non_extensible_codelist_codes
    - name: variable_value
      operator: non_empty
    - name: variable_value
      operator: is_not_substring_of
      value: library_variable_ccode_values
Operations:
  - id: $non_extensible_codelist_codes
    operator: get_codelist_attributes
    ct_attribute: Codelist Code
    ct_conditions:
      - Extensible: "No"
      - Standard Type: sdtm
```

## Value Level Metadata Check against Define XML

_Not supported by the SQL engine._ The SQL engine validates the rule as `Record Data`: the rule macro is not applied.

### Columns

Columns are the columns within the original dataset

### Rule Macro

Attach define xml metadata at value level

## Variable Metadata Check

### Columns

- `variable_name`
- `variable_order_number`
- `variable_label`
- `variable_length`
- `variable_data_type`
- `variable_format`

### Example

```yaml
all:
  - name: variable_label
    operator: longer_than
    value: 40
```

## Variable Metadata Check against Define XML

### Columns

- `variable_name`
- `variable_order_number`
- `variable_label`
- `variable_`...
- `define_variable_name`
- `define_variable_label`
- `define_variable_`...

### Rule Macro

Attach define xml metadata at variable level

**SQL engine:** the rule macro is not applied: each condition is evaluated as written.

### Example

```yaml
all:
  - name: variable_name
    operator: not_equal_to
    value: define_variable_name
```

## Variable Metadata Check against Library Metadata

### Columns

- `variable_name`
- `variable_order_number`
- `variable_label`
- `variable_length`
- `variable_data_type`
- `variable_format`
- `variable_has_empty_values`
- `library_variable_name`
- `library_variable_order_number`
- `library_variable_label`
- `library_variable_data_type`
- `library_variable_role`
- `library_variable_core`
- `library_variable_ccode` _(SQL engine only)_

## Variable Metadata Check against Define XML and Library Metadata

### Columns

- `variable_name`
- `variable_label`
- `variable_length`
- `variable_order_number`
- `variable_data_type`
- `define_variable_name`
- `define_variable_label`
- `define_variable_data_type`
- `define_variable_is_collected`
- `define_variable_role`
- `define_variable_length`
- `define_variable_ccode`
- `define_variable_format`
- `define_variable_allowed_terms`
- `define_variable_origin_type`
- `define_variable_source_type`
- `define_variable_has_no_data`
- `define_variable_order_number`
- `define_variable_has_codelist`
- `define_variable_codelist_coded_values`
- `define_variable_codelist_coded_codes`
- `define_variable_mandatory`
- `define_variable_has_comment`
- `library_variable_name`
- `library_variable_role`
- `library_variable_label`
- `library_variable_core`
- `library_variable_order_number`
- `library_variable_data_type`
- `variable_has_empty_values`
- `variable_count` _(SQL engine only)_
- `variable_is_empty` _(SQL engine only)_
- `library_variable_ccode` _(SQL engine only)_
