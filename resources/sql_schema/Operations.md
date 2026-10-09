## define_variable_metadata

If a target variable `name` is specified, returns the specified metadata in the define for the specified target variable.

- Input

  ```yaml
  - operator: define_variable_metadata
    attribute_name: define_variable_label
    name: LBTESTCD
    id: $LBTESTCD_VARIABLE_LABEL
  ```

- Output

  `Laboratory Test Code`

If no target variable `name` specified, returns a dictionary containing the specified metadata in the define for all variables.

- Input

  ```yaml
  - operator: define_variable_metadata
    attribute_name: define_variable_label
    id: $VARIABLE_LABEL
  ```

- Output

  ```json
  {
    "STUDYID": "Study Identifier",
    "USUBJID": "Unique Subject Identifier",
    "LBTESTCD": "Laboratory Test Code",
    "...": "..."
  }
  ```

## dataset_names

Returns a list of the submitted dataset filenames in all uppercase

ex. if TS.xpt, AE.xpt, EC.xpt, and SUPPEC.xpt are submitted -> [TS, AE, EC, SUPPEC] will be returned

## distinct

Get a distinct list of values for the given `name`. If a `group` list is specified, the distinct value list will be grouped by the variables within `group`.

If `group` is provided, `group_aliases` may also be provided to assign new grouping variable names so that results grouped by the values in one set of grouping variables can be merged onto a dataset according to the same grouping value(s) stored in different set of grouping variables. When both `group` and `group_aliases` are provided, columns are renamed according to corresponding list position (i.e., the 1st column in `group` is renamed to the 1st column in `group_aliases`, etc.). If there are more columns listed in `group` than in `group_aliases`, only the `group` columns with corresponding `group_aliases` columns will be renamed. If there are more columns listed in `group_aliases` than in `group`, the extra column names in `group_aliases` will be ignored. See [record_count](#record_count) for an example of the use of `group_aliases`.

```yaml
Check:
  all:
    - name: SSSTRESC
      operator: equal_to
      value: DEAD
      value_is_literal: true
    - name: $ds_dsdecod
      operator: does_not_contain
      value: DEATH
      value_is_literal: true
Operations:
  - operator: distinct
    domain: DS
    name: DSDECOD
    id: $ds_dsdecod
    group:
      - USUBJID
```

## domain_is_custom

Checks whether the domain is in the set of domains within the provided standard.

- Input

  Target Domain: `XY`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  Operations:
    - operator: domain_is_custom
      id: $domain_is_custom
  ```

- Output

  `true`

## domain_label

Returns the label for the domain the operation is executing on within the provided standard.

- Input.

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  Operations:
    - operator: domain_label
      id: $domain_label
  ```

- Output

  `Laboratory Test Results`

## dy

Calculates the number of days between the DTC and RFSTDTC. The Study Day value is incremented by 1 for each date following RFSTDTC. Dates prior to RFSTDTC are decreased by 1, with the date preceding RFSTDTC designated as Study Day -1 (there is no Study Day 0). . . . All Study Day values are integers. Thus, to calculate Study Day:

- `--DY = (date portion of --DTC) - (date portion of RFSTDTC) + 1 if --DTC is on or after RFSTDTC`
- `--DY = (date portion of --DTC) - (date portion of RFSTDTC) if --DTC precedes RFSTDTC`

This algorithm should be used across all domains.

```yaml
Check:
  all:
    - name: --DY
      operator: non_empty
    - name: --DTC
      operator: is_complete_date
    - name: RFSTDTC
      operator: is_complete_date
    - name: --DY
      operator: not_equal_to
      value: $dy
Operations:
  - name: --DTC
    operator: dy
    id: $dy
Match Datasets:
  - Name: DM
    Keys:
      - USUBJID
```

## expected_variables

Returns the expected ("Core" = Exp ) variables for the domain in the current standard
Variable Metadata for custom domains will pull from the model while non-custom domains will be from the IG and Model.

- Input:

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operator: expected_variables
    id: $expected_variables
  ```

- Output:

  ```json
  ["LBCAT", "LBORRES", "LBORRESU", "..."]
  ```

## extract_metadata

Returns the requested dataset level metadata value for the current dataset. Possible `name` values are:

- `dataset_size`
- `dataset_location`
- `dataset_name`
- `dataset_label`

Example

- Input:

  Target domain: `LB`

  ```yaml
  - name: dataset_label
    operator: extract_metadata
    id: $dataset_label
  ```

- Output:

  `Laboratory Test Results`

## get_codelist_attributes

Returns the values of a controlled terminology attribute (`ct_attribute`) for the CT version(s) in use.

- `ct_attribute`: one of `Term CCODE`, `Term Signification`, `Term`, `Codelist Code`, `Codelist Name`, `Extensible`, `Synonyms`, `Definition`, `Standard Type`, `Version Date`, `Standard and Date`
- `ct_conditions` (optional): a list of `{attribute: value}` filters on the same attributes
- `version` (optional): a variable holding the CT version (e.g. `TSVCDVER`) or a literal version. Without it, the CT packages provided to the engine are used.

```yaml
- id: $VALID_TERM_CODES
  operator: get_codelist_attributes
  ct_attribute: Term CCODE
  version: TSVCDVER
  ct_conditions:
    - Codelist Code: C66742
```

## get_column_order_from_dataset

Returns list of dataset columns in order

```yaml
Check:
  all:
    - name: $column_order_from_dataset
      operator: is_not_ordered_subset_of
      value: $library_with_dataset_only
Operations:
  - id: $column_order_from_library
    operator: get_column_order_from_library
  - id: $column_order_from_dataset
    operator: get_column_order_from_dataset
  - id: $library_with_dataset_only
    operator: intersect
    name: $column_order_from_library
    subtract: $column_order_from_dataset
```

## get_column_order_from_library

Fetches column order for a given domain from the CDISC library. The lists with column names are sorted in accordance to "ordinal" key of library metadata.

```yaml
Rule Type: Variable Metadata Check
Check:
  all:
    - name: variable_name
      operator: is_not_contained_by
      value: $ig_variables
Operations:
  - id: $ig_variables
    operator: get_column_order_from_library
```

## get_model_column_order

Fetches column order for a given model class from the CDISC library. The lists with column names are sorted in accordance to "ordinal" key of library metadata.

```yaml
Rule Type: Variable Metadata Check
Check:
  all:
    - name: variable_name
      operator: is_not_contained_by
      value: $model_variables
Operations:
  - id: $model_variables
    operator: get_model_column_order
```

## get_model_filtered_variables

Fetches variable level library model properties filtered by the provided `key_name` and `key_value`

Example

- Input

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operator: get_model_filtered_variables
    id: $model_filtered_variables
    key_name: "role"
    key_value: "Timing"
  ```

- Output

  ```json
  ["VISITNUM", "VISIT", "VISITDY", "TAETORD", "..."]
  ```

## get_parent_model_column_order

Fetches column order for a given SUPP's parent model class from the CDISC library. The lists with column names are sorted in accordance to "ordinal" key of library metadata.

```yaml
Check:
  all:
    - operator: is_not_contained_by
      value: $parent_model_variables
Operations:
  - id: $parent_model_variables
    operator: get_parent_model_column_order
```

## label_referenced_variable_metadata

Generates a dataframe where each record in the dataframe is the library ig variable metadata corresponding with the variable label found in the column provided in `name`

- Input

  Target Dataset: `SUPPLB`

  Product: `sdtmig`

  Version: `3-4`

  Dataset:

  ```json
  {
    "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
    "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1"],
    "QLABEL": ["Toxicity", "Viscosity", "Analysis Method"]
  }
  ```

  Rule:

  ```yaml
  - operator: label_referenced_variable_metadata
    id: $label_referenced_variable_metadata
    name: "QLABEL"
  ```

- Output

  ```json
  {
    "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
    "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1"],
    "QLABEL": ["Toxicity", "Viscosity", "Analysis Method"],
    "$label_referenced_variable_name": ["LBTOX", null, "LBANMETH"],
    "$label_referenced_variable_role": [
      "Variable Qualifier",
      null,
      "Record Qualifier"
    ],
    "$label_referenced_variable_ordinal": [44, null, 38],
    "$label_referenced_variable_label": ["Toxicity", null, "Analysis Method"]
  }
  ```

## max

If no `group` is provided, returns the max value in `name`. If `group` is provided, returns the max value in `name`, within each unique set of the grouping variables.

```yaml
Check:
  all:
    - name: "$max_age"
      operator: "greater_than"
      value: "MAXAGE"
Operations:
  - operator: "max"
    domain: "DM"
    name: "AGE"
    id: "$max_age"
```

## max_date

If no `group` is provided, returns the max date value in `name`. If `group` is provided, returns the max date value in `name`, within each unique set of the grouping variables.

```yaml
Check:
  all:
    - name: USUBJID
      operator: is_contained_by
      value: $ex_usubjid
    - name: RFXENDTC
      operator: not_equal_to
      value: $max_ex_exstdtc
    - name: RFXENDTC
      operator: not_equal_to
      value: $max_ex_exendtc
Operations:
  - operator: distinct
    domain: EX
    name: USUBJID
    id: $ex_usubjid
  - operator: max_date
    domain: EX
    name: EXSTDTC
    id: $max_ex_exstdtc
    group:
      - USUBJID
  - operator: max_date
    domain: EX
    name: EXENDTC
    id: $max_ex_exendtc
    group:
      - USUBJID
```

## mean

Example: AAGE > mean(DM.AGE), where AAGE is a fictitious NSV

```yaml
Check:
  all:
    - name: "AAGE"
      operator: "greater_than"
      value: "$average_age"
Operations:
  - operator: "mean"
    domain: "DM"
    name: "AGE"
    id: "$average_age"
```

## min

If no `group` is provided, returns the min value in `name`. If `group` is provided, returns the min value in `name`, within each unique set of the grouping variables.

```yaml
Check:
  all:
    - name: "$min_age"
      operator: "less_than"
      value: "MINAGE"
Operations:
  - operator: "min"
    domain: "DM"
    name: "AGE"
    id: "$min_age"
```

## min_date

If no `group` is provided, returns the min date value in `name`. If `group` is provided, returns the min date value in `name`, within each unique set of the grouping variables.

Example: RFSTDTC is greater than min AE.AESTDTC for the current USUBJID

```yaml
Check:
  all:
    - name: "RFSTDTC"
      operator: "date_greater_than"
      value: "$ae_aestdtc"
Operations:
  - operator: "min_date"
    domain: "AE"
    name: "AESTDTC"
    id: "$ae_aestdtc"
    group:
      - USUBJID
```

## Aggregating over a grouped operation

`max`, `min`, `mean`, `max_date`, `min_date` and `record_count` can aggregate the per-group results of a previous grouped operation by giving its id as `name`. The `group` and `filter` of the outer operation may only use the grouping columns of the inner one.

```yaml
Operations:
  - id: $latest_per_subject
    operator: max_date
    domain: EX
    name: EXENDTC
    group:
      - USUBJID
  - id: $earliest_last_dose
    operator: min_date
    name: $latest_per_subject
```

Optional parameters for these operations:

- `regex`: only aggregate values matching the regular expression (PostgreSQL syntax)
- `ignore_empty_filtered_groups: true`: with `group` and `filter`, leave out groups that have no rows matching the filter instead of returning an empty result for them
- `use_rule_type_table: true`: aggregate over the dataset built by the rule's `Rule Type` (e.g. the variable metadata table) instead of the domain's dataset

## name_referenced_variable_metadata

Generates a dataframe where each record in the dataframe is the library ig variable metadata corresponding with the variable name found in the column provided in `name`

- Input

  Target Dataset: `SUPPLB`

  Product: `sdtmig`

  Version: `3-4`

  Dataset:

  ```json
  {
    "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
    "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1"],
    "QNAM": ["Toxicity", "LBVISCOS", "Analysis Method"]
  }
  ```

  Rule:

  ```yaml
  - operator: name_referenced_variable_metadata
    id: $name_referenced_variable_metadata
    name: "QNAM"
  ```

- Output

  ```json
  {
    "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
    "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1"],
    "QNAM": ["LBTOX", "LBVISCOS", "LBANMETH"],
    "$label_referenced_variable_name": ["LBTOX", null, "LBANMETH"],
    "$label_referenced_variable_role": [
      "Variable Qualifier",
      null,
      "Record Qualifier"
    ],
    "$label_referenced_variable_ordinal": [44, null, 38],
    "$label_referenced_variable_label": ["Toxicity", null, "Analysis Method"]
  }
  ```

## permissible_variables

Returns the permissible variables ("Core" = Perm ) for a given domain and standard
Variable Metadata for custom domains will pull from the model while non-custom domains will be from the IG and Model.

- Input:

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operator: permissible_variables
    id: $permissible_variables
  ```

- Output:

  ```json
  ["LBGRPID", "LBREFID", "LBSPID", "..."]
  ```

## record_count

If no `filter` or `group` is provided, returns the number of records in the dataset. If `filter` is provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter. If `group` is provided, returns the number of rows matching each unique set of the grouping variables. These are column names. If both `filter` and `group` are provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter that also match each unique set of the grouping variables.

**Wildcard Filtering**: Filter values ending with `%` will match any records where the column value starts with the specified prefix. For example, `RACE%` will match `RACE1`, `RACE2`, `RACE3`, etc. This is useful for matching related variables with numeric or alphabetic suffixes.

Example: return the number of records in a dataset.

```yaml
- operator: record_count
  id: $records_in_dataset
```

Example: return the number of records where STUDYID = "CDISC01" and FLAGVAR = "Y".

```yaml
- operator: record_count
  id: $flagged_cdisc01_records_in_dataset
  filter:
    STUDYID: "CDISC01"
    FLAGVAR: "Y"
```

Example: return the number of records where QNAM starts with "RACE" (matches RACE1, RACE2, RACE3, etc.) per USUBJID.

```yaml
- operator: record_count
  id: $race_records_in_dataset
  filter:
    QNAM: "RACE%"
  group:
    - "USUBJID"
```

Example: return the number of records grouped by USUBJID.

```yaml
- operator: record_count
  id: $records_per_usubjid
  group:
    - USUBJID
```

Example: return the number of records grouped by USUBJID where FLAGVAR = "Y".

```yaml
- operator: record_count
  id: $flagged_records_per_usubjid
  group:
    - USUBJID
  filter:
    FLAGVAR: "Y"
```

## required_variables

Returns the required variables ( "Core" = Req ) for a given domain and standard
Variable Metadata for custom domains will pull from the model while non-custom domains will be from the IG and Model.

- Input:

  Target Domain: `LB`

  Product: `sdtmig`

  Version: `3-4`

  ```yaml
  - operator: required_variables
    id: $required_variables
  ```

- Output:

  ```json
  ["STUDYID", "DOMAIN", "USUBJID", "LBSEQ", "LBTESTCD", "LBTEST"]
  ```

## study_domains

Returns a list of the domains in the study

## valid_codelist_dates

Returns the valid terminology package dates for a given standard.

Given a list of terminology packages:

```json
[
  "sdtmct-2023-10-26",
  "sdtmct-2023-12-13",
  "adamct-2023-12-13",
  "cdashct-2023-05-19"
]
```

and standard: `sdtmig`

the operation will return:

```json
["2023-10-26", "2023-12-13"]
```

By default, the standard is as specified when running validation - as the validation runtime parameter and/or as specified in the rule header - and the list of terminology packages is obtained from the current cache. If required, the default standard may be overridden using the optional `ct_package_types` parameter. For example, given the same list of terminology packages, the following operation:

```yaml
Operations:
  - operator: valid_codelist_dates
    id: $valid_dates
    ct_package_types:
      - SDTM
      - CDASH
```

will return:

```json
["2023-05-19", "2023-10-26", "2023-12-13"]
```

## whodrug_code_hierarchy

Determines whether the values are valid and in the correct hierarchical structure in the following variables:

- `--DECOD`
- `--CLAS`
- `--CLASCD`

**Input:**

```yaml
Operations:
  - id: $valid_whodrug_codes
    operator: whodrug_code_hierarchy
```

## variable_count

Returns a mapping of variable names to the number of times that variable appears in a domain within the study.

- Input

  ```json
  {
    "AE": ["STUDYID", "DOMAIN", "USUBJID", "AETERM", "AEENDTC"],
    "LB": ["STUDYID", "DOMAIN", "USUBJID", "LBTESTCD", "LBENDTC"]
  }
  ```

- Output

  ```json
  {
    "STUDYID": 2,
    "DOMAIN": 2,
    "USUBJID": 2,
    "--TERM": 1,
    "--TESTCD": 1,
    "--ENDTC": 2
  }
  ```

## variable_exists

Flag an error if MIDS is in the dataset currently being evaluated and the TM domain is not present in the study

```yaml
Rule Type: Domain Presence Check
Check:
  all:
    - name: $MIDS_EXISTS
      operator: equal_to
      value: true
    - name: TM
      operator: not_exists
Operations:
  - id: $MIDS_EXISTS
    name: MIDS
    operator: variable_exists
```

## variable_is_null

Returns `true` if every record of the variable given by `name` is null or the variable is not in the dataset, otherwise `false`. For Char variables, empty strings and values containing only whitespace (spaces, tabs, newlines) count as null. A dataset with no records returns `true`.

`name` is looked up in the operation's `domain`, which defaults to the dataset being evaluated. `--` in `name` is replaced with the domain. Set `use_rule_type_table: true` to look it up in the rule type's table instead (e.g. `define_variable_has_no_data` or `dataset_label`). If the domain or table doesn't exist, the rule is skipped.

Combine it with `exists` to only flag variables that are present.

Flag an error if AEDUR is in the dataset but has no populated values

```yaml
Check:
  all:
    - name: AEDUR
      operator: exists
    - name: $AEDUR_IS_NULL
      operator: equal_to
      value: true
Operations:
  - id: $AEDUR_IS_NULL
    name: AEDUR
    operator: variable_is_null
```

## calc

Evaluates an arithmetic formula in `value`. The formula may use numbers, dataset variables and operation results, combined with `+`, `-`, `*`, `/` and parentheses. Non-numeric values evaluate to null.

```yaml
- id: $dose_per_kg
  operator: calc
  value: EXDOSE / $baseline_weight
```

## define_extended_terms_in_ct

Returns the extended codelist terms in the define.xml that already exist in the controlled terminology version in use, within the same codelist, as a duplicate submission value, a synonym, or part of a `;`-separated submission value. Each result identifies its codelist, e.g. `Ophthalmic Exam Test Code (C117743): INTP`. Optional parameters: `ct_attribute`, `ct_conditions`, `version` and `case_sensitive` (default true).

```yaml
Check:
  all:
    - name: $duplicate_extended_terms
      operator: non_empty
Operations:
  - id: $duplicate_extended_terms
    operator: define_extended_terms_in_ct
```

## get_countries

Returns the list of country values for the attribute in `attribute_name` (`country_name`, `alpha_2` or `alpha_3`; default `country_name`). `codelist` selects `ISO 3166` or `GENC`; any other value (or none) returns the union of both.

```yaml
- id: $alpha_3_genc_country_codes
  operator: get_countries
  attribute_name: alpha_3
  codelist: GENC
```

## get_define_external_dictionary_version

Returns the version of the external dictionary in `external_dictionary_type` declared in the define.xml

```yaml
- id: $define_meddra_version
  operator: get_define_external_dictionary_version
  external_dictionary_type: meddra
```

## get_external_dictionary_version

Returns the version of the external dictionary in `external_dictionary_type` loaded into the engine

```yaml
- id: $dict_loinc_version
  operator: get_external_dictionary_version
  external_dictionary_type: loinc
```

## intersect

Returns the values in `name` that are also in `subtract`, preserving the order of `name`. Each of `name` and `subtract` may be a variable, a list of literal values or an operation result. Comparison is case sensitive unless `case_sensitive: false`.

```yaml
- id: $library_with_dataset_only
  operator: intersect
  name: $column_order_from_library
  subtract: $column_order_from_dataset
```

## minus

Returns the values in `name` that are not in `subtract`, preserving the order of `name`. Each of `name` and `subtract` may be a variable, a list of literal values or an operation result. Comparison is case sensitive unless `case_sensitive: false`.

```yaml
- id: $missing_expected_variables
  operator: minus
  name: $expected_variables
  subtract: $dataset_variables
```

## referenced_domain_variable_names

For each record, returns the variable names of the study dataset whose domain is the value in `name` (e.g. `RDOMAIN`)

```yaml
Check:
  all:
    - name: IDVAR
      operator: is_not_contained_by
      value: $rdomain_variables
Operations:
  - id: $rdomain_variables
    operator: referenced_domain_variable_names
    domain: SUPP--
    name: RDOMAIN
```

## split

Splits the values in `name` (a variable or operation result) on commas into a list of values, with surrounding whitespace removed.

```yaml
- id: $listed_domains
  operator: split
  name: TSVAL
```

## standard_domains

Returns the domains defined by the standard in use. `domain_class` optionally restricts them to one class (e.g. `FINDINGS`).

```yaml
- id: $valid_domain_names
  operator: standard_domains
```
