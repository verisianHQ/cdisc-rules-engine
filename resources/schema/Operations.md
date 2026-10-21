# Summary

| Operations                                                                                                       | What they return                                                           |
| ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| **Operations**                                                                                                   |                                                                            |
| [`calc`](#calc)                                                                                                  | Arithmetic formula evaluated for each record.                              |
| [`dataset_names`](#dataset_names)                                                                                | Names of the submitted datasets.                                           |
| [`define_extended_terms_in_ct`](#define_extended_terms_in_ct)                                                    | Sponsor-extended define.xml terms that already exist in the CT codelist.   |
| [`define_variable_metadata`](#define_variable_metadata)                                                          | A variable's metadata from the define.xml.                                 |
| [`distinct`](#distinct)                                                                                          | Distinct values of a variable, optionally per group.                       |
| [`domain_is_custom`](#domain_is_custom)                                                                          | Whether the domain is not in the standard.                                 |
| [`domain_label`](#domain_label)                                                                                  | Label of the domain in the standard.                                       |
| [`dy`](#dy)                                                                                                      | Study day of a --DTC date, relative to DM.RFSTDTC.                         |
| [`expected_variables`](#expected_variables), `permissible_variables`, `required_variables`                       | Expected, permissible or required variables of the domain.                 |
| [`extract_metadata`](#extract_metadata)                                                                          | Dataset level metadata value (e.g. dataset name).                          |
| [`get_codelist_attributes`](#get_codelist_attributes)                                                            | Values of a controlled terminology attribute (e.g. term codes).            |
| [`get_column_order_from_dataset`](#get_column_order_from_dataset)                                                | Variable names in the order of the dataset.                                |
| [`get_column_order_from_library`](#get_column_order_from_library)                                                | Variable names in the library order for the domain.                        |
| [`get_countries`](#get_countries)                                                                                | List of countries (ISO 3166 or GENC).                                      |
| [`get_model_column_order`](#get_model_column_order)                                                              | Variable names in the library order for the model class.                   |
| [`get_model_filtered_variables`](#get_model_filtered_variables)                                                  | Model variables filtered by library metadata.                              |
| [`get_parent_model_column_order`](#get_parent_model_column_order)                                                | Variable names in the library order for a SUPP dataset's parent class.     |
| [`global_max_variable_length`](#global_max_variable_length), `global_min_variable_length`                        | Largest or smallest declared length of each variable across all datasets.  |
| [`intersect`](#intersect)                                                                                        | Values of a list that are also in another list.                            |
| [`label_referenced_variable_metadata`](#label_referenced_variable_metadata), `name_referenced_variable_metadata` | Library metadata of the variables whose label or name is held in a column. |
| [`max`](#max), `min`, `mean`                                                                                     | Largest, smallest or mean value, optionally per group.                     |
| [`max_date`](#max_date), `min_date`                                                                              | Latest or earliest date, optionally per group.                             |
| [`max_variable_value_length`](#max_variable_value_length)                                                        | Length of the longest value of each variable.                              |
| [`minus`](#minus)                                                                                                | Values of a list that are not in another list.                             |
| [`record_count`](#record_count)                                                                                  | Number of records, optionally filtered and per group.                      |
| [`referenced_domain_variable_names`](#referenced_domain_variable_names)                                          | Variable names of the dataset whose domain is held in a column.            |
| [`split`](#split)                                                                                                | Comma-separated text split into a list of values.                          |
| [`standard_domains`](#standard_domains)                                                                          | Domains of the standard being validated.                                   |
| [`study_domains`](#study_domains)                                                                                | Domains of the study.                                                      |
| [`valid_codelist_dates`](#valid_codelist_dates)                                                                  | Valid terminology package dates for a standard.                            |
| [`variable_count`](#variable_count)                                                                              | Number of datasets containing a variable.                                  |
| [`variable_exists`](#variable_exists)                                                                            | Whether a variable exists in the dataset.                                  |
| [`variable_is_null`](#variable_is_null)                                                                          | Whether a variable has no populated values.                                |
| **External Dictionary Validation Operations**                                                                    |                                                                            |
| [`get_define_external_dictionary_version`](#get_define_external_dictionary_version)                              | Dictionary version given in the define.xml.                                |
| [`get_external_dictionary_version`](#get_external_dictionary_version)                                            | Dictionary version read from the dictionary files.                         |
| [`whodrug_code_hierarchy`](#whodrug_code_hierarchy)                                                              | Whether WHODrug values follow the ATC hierarchy.                           |

# Operations

## calc

Evaluates the arithmetic formula in `value` for each record and returns a number. The formula can contain:

- numbers, e.g. `100` or `2.5`
- names of columns in the dataset being evaluated, e.g. `AVAL`
- ids of previous operations that return a single value, e.g. `$max_aval`
- `+`, `-`, `*`, `/` and parentheses

The usual precedence applies: `*` and `/` before `+` and `-`. A record gets an empty result if a value it uses is null or is not a number, or if the formula divides by zero. `--` can't be used in the formula, so write full variable names.

Example: flag records where PCHG is not the percent change from baseline

```yaml
Check:
  all:
    - name: PCHG
      operator: not_equal_to
      value: $pchg
Operations:
  - id: $pchg
    operator: calc
    value: ((AVAL - BASE) / BASE) * 100
```

## define_extended_terms_in_ct

Returns the sponsor-extended terms of extensible codelists in the define.xml that already exist in the same codelist of the CT in use, as:

- the same submission value
- a synonym of a term
- a subset of a `;`-separated submission value, i.e. every `;`-separated part of the extended term is a part of the submission value (e.g. `bacteria` in `bacteria; bacteriophage`)

Each result names the define.xml codelist, its code and the extended term, e.g. `Ophthalmic Exam Test Code (C117743): INTP`. Matching is case-sensitive unless `case_sensitive` is `false`. Terms are compared with the CT packages given for the validation run, or with all loaded CT packages if none are given.

```yaml
Check:
  all:
    - name: $duplicate_extended_terms
      operator: non_empty
Operations:
  - id: $duplicate_extended_terms
    operator: define_extended_terms_in_ct
```

## define_variable_metadata

If a target variable `name` is specified, returns the specified metadata in the define for the specified target variable. `--` in `name` is replaced with the domain. If the domain or the variable is not found in the define, the rule fails with an execution error.

- Input

  ```yaml
  - operator: define_variable_metadata
    attribute_name: define_variable_label
    name: LBTESTCD
    id: $LBTESTCD_VARIABLE_LABEL
  ```

- Output

  `Laboratory Test Code`

## dataset_names

Returns a list of the submitted dataset filenames in all uppercase. The order of the names is not guaranteed.

ex. if TS.xpt, AE.xpt, EC.xpt, and SUPPEC.xpt are submitted -> [TS, AE, EC, SUPPEC] will be returned

## distinct

Get a distinct list of values for the given `name`. If a `group` list is specified, the distinct value list will be grouped by the variables within `group`.

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

`filter` can be used to only take values from records whose columns have the given values. Values must match exactly: the `%` wildcard of [record_count](#record_count) isn't supported.

```yaml
- id: $empty_tsval_tsparmcds
  operator: distinct
  name: TSPARMCD
  filter:
    TSVAL: ""
```

## domain_is_custom

Returns `true` if the domain is not in the set of domains within the provided standard (i.e. it is a custom domain), otherwise `false`.

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

- Input

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

Calculates the number of days between the --DTC variable given in `name` and DM.RFSTDTC (matched on USUBJID). The Study Day value is incremented by 1 for each date following RFSTDTC. Dates prior to RFSTDTC are decreased by 1, with the date preceding RFSTDTC designated as Study Day -1 (there is no Study Day 0). All Study Day values are integers. Thus, to calculate Study Day:

- `--DY = (date portion of --DTC) - (date portion of RFSTDTC) + 1 if --DTC is on or after RFSTDTC`
- `--DY = (date portion of --DTC) - (date portion of RFSTDTC) if --DTC precedes RFSTDTC`

This algorithm should be used across all domains.

If either date is missing or cannot be parsed as a date, the result for that record is empty. If no DM dataset is provided, the result is `0` for every record.

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

If the dataset being evaluated already has an RFSTDTC column, e.g. after `Match Datasets` with DM as above, that column is used instead of joining DM again. Partial dates are compared at the precision they share: e.g. `2023-01` and `2023-01-05` are compared as months, which gives `1`.

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

- `dataset_size` (file size in bytes)
- `dataset_location` (dataset filename)
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

Returns the distinct values of a controlled terminology attribute, taken from the CT packages of the run. `ct_attribute` is the attribute to return: `Term CCODE`, `Codelist Code`, `Codelist Name`, `Term`, `Term Signification`, `Synonyms`, `Definition`, `Extensible`, `Standard Type`, `Version Date` or `Standard and Date`.
Has optional parameters:

- `ct_conditions`: a list of conditions, each an attribute (with the same names as `ct_attribute`) and a value, that the returned terms must all meet. A `null` value matches terms where the attribute is empty.
- `version`: the CT package version date to use (e.g. `2020-03-27`), or the name of a variable holding each record's CT package version date (like TSVCDVER in TS). A literal version date is only used when no CT packages are given for the run. For records whose version is empty or is not one of the loaded CT packages, the CT packages of the run are used.

> Codes of the non-extensible SDTM codelists

```yaml
- id: $non_extensible_codelist_codes
  operator: get_codelist_attributes
  ct_attribute: Codelist Code
  ct_conditions:
    - Extensible: "No"
    - Standard Type: sdtm
```

> Term codes of the CT version given in TSVCDVER for each record

```yaml
- id: $TERM_CCODES
  operator: get_codelist_attributes
  ct_attribute: Term CCODE
  version: TSVCDVER
```

## get_column_order_from_dataset

Returns the list of the current dataset's column names, in the order in which they appear in the dataset.

```yaml
Check:
  all:
    - name: $column_order_from_dataset
      operator: is_not_ordered_subset_of
      value: $column_order_from_library
Operations:
  - id: $column_order_from_library
    operator: get_column_order_from_library
  - id: $column_order_from_dataset
    operator: get_column_order_from_dataset
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

`key_name` and `key_value` can be given to only return the variables whose library metadata attribute `key_name` equals `key_value`, as for [get_model_filtered_variables](#get_model_filtered_variables).

```yaml
- id: $library_timing_variables
  operator: get_column_order_from_library
  key_name: role
  key_value: Timing
```

## get_countries

Returns a list of countries. `attribute_name` sets what is returned for each country: `country_name` (default), `alpha_2` or `alpha_3`. `codelist` sets the country list: `ISO 3166` or `GENC`. Without `codelist`, the ISO 3166 list is used, plus the GENC countries whose name isn't in it.

Example: flag FCNTRY values in TS that aren't GENC alpha-3 codes

```yaml
Check:
  all:
    - name: TSPARMCD
      operator: equal_to
      value: FCNTRY
      value_is_literal: true
    - name: $alpha_3_genc_country_codes
      operator: does_not_contain
      value: TSVAL
Operations:
  - id: $alpha_3_genc_country_codes
    operator: get_countries
    attribute_name: alpha_3
    codelist: GENC
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

Fetches column order for a given SUPP's parent model class from the CDISC library. The parent domain of each record is taken from its `RDOMAIN` value. The lists with column names are sorted in accordance to "ordinal" key of library metadata.

```yaml
Check:
  all:
    - name: QNAM
      operator: is_contained_by
      value: $parent_model_variables
Operations:
  - id: $parent_model_variables
    operator: get_parent_model_column_order
```

## global_max_variable_length

For `Global Value Check with Variable Metadata` and `Global Variable Metadata Check` rules, returns the largest declared length (`variable_length`) of each variable across all datasets. It gives the same result as a `max` of `variable_length` grouped by `variable_name`, without reading every value of every record.

```yaml
Rule Type: Global Value Check with Variable Metadata
Check:
  any:
    - name: $global_max_variable_length
      operator: not_equal_to
      value: $global_min_variable_length
Operations:
  - id: $global_max_variable_length
    operator: global_max_variable_length
  - id: $global_min_variable_length
    operator: global_min_variable_length
```

## global_min_variable_length

As [global_max_variable_length](#global_max_variable_length), but returns the smallest declared length of each variable across all datasets.

## intersect

Returns the values in `name` that are also in `subtract`, in the order they appear in `name`, without duplicates. `name` and `subtract` can each be:

- a column of the operation's `domain` (its non-null values)
- a list of values
- the id of a previous operation

Matching is case-sensitive unless `case_sensitive` is `false`. In that case values are compared, and returned, in uppercase.

Example: the library variables that are in the dataset, in library order

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

## label_referenced_variable_metadata

For each record, returns the values of the library ig variable attribute given in `attribute_name` (e.g. `role`) for every library variable whose label matches the value in the column provided in `name`. `attribute_name` is required.

```yaml
- id: $fatest_lib_roles
  operator: label_referenced_variable_metadata
  name: FATEST
  attribute_name: role
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

`filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in [record_count](#record_count).

## max_date

If no `group` is provided, returns the max date value in `name`. If `group` is provided, returns the max date value in `name`, within each unique set of the grouping variables. If no valid date is found, the result is empty.

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

`filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in [record_count](#record_count). The date is returned as `YYYY-MM-DD`, without any time part.

## max_variable_value_length

For `Value Check with Variable Metadata` rules, returns the length of the longest value of each variable, to compare with `variable_length`. In `Global Value Check with Variable Metadata` and `Global Variable Metadata Check` rules, it returns the longest value of each variable across all datasets. It gives the same result as a `max` of `variable_value_length` grouped by `variable_name` with `use_rule_type_table: true`, without reading every value of every record.

Example: flag variables whose declared length is longer than their longest value

```yaml
Rule Type: Value Check with Variable Metadata
Check:
  all:
    - name: variable_length
      operator: greater_than
      value: $max_variable_value_length
Operations:
  - id: $max_variable_value_length
    operator: max_variable_value_length
```

## mean

If no `group` is provided, returns the mean value in `name`. If `group` is provided, returns the mean value in `name`, within each unique set of the grouping variables.

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

`filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in [record_count](#record_count).

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

`filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in [record_count](#record_count).

## min_date

If no `group` is provided, returns the min date value in `name`. If `group` is provided, returns the min date value in `name`, within each unique set of the grouping variables. If no valid date is found, the result is empty.

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

`filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in [record_count](#record_count). The date is returned as `YYYY-MM-DD`, without any time part.

## minus

Returns the values in `name` that are not in `subtract`, in the order they appear in `name`, without duplicates. `name`, `subtract` and `case_sensitive` work as for [intersect](#intersect).

Example: flag FDA-desired TS parameters that are missing

```yaml
Check:
  all:
    - name: $missing_tsparmcds
      operator: non_empty
Operations:
  - id: $distinct_tsparmcds
    operator: distinct
    name: TSPARMCD
  - id: $missing_tsparmcds
    operator: minus
    name:
      - ADDON
      - AGEMAX
      - AGEMIN
    subtract: $distinct_tsparmcds
```

## name_referenced_variable_metadata

For each record, returns the values of the library ig variable attribute given in `attribute_name` (e.g. `role`) for every library variable whose name matches the value in the column provided in `name`. `attribute_name` is required.

```yaml
- id: $qnam_lib_roles
  operator: name_referenced_variable_metadata
  name: QNAM
  attribute_name: role
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

If no `filter` or `group` is provided, returns the number of records in the dataset. If `filter` is provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter. If `group` is provided, returns the number of rows matching each unique set of the grouping variables. These must be column names. If both `filter` and `group` are provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter that also match each unique set of the grouping variables.

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

`group` must list columns. These parameters are also supported:

- `regex`: only count records whose `name` value matches this PostgreSQL regular expression. It matches anywhere in the value unless anchored with `^` or `$`. Needs `name`.
- `use_rule_type_table: true`: count records in the rule type's table instead of the operation's `domain`, e.g. the `variable_name` and `variable_value_length` columns of `Value Check with Variable Metadata`.
- `name` set to the id of a previous operation that used `group`: count, or with `max`, `min`, `mean`, `max_date` and `min_date` aggregate, that operation's per-group values instead of a column. `group` and `filter` can then only use that operation's `group` columns. `max`, `min` and `mean` need numbers, e.g. a previous `record_count`. `max_date` and `min_date` need dates, e.g. a previous `max_date`.
- `ignore_empty_filtered_groups: true`: with `group` and a `filter` or `regex`, groups with no matching records are kept with a count of `0` by default. With this option they are left out when a later operation aggregates this result as above. The count each record gets doesn't change.

Example: flag TI records without TIVERS when an IETESTCD occurs more than once in TI

```yaml
Check:
  all:
    - name: TIVERS
      operator: empty
    - name: $max_count_ietestcd
      operator: greater_than
      value: 1
Operations:
  - id: $count_ietestcd
    operator: record_count
    domain: TI
    group:
      - IETESTCD
  - id: $max_count_ietestcd
    operator: max
    name: $count_ietestcd
```

## referenced_domain_variable_names

For each record, returns the variable names of the study dataset whose domain is the value of the column given in `name`, e.g. `RDOMAIN`. If no dataset in the study has that domain, the list is empty.

Example: flag IDVAR values that aren't variables of the RDOMAIN dataset

```yaml
Check:
  all:
    - name: RDOMAIN
      operator: exists
    - name: IDVAR
      operator: non_empty
    - name: IDVAR
      operator: is_not_contained_by
      value: $rdomain_variables
Operations:
  - id: $rdomain_variables
    operator: referenced_domain_variable_names
    name: RDOMAIN
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

## split

Splits comma-separated text into a list of values. Each part is trimmed, and empty parts are dropped. `name` is either:

- a column of the operation's `domain`: the result is the distinct parts of all its values, not one list per record. `filter` can be used to only split some records' values.
- the id of a previous operation: its value(s) are split.

For example, given the current dataset:

| id  | XXVAL |
| --- | ----- |
| 1   | A, B  |
| 2   | C     |
| 3   |       |

the following operation:

```yaml
- id: $parts
  operator: split
  name: XXVAL
```

returns `["A", "B", "C"]`.

## standard_domains

Returns the domains of the standard being validated, sorted alphabetically. If `domain_class` is given, only the domains of that class are returned. Write the class in uppercase, e.g. `TRIAL DESIGN`, `SPECIAL PURPOSE` or `FINDINGS ABOUT`.

Example: flag trial design domains of the IG that weren't submitted

```yaml
Check:
  all:
    - name: $missing_trial_design_domains
      operator: non_empty
Operations:
  - id: $trial_design_domains
    operator: standard_domains
    domain_class: TRIAL DESIGN
  - id: $dataset_names
    operator: dataset_names
  - id: $missing_trial_design_domains
    operator: minus
    name: $trial_design_domains
    subtract: $dataset_names
    case_sensitive: false
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

## variable_count

Given a variable `name`, returns the number of datasets in the study that contain that variable. `--` in `name` is replaced with each dataset's domain, so `--ENDTC` matches AEENDTC in AE, LBENDTC in LB, etc.

- Input

  Study datasets:

  ```json
  {
    "AE": ["STUDYID", "DOMAIN", "USUBJID", "AETERM", "AEENDTC"],
    "LB": ["STUDYID", "DOMAIN", "USUBJID", "LBTESTCD", "LBENDTC"]
  }
  ```

  Rule:

  ```yaml
  - operator: variable_count
    id: $endtc_count
    name: --ENDTC
  ```

- Output

  `2`

## variable_exists

Returns `true` if the variable given by `name` exists in the dataset being evaluated, otherwise `false`. `--` in `name` is replaced with the domain.

Example: flag an error if MIDS is in the dataset currently being evaluated and the TM domain is not present in the study

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

`name` is looked up in the operation's `domain`, which defaults to the dataset being evaluated, so `domain` can point it at another dataset.

```yaml
- id: $EXVAMTU_EXISTS
  operator: variable_exists
  domain: EX
  name: EXVAMTU
```

## variable_is_null

Returns `true` if every record of the variable given by `name` is null or the variable is not in the dataset being evaluated, otherwise `false`. Empty strings count as null. A dataset with no records returns `true`. `--` in `name` is replaced with the domain.

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

For Char variables, values containing only whitespace (spaces, tabs, newlines) also count as null. `name` is looked up in the operation's `domain`, which defaults to the dataset being evaluated. Set `use_rule_type_table: true` to look it up in the rule type's table instead (e.g. `define_variable_has_no_data` or `dataset_label`). If the domain or table doesn't exist, the rule is skipped.

# External Dictionary Validation Operations

**Supported External Dictionary Types**

```
MEDDRA = "meddra"
WHODRUG = "whodrug"
LOINC = "loinc"
MEDRT = "medrt"
UNII = "unii"
SNOMED = "snomed"
```

**Generic External Dictionary Operations**

## get_define_external_dictionary_version

Returns the version given in the define.xml for the external dictionary in `external_dictionary_type`, i.e. the `Version` of the external codelist whose `Dictionary` matches it, ignoring case. If the define.xml has no such dictionary, the rule fails with an execution error.

Example: flag a define.xml LOINC version later than the LOINC dictionary provided

```yaml
Check:
  all:
    - name: $define_loinc_version
      operator: greater_than
      value: $dict_loinc_version
Operations:
  - id: $define_loinc_version
    operator: get_define_external_dictionary_version
    external_dictionary_type: loinc
  - id: $dict_loinc_version
    operator: get_external_dictionary_version
    external_dictionary_type: loinc
```

## get_external_dictionary_version

Returns the version of the external dictionary in `external_dictionary_type`, as read from the dictionary files provided for the validation run. If that dictionary wasn't provided, the rule fails with an execution error. See [get_define_external_dictionary_version](#get_define_external_dictionary_version) for an example.

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

**Output:**

```json
[true, false, true, true]
```

If `--CLAS` or `--CLASCD` is `MULTIPLE`, the record is valid when the `--DECOD` drug name has more than one entry in WHODrug.
