## calc

_SQL engine only._

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

## codelist_terms

Returns a list of valid codelist/term values. Used for evaluating whether NCI code or submission values are valid based on controlled terminology. Expects three parameters: `codelists` which is a list of the codelist submission value(s) to retrieve, `level` which is the level of data (either "codelist" or "term") at which to return data from, and `returntype` which is the type of values to return, either "code" for NCI Code(s) or "value" for submission value(s).

_Not supported by the SQL engine._ To get the terms of a codelist, use [get_codelist_attributes](#get_codelist_attributes) with `ct_conditions` instead.

```yaml
Check:
  all:
    - name: PPSTRESU
      operator: is_not_contained_by
      value: $terms
    - name: $extensible
      operator: equal_to
      value: true
Operations:
  - id: $terms
    operator: codelist_terms
    codelists:
      - PKUDUG
    level: term
    returntype: value
  - id: $extensible
    codelist: PKUDUG
    operator: codelist_extensible
```

If `ct_package_type`, `version`, `codelist_code` and one of `term_code` or `term_value` are provided, it will instead attach a new column containing the term for each combination provided in the source dataset. `version` is the name of the column holding each record's CT package version date, and `codelist_code` is either the name of a column holding the codelist code or a literal codelist code. If `term_code` is provided, it will find term values using the term codes. If `term_value` is provided, it will find term codes using the term values. Matching on `term_code`/`term_value` is case-insensitive, and records with no match get a null value. Only one of `term_code` or `term_value` can be provided.

For example, given the current dataset:

| id  | codeSystemVersion | $codelist_code | code    | decode     |
| --- | ----------------- | -------------- | ------- | ---------- |
| 1   | 2024-09-27        | C201264        | C201356 | After      |
| 2   | 2024-09-27        | C201265        | C201352 | End to End |
| 3   | 2023-03-29        | C127262        | C51282  | CLINIC     |

and the following operation:

```yaml
- id: $found_term_value
  operator: codelist_terms
  ct_package_type: DDF
  version: codeSystemVersion
  codelist_code: $codelist_code
  term_code: code
```

This will result in the following dataset:

| id  | codeSystemVersion | $codelist_code | code    | decode     | $found_term_value |
| --- | ----------------- | -------------- | ------- | ---------- | ----------------- |
| 1   | 2024-09-27        | C201264        | C201356 | After      | After             |
| 2   | 2024-09-27        | C201265        | C201352 | End to End | End to End        |
| 3   | 2023-03-29        | C127262        | C51282  | CLINIC     | CLINIC            |

Conversely, if given the same dataset, and the following operation:

```yaml
- id: $found_term_code
  operator: codelist_terms
  ct_package_type: DDF
  version: codeSystemVersion
  codelist_code: $codelist_code
  term_value: decode
```

This will result in the following dataset:

| id  | codeSystemVersion | $codelist_code | code    | decode     | $found_term_code |
| --- | ----------------- | -------------- | ------- | ---------- | ---------------- |
| 1   | 2024-09-27        | C201264        | C201356 | After      | C201356          |
| 2   | 2024-09-27        | C201265        | C201352 | End to End | C201352          |
| 3   | 2023-03-29        | C127262        | C51282  | CLINIC     | C51282           |

## codelist_extensible

Returns a Series indicating whether a specified `codelist` is extensible. Used in conjunction with `codelist_terms` to determine if values outside the codelist are acceptable. From the above example, `$extensible` will contain a bool if the codelist PKUDUG is extensible in all rows of the column.

_Not supported by the SQL engine._ Use the `is_extensible_codelist_code` check operator (see [Operator.md](Operator.md)) instead.

If `ct_package_type`, `version`, and `codelist_code` parameters are provided, it will instead attach a new column containing the extensible value for each combination provided in the source dataset. As for `codelist_terms`, `version` is the name of the column holding each record's CT package version date, and `codelist_code` is either the name of a column holding the codelist code or a literal codelist code.

For example, given the current dataset:

| id  | codeSystemVersion | $codelist_code |
| --- | ----------------- | -------------- |
| 1   | 2024-09-27        | C201264        |
| 2   | 2024-09-27        | C201265        |
| 3   | 2023-03-29        | C127262        |

and the following operation:

```yaml
- id: $codelist_extensible
  operator: codelist_extensible
  ct_package_type: DDF
  version: codeSystemVersion
  codelist_code: $codelist_code
```

This will result in the following dataset:

| id  | codeSystemVersion | $codelist_code | $codelist_extensible |
| --- | ----------------- | -------------- | -------------------- |
| 1   | 2024-09-27        | C201264        | false                |
| 2   | 2024-09-27        | C201265        | false                |
| 3   | 2023-03-29        | C127262        | true                 |

## define_extended_terms_in_ct

_SQL engine only._

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

## define_extensible_codelists

Returns a list of the sponsor-extended term submission values defined in the define.xml for extensible codelists. Used for evaluating whether submission values are valid based on controlled terminology. Expects the parameter `codelists` which is a list of the codelist submission value(s) to retrieve. If `codelists` is `["ALL"]`, it will return the extended terms of all extensible codelists in a single list.

_Not supported by the SQL engine._

```yaml
- id: $ext_value
  operator: define_extensible_codelists
  codelists:
    - ALL
```

## define_variable_metadata

If a target variable `name` is specified, returns the specified metadata in the define for the specified target variable. `--` in `name` is replaced with the domain. If the variable or attribute is not found in the define, an empty string is returned.

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

**SQL engine:** `name` is required: only the single-variable form is supported. If the domain or the variable isn't in the define, the rule fails with an execution error instead of returning an empty string.

## dataset_names

Returns a list of the submitted dataset filenames in all uppercase. The order of the names is not guaranteed.

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

**SQL engine:** `group_aliases` is ignored. `filter` can be used to only take values from records whose columns have the given values. Values must match exactly: the `%` wildcard of [record_count](#record_count) isn't supported.

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

**SQL engine:** if the dataset being evaluated already has an RFSTDTC column, e.g. after `Match Datasets` with DM as above, that column is used instead of joining DM again. Partial dates are compared at the precision they share: e.g. `2023-01` and `2023-01-05` are compared as months, which gives `1`.

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
      operator: is_not_ordered_by
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

**SQL engine:** `key_name` and `key_value` can be given to only return the variables whose library metadata attribute `key_name` equals `key_value`, as for [get_model_filtered_variables](#get_model_filtered_variables).

```yaml
- id: $library_timing_variables
  operator: get_column_order_from_library
  key_name: role
  key_value: Timing
```

## get_countries

_SQL engine only._

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

## get_dataset_filtered_variables

Filters variables from the dataset based on specified library metadata criteria (`key_name` and `key_value`). Returns a list of variable names, in library order, that exist in the dataset and whose library metadata attribute `key_name` equals `key_value`.

_Not supported by the SQL engine._

```yaml
- operator: get_dataset_filtered_variables
  id: $timing_variables
  key_name: "role"
  key_value: "Timing"
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

_SQL engine only._

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

_SQL engine only._

As [global_max_variable_length](#global_max_variable_length), but returns the smallest declared length of each variable across all datasets.

## intersect

_SQL engine only._

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

Generates a dataframe where each record in the dataframe is the library ig variable metadata corresponding with the variable label found in the column provided in `name`. A column is added for each library metadata attribute, named `<id>_<attribute>` (e.g. `$label_referenced_variable_metadata_role`). Records whose label does not match a library variable get empty values.

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
    "$label_referenced_variable_metadata_name": ["LBTOX", "", "LBANMETH"],
    "$label_referenced_variable_metadata_role": [
      "Variable Qualifier",
      "",
      "Record Qualifier"
    ],
    "$label_referenced_variable_metadata_ordinal": [44, "", 38],
    "$label_referenced_variable_metadata_label": [
      "Toxicity",
      "",
      "Analysis Method"
    ],
    "...": "..."
  }
  ```

**SQL engine:** `attribute_name` is required, and no columns are added. For each record, the operation returns the values of that one library attribute, e.g. `role`, for every library variable whose label matches the value in `name`.

```yaml
- id: $fatest_lib_roles
  operator: label_referenced_variable_metadata
  name: FATEST
  attribute_name: role
```

## map

Allows the creation of a lookup table to take the values from multiple input columns and map them to values in an output column. The `map` parameter contains a list of objects. Each dictionary contains column names as properties that match the column names in the source dataset and an `output` property that will be returned as a result.

If `map` has a single object and `output` is the only property specified on that object, this will function as a direct assignment.

_Not supported by the SQL engine._

For example, given the following current dataset:

| id  | parent_entity | parent_rel     |
| --- | ------------- | -------------- |
| 1   | Timing        | relativeToFrom |
| 2   | Something     | relativeToFrom |
| 3   | Timing        | type           |

and the following operation:

```yaml
Operations:
  - id: $codelist_code
    operator: map
    map:
      - parent_entity: Timing
        parent_rel: type
        output: C201264
      - parent_entity: Timing
        parent_rel: relativeToFrom
        output: C201265
```

This will result in the following dataset:

| id  | parent_entity | parent_rel     | $codelist_code |
| --- | ------------- | -------------- | -------------- |
| 1   | Timing        | relativeToFrom | C201265        |
| 2   | Something     | relativeToFrom | None           |
| 3   | Timing        | type           | C201264        |

The following operation:

```yaml
Operations:
  - id: $codelist_code
    operator: map
    map:
      - output: C201264
```

Will result in the following dataset:

| id  | parent_entity | parent_rel     | $codelist_code |
| --- | ------------- | -------------- | -------------- |
| 1   | Timing        | relativeToFrom | C201264        |
| 2   | Something     | relativeToFrom | C201264        |
| 3   | Timing        | type           | C201264        |

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

**SQL engine:** `filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in the SQL engine notes of [record_count](#record_count).

## max_date

If no `group` is provided, returns the max date value in `name`. If `group` is provided, returns the max date value in `name`, within each unique set of the grouping variables. The date is returned as an ISO 8601 string; if no valid date is found, the result is empty.

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

**SQL engine:** `filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in the SQL engine notes of [record_count](#record_count). The date is returned as `YYYY-MM-DD`, without any time part.

## max_variable_value_length

_SQL engine only._

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

**SQL engine:** `filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in the SQL engine notes of [record_count](#record_count).

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

**SQL engine:** `filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in the SQL engine notes of [record_count](#record_count).

## min_date

If no `group` is provided, returns the min date value in `name`. If `group` is provided, returns the min date value in `name`, within each unique set of the grouping variables. The date is returned as an ISO 8601 string; if no valid date is found, the result is empty.

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

**SQL engine:** `filter` is also supported, as for [record_count](#record_count), and so are `regex`, `use_rule_type_table` and the id of a previous grouped operation in `name`, described in the SQL engine notes of [record_count](#record_count). The date is returned as `YYYY-MM-DD`, without any time part.

## minus

_SQL engine only._

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

Generates a dataframe where each record in the dataframe is the library ig variable metadata corresponding with the variable name found in the column provided in `name`. A column is added for each library metadata attribute, named `<id>_<attribute>` (e.g. `$name_referenced_variable_metadata_role`). Records whose name does not match a library variable get empty values.

- Input

  Target Dataset: `SUPPLB`

  Product: `sdtmig`

  Version: `3-4`

  Dataset:

  ```json
  {
    "STUDYID": ["STUDY1", "STUDY1", "STUDY1"],
    "USUBJID": ["SUBJ1", "SUBJ1", "SUBJ1"],
    "QNAM": ["LBTOX", "LBVISCOS", "LBANMETH"]
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
    "$name_referenced_variable_metadata_name": ["LBTOX", "", "LBANMETH"],
    "$name_referenced_variable_metadata_role": [
      "Variable Qualifier",
      "",
      "Record Qualifier"
    ],
    "$name_referenced_variable_metadata_ordinal": [44, "", 38],
    "$name_referenced_variable_metadata_label": [
      "Toxicity",
      "",
      "Analysis Method"
    ],
    "...": "..."
  }
  ```

**SQL engine:** `attribute_name` is required, and no columns are added. For each record, the operation returns the values of that one library attribute, e.g. `role`, for every library variable whose name matches the value in `name`.

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

If no `filter` or `group` is provided, returns the number of records in the dataset. If `filter` is provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter. If `group` is provided, returns the number of rows matching each unique set of the grouping variables. These can be static column name(s) or can be derived from other operations like `get_dataset_filtered_variables`. If both `filter` and `group` are provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter that also match each unique set of the grouping variables.

**Wildcard Filtering**: Filter values ending with `%` will match any records where the column value starts with the specified prefix. For example, `RACE%` will match `RACE1`, `RACE2`, `RACE3`, etc. This is useful for matching related variables with numeric or alphabetic suffixes.

If `group` is provided, `group_aliases` may also be provided to assign new grouping variable names so that results grouped by the values in one set of grouping variables can be merged onto a dataset according to the same grouping value(s) stored in different set of grouping variables. When both `group` and `group_aliases` are provided, columns are renamed according to corresponding list position (i.e., the 1st column in `group` is renamed to the 1st column in `group_aliases`, etc.). If there are more columns listed in `group` than in `group_aliases`, only the `group` columns with corresponding `group_aliases` columns will be renamed. If there are more columns listed in `group_aliases` than in `group`, the extra column names in `group_aliases` will be ignored.

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

Example: return the number of records grouped by USUBJID and IDVARVAL where QNAM = "TEST1" and IDVAR = "GROUPID", renaming the IDVARVAL column to GROUPID for subsequent merging.

```yaml
- operator: record_count
  id: $test1_records_per_usubjid_groupid
  group:
    - USUBJID
    - IDVARVAL
  filter:
    QNAM: "TEST1"
    IDVAR: "GROUPID"
  group_aliases:
    - USUBJID
    - GROUPID
```

Example: Group the `StudyIdentifier` dataset by `parent_id` and merge the result back to the context dataset `StudyVersion` using `StudyVersion.id == StudyIdentifier.parent_id`

```yaml
Scope:
  Entities:
    Include:
      - StudyVersion
Operations:
  - domain: StudyIdentifier
    filter:
      parent_entity: "StudyVersion"
      parent_rel: "studyIdentifiers"
      rel_type: "definition"
      studyIdentifierScope.organizationType.code: "C70793"
      studyIdentifierScope.organizationType.codeSystem: "http://www.cdisc.org"
    group:
      - parent_id
    group_aliases:
      - id
    id: $num_sponsor_ids
    operator: record_count
```

**SQL engine:** `group_aliases` is ignored, and `group` must list columns: grouping by another operation's result (e.g. `get_dataset_filtered_variables`) isn't supported. These extra parameters are supported:

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

_SQL engine only._

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

_SQL engine only._

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

_SQL engine only._

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

**SQL engine:** `name` is looked up in the operation's `domain`, which defaults to the dataset being evaluated, so `domain` can point it at another dataset.

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

**SQL engine:** for Char variables, values containing only whitespace (spaces, tabs, newlines) also count as null. `name` is looked up in the operation's `domain`, which defaults to the dataset being evaluated. Set `use_rule_type_table: true` to look it up in the rule type's table instead (e.g. `define_variable_has_no_data` or `dataset_label`). If the domain or table doesn't exist, the rule is skipped.

## variable_library_metadata

Get the metadata value from the library for all variables defined in the library for the current domain. Metadata attribute is specified by the `name`. Returns a mapping of variable names to the metadata value.

_Not supported by the SQL engine._

Result

```json
{
  "STUDYID": "Req",
  "DOMAIN": "Req",
  "AEGRPID": "Perm",
  "AETERM": "Req",
  "AELLT": "Exp",
  "...": "..."
}
```

> Condition: Variable Core Status = Required

> Rule: Variable ^= null

```yaml
Check:
  any:
    - all:
        - operator: variable_metadata_equal_to
          value: Req
          metadata: $var_perm
        - operator: empty
Operations:
  - id: $var_perm
    operator: variable_library_metadata
    name: core
```

## variable_names

Return the set of variable names from the library for the given standard

_Not supported by the SQL engine._

## variable_value_count

Given a variable `name`, returns a mapping of variable values to the number of domains in the study in which that value appears in the variable. `--` in `name` is replaced with each domain code. Each value is counted at most once per domain.

_Not supported by the SQL engine._

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

_SQL engine only._

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

_SQL engine only._

Returns the version of the external dictionary in `external_dictionary_type`, as read from the dictionary files provided for the validation run. If that dictionary wasn't provided, the rule fails with an execution error. See [get_define_external_dictionary_version](#get_define_external_dictionary_version) for an example.

## valid_define_external_dictionary_version

Returns true if the version of an external dictionary provided in the define.xml file matches
the version parsed from the dictionary files.

_Not supported by the SQL engine._ Compare [get_define_external_dictionary_version](#get_define_external_dictionary_version) with [get_external_dictionary_version](#get_external_dictionary_version) instead.

Input:

```yaml
Operations:
  - operator: valid_define_external_dictionary_version
    id: $is_valid_loinc_version
    external_dictionary_type: loinc
```

Output:

```json
[true, true, true, true]
```

## valid_external_dictionary_value

Returns true if the target variable contains a valid external dictionary value, otherwise false.

Can be case insensitive by setting `case_sensitive` attribute to false. It is true by default.

_Not supported by the SQL engine._ Use the `is_valid_<dictionary>_term_reference` check operators (see [Operator.md](Operator.md)) instead.

Input:

```yaml
Operations:
  - operator: valid_external_dictionary_value
    name: --DECOD
    id: $is_valid_decod_value
    external_dictionary_type: meddra
    dictionary_term_type: PT
    case_sensitive: false
```

Output:

```json
[true, false, false, true]
```

## valid_external_dictionary_code

Returns true if the target variable contains a valid external dictionary code, otherwise false.

_Not supported by the SQL engine._ Use the `is_valid_<dictionary>_code_reference` check operators (see [Operator.md](Operator.md)) instead.

Input:

```yaml
Operations:
  - operator: valid_external_dictionary_code
    name: --COD
    id: $is_valid_cod_code
    external_dictionary_type: meddra
    dictionary_term_type: PT
```

Output:

```json
[true, false, false, true]
```

## valid_external_dictionary_code_term_pair

Returns true if the row in the dataset contains a matching pair of code and term, otherwise false.

For this operator, the `name` parameter should contain the name of the variable containing the code, and the
`external_dictionary_term_variable` parameter should contain the name of the variable containing the term.

_Not supported by the SQL engine._ Use the `is_valid_<dictionary>_code_term_pair` check operators (see [Operator.md](Operator.md)) instead.

Input:

```yaml
Operations:
  - operator: valid_external_dictionary_code_term_pair
    name: --COD
    id: $is_valid_loinc_code_term_pair
    external_dictionary_type: loinc
    external_dictionary_term_variable: --DECOD
```

Output:

```json
[true, false, false, true]
```

**MedDRA-Specific Operations**

## valid_meddra_code_references

Determines whether the values are valid in the following variables:

- `--SOCCD` (System Organ Class Code)
- `--HLGTCD` (High Level Group Term Code)
- `--HLTCD` (High Level Term Code)
- `--PTCD` (Preferred Term Code)
- `--LLTCD` (Lowest Level Term Code)

_Not supported by the SQL engine._ Use the `is_valid_meddra_code_reference` check operator (see [Operator.md](Operator.md)) on each variable instead.

**Input:**

```yaml
Operations:
  - id: $is_valid_meddra_codes
    operator: valid_meddra_code_references
```

**Output:**

```json
[true, false, true, true]
```

## valid_meddra_code_term_pairs

Determines whether the values are valid in the following variable pairs:

- `--SOCCD`, `--SOC` (System Organ Class Code and Term)
- `--HLGTCD`, `--HLGT` (High Level Group Term Code and Term)
- `--HLTCD`, `--HLT` (High Level Term Code and Term)
- `--PTCD`, `--DECOD` (Preferred Term Code and Dictionary-Derived Term)
- `--LLTCD`, `--LLT` (Lowest Level Term Code and Term)

_Not supported by the SQL engine._ Use the `is_valid_meddra_code_term_pair` check operator (see [Operator.md](Operator.md)) on each pair instead.

**Input:**

```yaml
Operations:
  - id: $is_valid_meddra_pairs
    operator: valid_meddra_code_term_pairs
```

**Output:**

```json
[true, true, false, true, true]
```

## valid_meddra_term_references

Determines whether the values are valid in the following variables:

- `--SOC` (System Organ Class)
- `--HLGT` (High Level Group Term)
- `--HLT` (High Level Term)
- `--DECOD` (Dictionary-Derived Term)
- `--LLT` (Lowest Level Term)

_Not supported by the SQL engine._ Use the `is_valid_meddra_term_reference` check operator (see [Operator.md](Operator.md)) on each variable instead.

**Input:**

```yaml
Operations:
  - id: $is_valid_meddra_terms
    operator: valid_meddra_term_references
```

**Output:**

```json
[true, true, false, true, true]
```

**WHODrug-Specific Operations**

## valid_whodrug_references

Checks if a reference to whodrug term in `name` points to the existing code in Atc Text (INA) file.

_Not supported by the SQL engine._ The `is_valid_whodrug_code_reference` check operator (see [Operator.md](Operator.md)) checks codes against the WHODrug ATC level 4 codes instead.

**Input:**

```yaml
Operations:
  - id: $whodrug_refs_valid
    operator: valid_whodrug_references
    name: --CLASCD
```

**Output:**

```json
[true, false, true, true]
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

**Output:**

```json
[true, false, true, true]
```

**SQL engine:** if `--CLAS` or `--CLASCD` is `MULTIPLE`, the record is valid when the `--DECOD` drug name has more than one entry in WHODrug.
