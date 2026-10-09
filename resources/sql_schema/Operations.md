## define_variable_metadata

Returns the define.xml metadata attribute in `attribute_name` for the variable in `name` (required), within the operation's domain. The operation fails if the variable is not in the define.xml.

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

Returns a list of the submitted dataset filenames in all uppercase

ex. if TS.xpt, AE.xpt, EC.xpt, and SUPPEC.xpt are submitted -> [TS, AE, EC, SUPPEC] will be returned

## distinct

Get a distinct list of values for the given `name`. If a `group` list is specified, the distinct value list will be grouped by the variables within `group`.

`filter` keeps only the records matching every `{column: value}` pair. Unlike the `filter` of [record_count](#record_count) and the other aggregate operations, values are matched exactly: a trailing `%` is not a wildcard.

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
Variables come from the IG domain metadata only, so a custom domain (not in the IG) returns an empty list.

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

- `dataset_name`: the name of the dataset being validated (for a split dataset, of the part each record came from)
- `dataset_size`: the dataset's file size in bytes (for a split dataset, of the part each record came from)
- `label`, `description`, `datasetStructure`: the domain's metadata in the IG library. The operation fails if the domain has no such metadata (e.g. a custom domain).

Example: the IG domain label

- Input:

  Target domain: `LB`

  ```yaml
  - name: label
    operator: extract_metadata
    id: $domain_label
  ```

- Output:

  `Laboratory Test Results`

## get_codelist_attributes

Returns the values of a controlled terminology attribute (`ct_attribute`) for the CT version(s) in use.

- `ct_attribute`: one of `Term CCODE`, `Term Signification`, `Term`, `Codelist Code`, `Codelist Name`, `Extensible`, `Synonyms`, `Definition`, `Standard Type`, `Version Date`, `Standard and Date`. `Synonyms` values are returned as stored: one `;`-joined string per term.
- `ct_conditions` (optional): a list of `{attribute: value}` filters on the same attributes
- `version` (optional): a variable holding each record's CT version (e.g. `TSVCDVER`) or a literal version. A literal version is ignored when CT packages are provided to the engine (`-ct`). Without `version`, the provided CT packages are used, or else all loaded ones.

`name` is ignored.

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

If both `key_name` and `key_value` are provided, only the variables whose library metadata attribute `key_name` equals `key_value` are returned (e.g. `key_name: role`, `key_value: Timing`).

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

For each record, returns the IG library attribute in `attribute_name` (`role`, `core`, `label`, `name`, `ordinal` or `simpleDatatype`) of the variables whose label is the record's value in the column `name`. Variables of every dataset in the IG are matched, so the result is a collection: empty when no variable has that label, with several values when several do.

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
    id: $qlabel_variable_names
    name: QLABEL
    attribute_name: name
  ```

- Output

  ```json
  {
    "QLABEL": ["Toxicity", "Viscosity", "Analysis Method"],
    "$qlabel_variable_names": [
      ["LBTOX", "VSTOX"],
      [],
      ["CPANMETH", "GFANMETH", "LBANMETH", "MSANMETH", "PPANMETH"]
    ]
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

Values are cast to dates, so a partial date (e.g. `2023-01`) in `name` makes the operation fail; `regex: ^\d{4}-\d{2}-\d{2}` skips them. Empty values are ignored. The result is `YYYY-MM-DD` (any time part is dropped), or empty when there is no date.

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

If no `group` is provided, returns the min date value in `name`. If `group` is provided, returns the min date value in `name`, within each unique set of the grouping variables. Dates are handled as in [max_date](#max_date): a partial date makes the operation fail and the result is `YYYY-MM-DD`.

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

- `filter`: only aggregate the records matching every `{column: value}` pair; a value ending in `%` is a prefix match (see [record_count](#record_count))
- `regex`: only aggregate the values of `name` matching the regular expression (PostgreSQL syntax, matched anywhere in the value unless anchored). Ignored without `name`.
- `ignore_empty_filtered_groups: true`: only matters when this operation has `group` and `filter` and another operation aggregates over it as above: groups with no records matching the filter are left out instead of contributing an empty value (`0` for `record_count`). The operation's own per-record result is unchanged.
- `use_rule_type_table: true`: aggregate over the dataset built by the rule's `Rule Type` (e.g. the variable metadata table) instead of the domain's dataset

## name_referenced_variable_metadata

As [label_referenced_variable_metadata](#label_referenced_variable_metadata), but matches the IG variables whose name is the record's value in the column `name`.

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
    id: $qnam_variable_labels
    name: QNAM
    attribute_name: label
  ```

- Output

  ```json
  {
    "QNAM": ["LBTOX", "LBVISCOS", "LBANMETH"],
    "$qnam_variable_labels": [["Toxicity"], [], ["Analysis Method"]]
  }
  ```

## permissible_variables

Returns the permissible variables ("Core" = Perm ) for a given domain and standard
Variables come from the IG domain metadata only, so a custom domain (not in the IG) returns an empty list.

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

If no `filter` or `group` is provided, returns the number of records in the dataset. If `filter` is provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter. If `group` is provided, returns the number of rows matching each unique set of the grouping variables. These are column names. If both `filter` and `group` are provided, returns the number of records in the dataset that contain the value(s) in the corresponding column(s) provided in the filter that also match each unique set of the grouping variables. If `name` is provided, only the records where `name` is not null are counted.

**Wildcard Filtering**: Filter values ending with `%` will match any records where the column value starts with the specified prefix. For example, `RACE%` will match `RACE1`, `RACE2`, `RACE3`, etc. This is useful for matching related variables with numeric or alphabetic suffixes. The same applies to the `filter` of the other aggregate operations and of `split`, but not of `distinct`.

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
Variables come from the IG domain metadata only, so a custom domain (not in the IG) returns an empty list.

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

By default, the standard is as specified when running validation - as the validation runtime parameter and/or as specified in the rule header - and the list of terminology packages is obtained from the current cache. If required, the default standard may be overridden using the optional `ct_package_types` parameter, a list of `ADAM`, `ADAMIG`, `CDASHIG`, `SDTM`, `SDTMIG`, `SENDIG` or `USDM` (case-insensitive). Standards and package types map to terminology packages as follows:

- `SDTM`, `SDTMIG`: `sdtmct`
- `SENDIG`: `sendct`
- `CDASHIG`: `cdashct`
- `ADAM`, `ADAMIG`: `adamct` and `sdtmct`
- `USDM`: `ddfct` and `sdtmct`

For example, given the same list of terminology packages, the following operation:

```yaml
Operations:
  - operator: valid_codelist_dates
    id: $valid_dates
    ct_package_types:
      - SDTM
      - CDASHIG
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

A record is valid if the three values match a WHODrug entry (drug name, ATC level 4 text and ATC code), or if `--CLAS` or `--CLASCD` is `MULTIPLE` and `--DECOD` occurs more than once in the dictionary. The result is one collection of these booleans for the whole dataset, not a per-record value.

**Input:**

```yaml
Operations:
  - id: $valid_whodrug_codes
    operator: whodrug_code_hierarchy
```

## variable_count

Returns the number of study datasets that contain the variable in `name`. A `--` prefix (or the current domain's prefix) matches each dataset's own prefix, e.g. `--ENDTC` matches `AEENDTC` in AE and `LBENDTC` in LB.

- Input

  Study datasets:

  ```json
  {
    "AE": ["STUDYID", "DOMAIN", "USUBJID", "AETERM", "AEENDTC"],
    "LB": ["STUDYID", "DOMAIN", "USUBJID", "LBTESTCD", "LBENDTC"]
  }
  ```

  ```yaml
  - operator: variable_count
    id: $endtc_dataset_count
    name: --ENDTC
  ```

- Output

  `2`

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

Evaluates an arithmetic formula in `value`. The formula may use numbers, dataset variables and operation results, combined with `+`, `-`, `*`, `/` and parentheses. Non-numeric values evaluate to null, as does division by zero.

- Variables are looked up in the operation's `domain`. `--` names are not supported (`-` is read as minus).
- `$` references must be single-value (constant) operation results, such as `max` or `record_count`, not collections such as `distinct`.

```yaml
- id: $dose_per_kg
  operator: calc
  value: EXDOSE / $baseline_weight
```

## define_extended_terms_in_ct

Returns the extended codelist terms in the define.xml that already exist in the controlled terminology version in use, within the same codelist, as a duplicate submission value, a synonym, or part of a `;`-separated submission value. Each result identifies its codelist, e.g. `Ophthalmic Exam Test Code (C117743): INTP`. Optional parameters: `version` (as in [get_codelist_attributes](#get_codelist_attributes)) and `case_sensitive` (default true).

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

Returns the version of the external dictionary in `external_dictionary_type` loaded into the engine. `whodrug` is not supported.

```yaml
- id: $dict_loinc_version
  operator: get_external_dictionary_version
  external_dictionary_type: loinc
```

## intersect

Returns the values in `name` that are also in `subtract`, preserving the order of `name`. Each of `name` and `subtract` may be a variable, a list of literal values or an operation result. Comparison is case sensitive unless `case_sensitive: false`. See [minus](#minus) for details.

```yaml
- id: $library_with_dataset_only
  operator: intersect
  name: $column_order_from_library
  subtract: $column_order_from_dataset
```

## minus

Returns the values in `name` that are not in `subtract`, preserving the order of `name`. Each of `name` and `subtract` may be a variable, a list of literal values or an operation result. Comparison is case sensitive unless `case_sensitive: false`.

- Duplicates are removed. A variable contributes its non-null values across the whole dataset.
- `--` in `name` and `subtract` is not replaced with the domain.
- With `case_sensitive: false`, the values are returned in upper case.

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

Splits the values in `name` (a variable or operation result) on commas into a list of values, with surrounding whitespace removed. Empty values are skipped.

When `name` is a variable, the result is one list of the distinct parts across all records of the dataset, not a list per record; `filter` restricts the records used (a value ending in `%` is a prefix match). `filter` is ignored when `name` is an operation result.

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
