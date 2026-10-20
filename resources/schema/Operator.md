# Relational

## equal_to

Value comparison. Works for both string and number. True if the value in `name` is equal to `value`. If `value` matches the name of a column in the dataset, the value of that column in the same record is used for the comparison. Returns False when either value is null or empty.
Has optional parameters:

- 'value_is_literal' when true, `value` is always treated as a literal, even if a column with that name exists in the dataset. Set it whenever `value` is a literal string: the SQL engine treats a string `value` as a column name unless this is set, and returns False if no such column exists.
- 'value_is_reference' when true, the value parameter specifies a column name whose content determines which column to compare against dynamically.
- 'type_insensitive' when true, both values are converted to strings before comparison to handle type mismatches between string and numeric data.

> --OCCUR = N

```yaml
- name: --OCCUR
  operator: equal_to
  value: "N"
```

> IDVARVAL = the column specified in the IDVAR column for each row (type insensitive comparison).

```yaml
- name: IDVARVAL
  operator: equal_to
  value: "IDVAR"
  value_is_reference: true
  type_insensitive: true
```

> EXDOSE EQ 0

```yaml
- name: EXDOSE
  operator: equal_to
  value: 0
```

## not_equal_to

Complement of `equal_to`. Also has the optional parameters 'value_is_literal', 'value_is_reference' and 'type_insensitive'. Note that, like `equal_to`, it returns False when both values are null or empty.

> --OCCUR ^= Y

```yaml
- name: --OCCUR
  operator: not_equal_to
  value: "Y"
```

## equal_to_case_insensitive

Case insensitive `equal_to`. Supports the optional parameter 'value_is_literal', but not 'value_is_reference' or 'type_insensitive'.

> DSTERM is "Informed consent obtained"

```yaml
- name: DSTERM
  operator: equal_to_case_insensitive
  value: Informed consent obtained
```

## not_equal_to_case_insensitive

Complement of `equal_to_case_insensitive`

## greater_than

Value comparison

> TSVAL > 0

```yaml
- name: TSVAL
  operator: greater_than
  value: 0
```

## greater_than_or_equal_to

Value comparison

> TSVAL >= 0

```yaml
- name: TSVAL
  operator: greater_than_or_equal_to
  value: 0
```

## less_than

Value comparison

> TSVAL < 1

```yaml
- name: TSVAL
  operator: less_than
  value: 1
```

## less_than_or_equal_to

Value comparison

> TSVAL <= 1

```yaml
- name: TSVAL
  operator: less_than_or_equal_to
  value: 1
```

## empty

Value presence

> --OCCUR = null

```yaml
- name: --OCCUR
  operator: empty
```

## non_empty

Complement of `empty`

> --OCCUR ^= null

```yaml
- name: --OCCUR
  operator: non_empty
```

# String

## equals_string_part

True if the value in `name` is equal to the part of `value` extracted by the regular expression in `regex` (the capture group, if the regex has one). `value` can be a variable name or a literal.

> RDOMAIN equals characters 5 and 6 of SUPP dataset name

```yaml
- name: RDOMAIN
  operator: equals_string_part
  value: dataset_name
  regex: ".{4}(..).*"
```

## does_not_equal_string_part

Complement of `equals_string_part`

## matches_regex

Regular Expression value matching

- Determine if each string starts with a match of a regular expression. Refer to this pandas documentation: https://pandas.pydata.org/docs/reference/api/pandas.Series.str.match.html
- To "search" for a regex within the entire text, prefix the regex with `.*` and do not use anchors `^` , `$`
- To do a "fullmatch" of a regex with the entire text, suffix the regex with an anchor `$` and do not prefix the regex with `.*`
- For syntax guide, refer to this Python documentation: [Regular Expression HOWTO](https://docs.python.org/3/howto/regex.html).
- Suggestion for an online regular expression tester: https://regex101.com, choose the Python dialect.
- For regex token visualization, try https://www.debuggex.com.

> --DOSTXT value is numeric

```yaml
- name: --DOSTXT
  operator: matches_regex
  value: ^\d*\.?\d*$
```

## not_matches_regex

Complement of `matches_regex`

> --TESTCD is not (<= 8 chars, containing only letters, numbers, and underscores, and not starting with a number)

```yaml
- name: --TESTCD
  operator: not_matches_regex
  value: ^[A-Z_][A-Z0-9_]{0,7}$
```

## prefix_matches_regex

True if the `prefix` number of characters beginning a string in `name` match a regular expression in `value`

```yaml
- name: DOMAIN
  operator: prefix_matches_regex
  prefix: 2
  value: (AP|ap)
```

## not_prefix_matches_regex

Complement of `prefix_matches_regex`

## suffix_matches_regex

True if the `suffix` number of characters ending a string in `name` match a regular expression in `value`

> QNAM ends with numbers

```yaml
- name: "QNAM"
  operator: "suffix_matches_regex"
  suffix: 2
  value: '\d\d'
```

## not_suffix_matches_regex

Complement of `suffix_matches_regex`

> QNAM does not end with numbers

```yaml
- name: "QNAM"
  operator: "not_suffix_matches_regex"
  suffix: 2
  value: '\d\d'
```

## starts_with

Substring matching. True if the value in `name` starts with the string in `value` (case sensitive).

> DOMAIN beginning with 'AP'

```yaml
- name: "DOMAIN"
  operator: "starts_with"
  value: "AP"
```

## ends_with

Substring matching. True if the value in `name` ends with the string in `value` (case sensitive).

> DOMAIN ending with 'FOOBAR'

```yaml
- name: "DOMAIN"
  operator: "ends_with"
  value: "FOOBAR"
```

## prefix_equal_to

True if the `prefix` number of characters beginning a string in `name` match the string in `value`

```yaml
- name: dataset_name
  operator: prefix_equal_to
  prefix: 2
  value: DOMAIN
```

## prefix_not_equal_to

Complement of `prefix_equal_to`

## suffix_equal_to

True if the `suffix` number of characters ending a string in `name` match the string in `value`

```yaml
- name: dataset_name
  operator: suffix_equal_to
  suffix: 2
  value: DOMAIN
```

## suffix_not_equal_to

Complement of `suffix_equal_to`

## contains

True if the value in `value` is a substring of the value in `name`

> --TOXGR contains 'GRADE'

```yaml
- name: "--TOXGR"
  operator: "contains"
  value: "GRADE"
```

## does_not_contain

Complement of `contains`

## contains_case_insensitive

True if the value in `value` is a case insensitive substring of the value in `name`

> --TOXGR contains 'GRADE', regardless of text case

```yaml
- name: "--TOXGR"
  operator: "contains_case_insensitive"
  value: "grade"
```

## does_not_contain_case_insensitive

Complement of `contains_case_insensitive`

> --TOXGR does not contain 'GRADE', regardless of text case

```yaml
- name: "--TOXGR"
  operator: "does_not_contain_case_insensitive"
  value: "grade"
```

## longer_than

Length comparison. True if the length of the value in `name` is greater than `value`. If `value` is the name of a non-integer variable, the lengths of the values in the two variables are compared instead.

> SETCD value length > 8

```yaml
- name: "SETCD"
  operator: "longer_than"
  value: 8
```

## longer_than_or_equal_to

Length comparison. True if the length of the value in `name` is greater than or equal to `value`. If `value` is the name of a non-integer variable, the lengths of the values in the two variables are compared instead.

> TSVAL value length >= 201

```yaml
- name: "TSVAL"
  operator: "longer_than_or_equal_to"
  value: 201
```

## shorter_than

Length comparison. Complement of `longer_than_or_equal_to`: true if the length of the value in `name` is less than `value`.

> SETCD value length < 9

```yaml
- name: "SETCD"
  operator: "shorter_than"
  value: 9
```

## shorter_than_or_equal_to

Length comparison. Complement of `longer_than`: true if the length of the value in `name` is less than or equal to `value`.

> TSVAL value length <= 200

```yaml
- name: "TSVAL"
  operator: "shorter_than_or_equal_to"
  value: 200
```

## has_equal_length

Length comparison. True if the length of the value in `name` is equal to the length of the value in the variable `value`. If `value` is an integer, or an integer variable, the length is compared to that number instead.

> SEENDTC value length = SESTDTC value length

```yaml
- name: SEENDTC
  operator: has_equal_length
  value: SESTDTC
```

## has_not_equal_length

Complement of `has_equal_length`

# Date

## date_equal_to

Date comparison. Compare `name` to `value`. Compares partial dates if `date_component` is specified.

## date_not_equal_to

Complement of `date_equal_to`

Date comparison. Compare `name` to `value`. Compares partial dates if `date_component` is specified.

## date_greater_than

Date comparison. Compare `name` to `value`. Compares partial dates if `date_component` is specified.

> Year part of BRTHDTC > 2021

```yaml
- name: "BRTHDTC"
  operator: "date_greater_than"
  date_component: "year"
  value: "2021"
```

## date_greater_than_or_equal_to

Date comparison. Compare `name` to `value`. Compares partial dates if `date_component` is specified.

> Year part of BRTHDTC >= 2021

```yaml
- name: "BRTHDTC"
  operator: "date_greater_than_or_equal_to"
  date_component: "year"
  value: "2021"
```

## date_less_than

Date comparison. Compare `name` to `value`. Compares partial dates if `date_component` is specified.

> AEENDTC < AESTDTC

```yaml
- name: "AEENDTC"
  operator: "date_less_than"
  value: "AESTDTC"
```

> SSDTC < all DS.DSSTDTC when SSSTRESC = "DEAD"

```yaml
Check:
  all:
    - name: "SSSTRESC"
      operator: "equal_to"
      value: "DEAD"
    - name: "SSDTC"
      operator: "date_less_than"
      value: "$max_ds_dsstdtc"
Operations:
  - operator: "max_date"
    domain: "DS"
    name: "DSSTDTC"
    id: "$max_ds_dsstdtc"
```

## date_less_than_or_equal_to

Date comparison. Compare `name` to `value`. Compares partial dates if `date_component` is specified.

> AEENDTC <= AESTDTC

```yaml
- name: "AEENDTC"
  operator: "date_less_than_or_equal_to"
  value: "AESTDTC"
```

## is_complete_date

Date check. True if the value in `name` is a complete ISO 8601 date, i.e. it contains at least year, month and day (e.g. `2021-03-15`); the time part is optional.

> DM.RFSTDTC = complete date

```yaml
- name: "RFSTDTC"
  operator: "is_complete_date"
```

## is_incomplete_date

Complement of `is_complete_date`

Date check

> DM.RFSTDTC ^= complete date

```yaml
- name: "RFSTDTC"
  operator: "is_incomplete_date"
```

## invalid_date

Date check

> BRTHDTC is invalid

```yaml
- name: "BRTHDTC"
  operator: "invalid_date"
```

## invalid_duration

Duration ISO-8601 check, returns True if a duration is not in ISO-8601 format. The negative parameter must be specified to indicate if negative durations are either allowed (True) or disallowed (False)

> DURVAR is invalid (negative durations disallowed)

```yaml
- name: "DURVAR"
  operator: "invalid_duration"
  negative: False
```

# Metadata

## exists

True if the column exists in the current dataframe. (Works for datasets and variables)

> --OCCUR is present in dataset

```yaml
- name: "--OCCUR"
  operator: "exists"
```

> Domain SJ exists

```yaml
Rule Type: Domain Presence Check
Check:
  all:
    - name: "SJ"
      operator: "exists"
```

## not_exists

Complement of `exists`

> AEOCCUR not present in dataset

```yaml
- name: "AEOCCUR"
  operator: "not_exists"
```

> Domain SJ does not exist

```yaml
Rule Type: Domain Presence Check
Check:
  all:
    - name: "SJ"
      operator: "not_exists"
```

## inconsistent_enumerated_columns

Checks for inconsistencies in enumerated columns of a DataFrame. Starting with the smallest/largest enumeration of the given variable, returns True if VARIABLE(N+1) is populated but VARIABLE(N) is not populated. Repeats for all variables belonging to the enumeration. Note that the initial variable will not have an index (VARIABLE) and the next enumerated variable has index 1 (VARIABLE1).

ex: Check if there are inconsistencies in the TSVAL columns (TSVAL, TSVAL1, TSVAL2, etc.)

```yaml
Check:
  all:
    - name: "TSVAL"
      operator: "inconsistent_enumerated_columns"
```

## variable_metadata_equal_to

True if the metadata value of the variable `name`, looked up in the dictionary held by `metadata` (a dictionary keyed by variable name, typically produced by an operation), is equal to `value`. When `name` is omitted, the condition is evaluated for each of the rule's target variables.

Could be useful, for example, in checking variable permissibility in conjunction with the `variable_library_metadata` operation:

```yaml
Check:
  all:
    - operator: variable_metadata_equal_to
      value: Exp
      metadata: $permissibility
    - operator: not_exists
Operations:
  - id: $permissibility
    operator: variable_library_metadata
    name: core
```

## variable_metadata_not_equal_to

Complement of `variable_metadata_equal_to`

# Relationship & Set

## is_contained_by

True if the value in `name` is one of the values in the list in `value`. The list can have literal values or be a reference to a `$variable`.

> ACTARM in ('Screen Failure', 'Not Assigned', 'Not Treated', 'Unplanned Treatment')

```yaml
- name: "ACTARM"
  operator: "is_contained_by"
  value:
    - "Screen Failure"
    - "Not Assigned"
    - "Not Treated"
    - "Unplanned Treatment"
```

## is_not_contained_by

Complement of `is_contained_by`

> ARM not in ('Screen Failure', 'Not Assigned')

```yaml
- name: "ARM"
  operator: "is_not_contained_by"
  value:
    - "Screen Failure"
    - "Not Assigned"
```

## is_contained_by_case_insensitive

Value in `name` case insensitive compared against a list in `value`. The list can have literal values or be a reference to a `$variable`.

> ACTARM in ('Screen Failure', 'Not Assigned', 'Not Treated', 'Unplanned Treatment'), regardless of text case

```yaml
- name: "ACTARM"
  operator: "is_contained_by_case_insensitive"
  value:
    - "Screen Failure"
    - "Not Assigned"
    - "Not Treated"
    - "Unplanned Treatment"
```

## is_not_contained_by_case_insensitive

Complement of `is_contained_by_case_insensitive`

> ARM not in ('Screen Failure', 'Not Assigned'), regardless of text case

```yaml
- name: "ARM"
  operator: "is_not_contained_by_case_insensitive"
  value:
    - "Screen Failure"
    - "Not Assigned"
```

## prefix_is_contained_by

True if the `prefix` number of characters beginning a string in `name` match one of the strings in the list in `value`

> Check if a variable's domain identifier exists in the study

```yaml
- name: variable_name
  operator: prefix_is_contained_by
  prefix: 2
  value: $study_domains
```

## prefix_is_not_contained_by

Complement of `prefix_is_contained_by`

## suffix_is_contained_by

True if the `suffix` number of characters ending a string in `name` match one of the strings in the list in `value`

> Check if a supp's parent domain exists in the study

```yaml
- name: dataset_name
  operator: suffix_is_contained_by
  suffix: 2
  value: $study_domains
```

## suffix_is_not_contained_by

Complement of `suffix_is_contained_by`

## contains_all

True if all values in `value` are contained within the variable `name`. The check is made across the whole dataset (each value must be present in at least one record), so the result is the same for every record.

> All of ('Screen Failure', 'Not Assigned', 'Not Treated', 'Unplanned Treatment') in ACTARM

```yaml
- name: "ACTARM"
  operator: "contains_all"
  value:
    - "Screen Failure"
    - "Not Assigned"
    - "Not Treated"
    - "Unplanned Treatment"
```

## not_contains_all

Complement of `contains_all`

> Not all of ('Screen Failure', 'Not Assigned', 'Not Treated', 'Unplanned Treatment') in ACTARM

```yaml
- name: "ACTARM"
  operator: "not_contains_all"
  value:
    - "Screen Failure"
    - "Not Assigned"
    - "Not Treated"
    - "Unplanned Treatment"
```

## is_inconsistent_across_dataset

Checks if a variable maintains consistent values within groups defined by one or more grouping variables. Groups records by specified value(s) and validates that the target variable maintains the same value within each unique combination of grouping variables.

Single grouping variable - true if the values of BGSTRESU differ within USUBJID:

```yaml
- name: "BGSTRESU"
  operator: is_inconsistent_across_dataset
  value: "USUBJID"
```

Multiple grouping variables - true if the values of --STRESU differ within each combination of --TESTCD, --CAT, --SCAT, --SPEC, and --METHOD:

```yaml
- name: "--STRESU"
  operator: is_inconsistent_across_dataset
  value:
    - "--TESTCD"
    - "--CAT"
    - "--SCAT"
    - "--SPEC"
    - "--METHOD"
```

## is_unique_set

Relationship Integrity Check. True if the combination of values in `name` and the variable(s) listed in `value` occurs only once in the dataset. Null values are treated as equal to each other.

> --SEQ is unique within DOMAIN, USUBJID, and --TESTCD

```yaml
- name: "--SEQ"
  operator: is_unique_set
  value:
    - "DOMAIN"
    - "USUBJID"
    - "--TESTCD"
```

> `name` can be a variable containing a list of columns and `value` does not need to be present

```yaml
Rule Type: Dataset Contents Check against Define XML
Check:
  all:
    - name: define_dataset_key_sequence # contains list of dataset key columns
      operator: is_unique_set
```

## is_not_unique_set

Complement of `is_unique_set`

> --SEQ is not unique within DOMAIN, USUBJID, and --TESTCD

```yaml
- name: "--SEQ"
  operator: is_not_unique_set
  value:
    - "DOMAIN"
    - "USUBJID"
    - "--TESTCD"
```

> `name` can be a variable containing a list of columns and `value` does not need to be present

```yaml
Rule Type: Dataset Contents Check against Define XML
Check:
  all:
    - name: define_dataset_key_sequence # contains list of dataset key columns
      operator: is_not_unique_set
```

## present_on_multiple_rows_within

True if the same value of `name` is present on multiple rows, grouped by `within`. A maximum allowed number of occurrences can be specified in the value attribute. In this instance the value: 4 means that an error will be flagged if the same value appears more than 4 times within a USUBJID. By default the operator will flag any time a value appears more than once.

```yaml
- operator: "present_on_multiple_rows_within"
  name: "RELID"
  value: 4 # optional
  within: "USUBJID"
```

## not_present_on_multiple_rows_within

Complement of `present_on_multiple_rows_within`

```yaml
- operator: "not_present_on_multiple_rows_within"
  name: "RELID"
  value: 4 # optional
  within: "USUBJID"
```

## is_unique_relationship

Relationship Integrity Check. True if the values in `name` and `value` have a one-to-one relationship: each value of `name` always corresponds to the same value of `value`, and vice versa. Records whose values break the relationship are flagged by `is_not_unique_relationship`.

> AETERM and AEDECOD has a 1-to-1 relationship

```yaml
- name: AETERM
  operator: is_unique_relationship
  value: AEDECOD
```

## is_not_unique_relationship

Complement of `is_unique_relationship`

## empty_within_except_last_row

> SEENDTC is not empty when it is not the last record, grouped by USUBJID, sorted by SESTDTC

```yaml
- name: SEENDTC
  operator: empty_within_except_last_row
  ordering: SESTDTC
  value: USUBJID
```

## non_empty_within_except_last_row

Complement of `empty_within_except_last_row`

## has_next_corresponding_record

Ensures that a value of a variable `name` in one record is equal to the value of another variable `value` in the next corresponding record. The rows are grouped by `within` and ordered by `ordering`.

> SEENDTC is equal to the SESTDTC of the next record within a USUBJID. Ordered by SESEQ

```yaml
- name: SEENDTC
  operator: has_next_corresponding_record
  value: SESTDTC
  within: USUBJID
  ordering: SESEQ
```

## does_not_have_next_corresponding_record

Complement of `has_next_corresponding_record`

## is_ordered_set

True if the dataset rows are in ascending order of the values within `name`, grouped by the values within `value`

```yaml
Check:
  all:
    - name: --SEQ
      operator: is_ordered_set
      value: USUBJID
```

## is_not_ordered_set

Complement of `is_ordered_set`

```yaml
Check:
  all:
    - name: --SEQ
      operator: is_not_ordered_set
      value: USUBJID
```

## is_ordered_by

True if the dataset rows are ordered by the values within `name`, given the ordering specified by `order` (`asc` or `dsc`)

```yaml
Check:
  all:
    - name: --SEQ
      operator: is_ordered_by
      order: asc
```

## is_not_ordered_by

Complement of `is_ordered_by`

## target_is_sorted_by

True if the values in `name` are ordered according to the values specified by `value` grouped by the values in `within`. Each `value` requires a variable `name`, ordering specified by `sort_order` (`asc` or `desc`), and the null position specified by `null_position` (`first` or `last`).

```yaml
Check:
  all:
    - name: --SEQ
      within: USUBJID
      operator: target_is_sorted_by
      value:
        - name: --STDTC
          sort_order: asc
          null_position: last
```

## target_is_not_sorted_by

Complement of `target_is_sorted_by`

## shares_at_least_one_element_with

Will raise an issue if at least one of the values in `name` is the same as one of the values in `value`

## shares_exactly_one_element_with

Will raise an issue if exactly one of the values in `name` is the same as one of the values in `value`

## shares_no_elements_with

Will raise an issue if the values in `name` do not share any of the values in `value`

> Check if $dataset_variables shares no elements with $timing_variables

```yaml
Check:
  all:
    - name: $dataset_variables
      operator: shares_no_elements_with
      value: $timing_variables
```

## has_same_values

True if all values in `name` are the same

> Condition: MHCAT ^= null
>
> Rule: MHCAT ^= the same value for all records

```yaml
Check:
  all:
    - name: MHCAT
      operator: non_empty
    - name: MHCAT
      operator: has_same_values
```

## has_different_values

Complement of `has_same_values`

## value_has_multiple_references

True if the value in `name` has a count greater than one in the dictionary held by the variable `value` (a dictionary mapping each value to the number of times it is referenced, typically produced by an operation)

## value_does_not_have_multiple_references

Complement of `value_has_multiple_references`

## is_ordered_subset_of

Checks if elements in the target list appear in the same relative order in the comparator list.

> Check if dataset column order is a correctly ordered subset of library column order

```yaml
- name: $column_order_from_dataset
  operator: is_ordered_subset_of
  value: $column_order_from_library
```

## is_not_ordered_subset_of

Complement of `is_ordered_subset_of`

# Define.XML

## conformant_value_data_type

Value Level Metadata Check against Define XML

True if the types in the row match the VLM types specified in the define.xml

## non_conformant_value_data_type

Complement of `conformant_value_data_type`

## conformant_value_length

Value Level Metadata Check against Define XML

True if the lengths in the row match the VLM lengths specified in the define.xml

## non_conformant_value_length

Complement of `conformant_value_length`

## references_correct_codelist

True if the codelist named within `value` is a valid codelist for the variable named within `name` in the define.xml.

## does_not_reference_correct_codelist

Complement of `references_correct_codelist`
