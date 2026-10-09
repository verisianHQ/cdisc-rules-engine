# How values are interpreted

- `name` is the column the condition checks. `--` is replaced by the dataset's domain prefix.
- A string `value` is read as a column name, or as an operation id (`$name`). To compare against literal text, set `value_is_literal: true`; this also applies to every item of a list. Numbers and booleans are always literals.
- The regex operators (`matches_regex`, `prefix_matches_regex`, `suffix_matches_regex` and their variants) always read `value` as a pattern.
- `value_is_reference: true` (`equal_to` family) reads `value` as a column whose content, on each row, names the column to compare against.
- If `name`, a column in `value`, `within`, `ordering` or `version` doesn't exist in the dataset, the condition is FALSE. This holds for the negated operators (`not_…`, `does_not_…`, `is_not_…`) too, so a missing column never raises an issue. Exceptions: `exists`, `is_unique_set`, `is_unique_relationship`, `is_ordered_subset_of` and their complements; `is_inconsistent_across_dataset` ignores missing grouping variables.
- Optional keys such as `prefix`, `within` or `case_insensitive` are only accepted on the operators that read them, as listed in each operator's section. `value_is_literal`, `value_is_reference` and `variable_regex_pattern` are accepted on every operator.

## variable_regex_pattern

With `variable_regex_pattern: true`, `name` is a Python regular expression, written anchored as `^...$`, matched against the whole of each variable name in the dataset. The rule runs once for each combination of matching variables, one variable per pattern. If a pattern matches no variable, the rule isn't expanded and that condition is FALSE.

Named groups such as `(?P<root>...)` can be referenced as `{{root}}` elsewhere in the rule: in any condition's `name` or `value`, in Outcome Output Variables and in the Message. Each group name may only be defined once. Quote templated values in YAML.

> Any --STDTC is after its matching --ENDTC (for every STDTC / ENDTC pair in the dataset)

```yaml
- name: '^(?P<root>\w+)STDTC$'
  variable_regex_pattern: true
  operator: date_greater_than
  value: "{{root}}ENDTC"
```

# Relational

## equal_to

Value comparison. Works for both string and number. Empty values never equal anything; `not_equal_to` is TRUE when exactly one side is empty.
Has optional parameter:

- 'value_is_reference' when true, the value parameter specifies a column name whose content determines which column to compare against dynamically.
- 'type_insensitive' when true, both values are compared as text, to handle type mismatches between string and numeric data. Set to `numeric` to compare both values as numbers instead (so `1`, `1.0` and `1E0` are equal); non-numeric values then become null.

> --OCCUR = N

```yaml
- name: --OCCUR
  operator: equal_to
  value: "N"
  value_is_literal: true
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

Complement of `equal_to`. Also has the optional parameters 'value_is_reference' and 'type_insensitive'.

> --OCCUR ^= Y

```yaml
- name: --OCCUR
  operator: not_equal_to
  value: "Y"
  value_is_literal: true
```

## equal_to_case_insensitive

Case insensitive `equal_to`. Also accepts 'value_is_reference' and 'type_insensitive'.

> DSTERM is "Informed consent obtained"

```yaml
- name: DSTERM
  operator: equal_to_case_insensitive
  value: Informed consent obtained
  value_is_literal: true
```

## not_equal_to_case_insensitive

Complement of `equal_to_case_insensitive`

## greater_than

Numeric comparison, also used by `greater_than_or_equal_to`, `less_than` and `less_than_or_equal_to`. Both values are cast to numbers, so a non-numeric value in the column makes the rule fail with an error. Empty values are FALSE.

> TSVAL > 0

```yaml
- name: TSVAL
  operator: greater_than
  value: 0
```

## greater_than_or_equal_to

Value comparison

> TSVAL >= 1

```yaml
- name: TSVAL
  operator: greater_than_or_equal_to
  value: 1
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

## does_not_equal_string_part

Complement of `equals_string_part`

## equals_string_part

True if `name` equals the first capture group of the regex `regex` (PostgreSQL syntax, see `matches_regex`) applied to `value`.

> RDOMAIN equals characters 5 and 6 of SUPP dataset name

```yaml
- name: RDOMAIN
  operator: equals_string_part
  value: dataset_name
  regex: ".{4}(..).*"
```

## matches_regex

Regular expression matching, evaluated by PostgreSQL ([POSIX regular expressions](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP)).

- The pattern is searched for anywhere in the value. Anchor it with `^` and `$` to match the whole value.
- Empty values never match, so `matches_regex` and `not_matches_regex` are both false for them.
- Common syntax (character classes, quantifiers, `\d`, `\w`, alternation, lookahead) behaves as in Python, with these differences:
  - `\b` is a backspace, not a word boundary. Use `\y` for a word boundary, or `\m` / `\M` for the start / end of a word.
  - Inline flags such as `(?i)` are only allowed at the very start of the pattern.
  - Named groups `(?P<name>...)` are not supported.
  - A literal `'` must be written `\x27`.
- In YAML, quote patterns with single quotes: inside double quotes `\d` is an invalid escape.

> --DOSTXT value is numeric

```yaml
- name: --DOSTXT
  operator: matches_regex
  value: ^\d*\.?\d*$
```

## not_matches_regex

Complement of `matches_regex`

> --TESTCD <= 8 chars and contains only letters, numbers, and underscores and can not start with a number

```yaml
- name: --TESTCD
  operator: not_matches_regex
  value: ^[A-Z_][A-Z0-9_]{0,7}$
```

## matches_regex_case_insensitive

Case insensitive `matches_regex`

## not_matches_regex_case_insensitive

Complement of `matches_regex_case_insensitive`

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

## prefix_matches_regex_case_insensitive

Case insensitive `prefix_matches_regex`

## not_prefix_matches_regex_case_insensitive

Complement of `prefix_matches_regex_case_insensitive`

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

## suffix_matches_regex_case_insensitive

Case insensitive `suffix_matches_regex`

## not_suffix_matches_regex_case_insensitive

Complement of `suffix_matches_regex_case_insensitive`

## starts_with

Substring matching. `value` may also be a list or a `$collection`, meaning any of them. Matching uses SQL `LIKE`, so `_` and `%` in `value` act as wildcards.

> DOMAIN beginning with 'AP'

```yaml
- name: "DOMAIN"
  operator: "starts_with"
  value: "AP"
  value_is_literal: true
```

## not_starts_with

Complement of `starts_with`

## ends_with

Substring matching, as `starts_with`.

> DOMAIN ending with 'FOOBAR'

```yaml
- name: "DOMAIN"
  operator: "ends_with"
  value: "FOOBAR"
  value_is_literal: true
```

## not_ends_with

Complement of `ends_with`

## prefix_equal_to

True if the `prefix` number of characters beginning a string in `name` match the string in `value`, ignoring case. `value` may also be a list or a `$collection`, meaning any of them.

```yaml
- name: dataset_name
  operator: prefix_equal_to
  prefix: 2
  value: DOMAIN
```

## prefix_not_equal_to

Complement of `prefix_equal_to`

## suffix_equal_to

True if the `suffix` number of characters ending a string in `name` match the string in `value`, ignoring case. `value` may also be a list or a `$collection`, meaning any of them.

```yaml
- name: dataset_name
  operator: suffix_equal_to
  suffix: 2
  value: DOMAIN
```

## suffix_not_equal_to

Complement of `suffix_equal_to`

## contains

True if the value in `value` is a substring of the value in `name`. `value` may also be a list or a `$collection`, meaning any of them. Matching uses SQL `LIKE`, so `_` and `%` in `value` act as wildcards. Empty values contain nothing, so `does_not_contain` is TRUE for them.

When `name` is a `$collection` operation result, it is instead TRUE if `value` is exactly one of its items.

> --TOXGR contains 'GRADE'

```yaml
- name: "--TOXGR"
  operator: "contains"
  value: "GRADE"
  value_is_literal: true
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
  value_is_literal: true
```

## does_not_contain_case_insensitive

Complement of `contains_case_insensitive`

> --TOXGR does not contain 'GRADE', regardless of text case

```yaml
- name: "--TOXGR"
  operator: "does_not_contain_case_insensitive"
  value: "grade"
  value_is_literal: true
```

## longer_than

Length comparison

> SETCD value length > 8

```yaml
- name: "SETCD"
  operator: "longer_than"
  value: 8
```

## longer_than_or_equal_to

Length comparison

> TSVAL value length >= 201

```yaml
- name: "TSVAL"
  operator: "longer_than_or_equal_to"
  value: 201
```

## shorter_than

Length comparison

> SETCD value length < 9

```yaml
- name: "SETCD"
  operator: "shorter_than"
  value: 9
```

## shorter_than_or_equal_to

Length comparison

> TSVAL value length <= 200

```yaml
- name: "TSVAL"
  operator: "shorter_than_or_equal_to"
  value: 200
```

## has_equal_length

Length comparison

> Check whether variable values has equal length of another variable.

```yaml
- name: SEENDTC
  operator: has_equal_length
  value: SESTDTC
```

## has_not_equal_length

Complement of `has_equal_length`

# Date

The `date_*` operators compare the date in `name` with the date in `value`. They are FALSE when either side is empty or, without `date_component`, not an ISO 8601 date (`YYYY`, `YYYY-MM`, `YYYY-MM-DD`, optionally followed by a time); values with missing components such as `2021---15` never match.

- Without `date_component`, both values are cut to the length of the shorter one and compared as text, so `2021` is equal to `2021-05-01` and neither is less than the other.
- With `date_component` (`year`, `month`, `day`, `hour`, `minute`, `second` or `microsecond`), only that component is compared, so for `month` `2021-05` equals `2022-05`. Both values must be long enough to contain the component, otherwise the condition is FALSE: `month` needs at least `YYYY-MM`, while for `year` a bare `2021` works.

## date_equal_to

Date comparison. Compare `name` to `value`.

## date_not_equal_to

Date comparison. True if `name` and `value` are different dates. Like the other date operators, it is FALSE when either side is empty or invalid, so it isn't a strict complement of `date_equal_to`.

## date_greater_than

Date comparison. Compare `name` to `value`.

> Year part of BRTHDTC > 2021

```yaml
- name: "BRTHDTC"
  operator: "date_greater_than"
  date_component: "year"
  value: "2021"
  value_is_literal: true
```

## date_greater_than_or_equal_to

Date comparison. Compare `name` to `value`.

> Year part of BRTHDTC >= 2021

```yaml
- name: "BRTHDTC"
  operator: "date_greater_than_or_equal_to"
  date_component: "year"
  value: "2021"
  value_is_literal: true
```

## date_less_than

Date comparison. Compare `name` to `value`.

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
      value_is_literal: true
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

Date comparison. Compare `name` to `value`.

> AEENDTC <= AESTDTC

```yaml
- name: "AEENDTC"
  operator: "date_less_than_or_equal_to"
  value: "AESTDTC"
```

## is_complete_date

Date check

> DM.RFSTDTC = complete date

```yaml
- name: "RFSTDTC"
  operator: "is_complete_date"
```

## is_incomplete_date

Complement of `is_complete_date`, so TRUE for empty values

Date check

> DM.RFSTDTC ^= complete date

```yaml
- name: "RFSTDTC"
  operator: "is_incomplete_date"
```

## invalid_date

Date check. Empty values are invalid (TRUE).

> BRTHDTC is invalid

```yaml
- name: "BRTHDTC"
  operator: "invalid_date"
```

## invalid_date_interval

ISO 8601 date interval check, returns True if the value is not a valid interval. A valid interval has exactly two parts separated by a solidus (`/`), in one of these forms:

- start datetime / end datetime, e.g. `2023-01-01T08:00:00/2023-01-02T08:00:00`
- start datetime / duration, e.g. `2023-01-01T08:00:00/P1DT2H`
- duration / end datetime, e.g. `P1DT2H/2023-01-02T08:00:00`

Datetime parts are validated as in `invalid_date` and duration parts as in `invalid_duration` (negative durations disallowed). Empty values are invalid.

> INTVAR is invalid

```yaml
- name: "INTVAR"
  operator: "invalid_date_interval"
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

> A Define-XML was provided for the study

```yaml
- name: "define.xml"
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

# Relationship & Set

## is_contained_by

Value in `name` compared against a list in `value`. The list can have literal values or be a reference to a `$variable`. If `value` is a column, TRUE when the value in `name` appears anywhere in that column (on any row). Empty values are never contained, so `is_not_contained_by` is TRUE for them.

> ACTARM in ('Screen Failure', 'Not Assigned', 'Not Treated', 'Unplanned Treatment')

```yaml
- name: "ACTARM"
  operator: "is_contained_by"
  value:
    - "Screen Failure"
    - "Not Assigned"
    - "Not Treated"
    - "Unplanned Treatment"
  value_is_literal: true
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
  value_is_literal: true
```

## is_contained_by_case_insensitive

Value in `name` case insensitive compared against a list in `value`. The list can have literal values or be a reference to a `$variable`.

> ACTARM in ('Screen Failure', 'Not Assigned', 'Not Treated', 'Unplanned Treatment')

```yaml
- name: "ACTARM"
  operator: "is_contained_by_case_insensitive"
  value:
    - "Screen Failure"
    - "Not Assigned"
    - "Not Treated"
    - "Unplanned Treatment"
  value_is_literal: true
```

## is_not_contained_by_case_insensitive

Complement of `is_contained_by_case_insensitive`

> ARM not in ('Screen Failure', 'Not Assigned')

```yaml
- name: "ARM"
  operator: "is_not_contained_by_case_insensitive"
  value:
    - "Screen Failure"
    - "Not Assigned"
  value_is_literal: true
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

## prefix_is_contained_by_case_insensitive

Case insensitive `prefix_is_contained_by`

## prefix_is_not_contained_by_case_insensitive

Complement of `prefix_is_contained_by_case_insensitive`

## suffix_is_contained_by_case_insensitive

Case insensitive `suffix_is_contained_by`

## suffix_is_not_contained_by_case_insensitive

Complement of `suffix_is_contained_by_case_insensitive`

## contains_all

True if all values in `value` (a list, a column or a `$collection`) appear in the variable `name` on at least one row. This is a dataset-level check, exact match, with the same result on every row.

> All of ('Screen Failure', 'Not Assigned', 'Not Treated', 'Unplanned Treatment') in ACTARM

```yaml
- name: "ACTARM"
  operator: "contains_all"
  value:
    - "Screen Failure"
    - "Not Assigned"
    - "Not Treated"
    - "Unplanned Treatment"
  value_is_literal: true
```

## not_contains_all

Complement of `contains_all`

> All of ('Screen Failure', 'Not Assigned', 'Not Treated', 'Unplanned Treatment') not in ACTARM

```yaml
- name: "ACTARM"
  operator: "not_contains_all"
  value:
    - "Screen Failure"
    - "Not Assigned"
    - "Not Treated"
    - "Unplanned Treatment"
  value_is_literal: true
```

## is_inconsistent_across_dataset

Checks if a variable maintains consistent values within groups defined by one or more grouping variables. Groups records by specified value(s) and validates that the target variable maintains the same value within each unique combination of grouping variables.

Empty values of `name` count as a value of their own, and empty grouping values form their own group. Grouping variables missing from the dataset are ignored (FALSE if none exist).

Optional parameters:

- `where_populated: true` only considers rows where `name` and all grouping variables are populated; other rows are FALSE.
- `where_populated_columns`: a list of further variables that must be populated in the same way. Variables missing from the dataset are ignored; if none exist and `where_populated` isn't set, the rule errors.

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

Relationship Integrity Check. True if the combination of `name` and the `value` columns occurs on only one row. `name` and `value` are column names and `value` is optional. Columns missing from the dataset, and `$operation` ids, are ignored. Empty values are equal to each other.

> --SEQ is unique within DOMAIN, USUBJID, and --TESTCD

```yaml
- name: "--SEQ"
  operator: is_unique_set
  value:
    - "DOMAIN"
    - "USUBJID"
    - "--TESTCD"
```

> STUDYID, USUBJID, --TESTCD and VISITNUM are unique together

```yaml
- name: STUDYID
  operator: is_unique_set
  value:
    - USUBJID
    - --TESTCD
    - VISITNUM
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

> STUDYID, USUBJID, --TESTCD and VISITNUM are not unique together

```yaml
- name: STUDYID
  operator: is_not_unique_set
  value:
    - USUBJID
    - --TESTCD
    - VISITNUM
```

## present_on_multiple_rows_within

True if the same value of `name` is present on multiple rows, grouped by `within` (a single variable). A maximum allowed number of occurrences can be specified in the value attribute as an integer. In this instance the value: 4 means that an error will be flagged if the same value appears more than 4 times within a USUBJID. By default the operator will flag any time a value appears more than once. Empty values are counted like any other value.

```yaml
- operator: "present_on_multiple_rows_within"
  name: "RELID"
  value: 4 # optional, defaults to 1
  within: "USUBJID"
```

## not_present_on_multiple_rows_within

Complement of `present_on_multiple_rows_within`

```yaml
- operator: "not_present_on_multiple_rows_within"
  name: "RELID"
  value: 4 # optional, defaults to 1
  within: "USUBJID"
```

## is_unique_relationship

Relationship Integrity Check

> AETERM and AEDECOD has a 1-to-1 relationship

```yaml
- name: AETERM
  operator: is_unique_relationship
  value: AEDECOD
```

## is_not_unique_relationship

Complement of `is_unique_relationship`

## empty_within_except_last_row

True if `name` is empty on a record that isn't the last of its group. `value` is the grouping variable; the optional `ordering` variable sets the record order within each group (default: dataset order).

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

Ensures that a value of a variable `name` in one record is equal to the value of another variable `value` in the next corresponding record. The rows are grouped by `within` (a single variable) and ordered by `ordering`. The last record of each group is always TRUE.

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

True if the dataset rows are in ascending order of the values within `name`, grouped by the values within `value`. Order means the physical row order of the dataset; numbers compare numerically, other values as text, and equal consecutive values are allowed. The result applies to every row of the group. Rows with an empty `name` are FALSE.

```yaml
Check:
  all:
    - name: --SEQ
      operator: is_ordered_set
      value: USUBJID
```

## is_not_ordered_set

Complement of `is_ordered_set`, except that rows with an empty `name` are FALSE for both

## target_is_not_sorted_by

Complement of `target_is_sorted_by`

## target_is_sorted_by

True if the values in `name` are ordered according to the sort specification in `value`, within each group defined by `within`. `within` may be a single variable or a list of variables. Each item in `value` requires the variable `name` (a column, not an operation result), the `sort_order` (`asc` or `desc`) and the `null_position` (`first` or `last`).

Each sort key is checked separately: a row is TRUE when its position in the group sorted by `name` (ascending, empty values last) matches its position sorted by every sort key. Rows with an empty sort key are FALSE, so `target_is_not_sorted_by` flags them. Rows whose first sort key overlaps the next row's as partial dates (e.g. `2021-05` and `2021-05-14`) are also FALSE.

Optional parameters:

- `strict_incremental_ordering: true` replaces the check above: the value in `name` must equal its 1-based position in the group sorted by all the sort keys together (e.g. 1, 2, 3); `null_position` applies and empty values of `name` are TRUE.
- `regex` extracts the integer to compare from each value of `name`, using the first capture group (PostgreSQL syntax), e.g. `.*?(\d+)$`.

```yaml
Check:
  all:
    - name: --SEQ
      within:
        - USUBJID
        - --CAT
      operator: target_is_sorted_by
      value:
        - name: --STDTC
          sort_order: asc
          null_position: last
```

## shares_at_least_one_element_with

Will raise an issue if at least one of the values in `name` is the same as one of the values in `value`

The `shares_*` operators compare whole sets at dataset level and give the same result on every row: all non-empty values of `name` (a column or `$operation` result) against those of `value`, which must be a column or a `$operation` id.

## shares_exactly_one_element_with

Will raise an issue if exactly one of the values in `name` is the same as one of the values in `value`. Dataset-level, as `shares_at_least_one_element_with`.

## shares_no_elements_with

Will raise an issue if the values in `name` do not share any of the values in `value`. Dataset-level, as `shares_at_least_one_element_with`.

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

## is_ordered_subset_of

Checks if elements in the target list appear in the same relative order in the comparator list. When `name` is a column, optional `prefix` / `suffix` compare only the first / last N characters of its values.

> Check if dataset column order is a correctly ordered subset of library column order

```yaml
- name: $column_order_from_dataset
  operator: is_ordered_subset_of
  value: $column_order_from_library
```

## is_not_ordered_subset_of

Complement of `is_ordered_subset_of`

## is_substring_of

True if the value in `name` is contained within the value(s) in `value`. `value` may be a variable, a list of literals or an operation result. Empty values are never substrings. Matching uses SQL `LIKE`, so `_` and `%` in `name` act as wildcards.

> AEDECOD appears within AETERM

```yaml
- name: AEDECOD
  operator: is_substring_of
  value: AETERM
```

## is_not_substring_of

Complement of `is_substring_of`

## in_enumerated_columns

True if the value in `value` equals the value of `name` or of any of its enumerated variables, i.e. `name` followed by a number (`RACE`, `RACE1`, `RACE2`, ...). Empty values never match.

Optional parameters:

- `regex`: a custom pattern for the enumerated variable names, used instead of `name` followed by a number (Python syntax, matched from the start of each variable name, ignoring case)
- `case_insensitive: true` compares the values ignoring case

> The value of RACEOTH is one of the RACE variables

```yaml
- name: RACE
  operator: in_enumerated_columns
  value: RACEOTH
```

## not_in_enumerated_columns

Complement of `in_enumerated_columns`

# Controlled Terminology

## is_extensible_codelist_code

True if the value in `name` is the codelist code (C-code) of an extensible codelist in the controlled terminology in use

## is_not_extensible_codelist_code

Complement of `is_extensible_codelist_code`

# External Dictionaries

These operators validate values against an external dictionary loaded into the engine (see the CLI `--meddra`, `--whodrug`, `--loinc`, `--medrt`, `--unii` and `--snomed-*` options). Each dictionary has the same set of operators:

- `is_valid_<dictionary>_code_reference`: True if the value in `name` is a code in the dictionary
- `is_valid_<dictionary>_term_reference`: True if the value in `name` is a term in the dictionary
- `is_valid_<dictionary>_code_term_pair`: True if the code in `name` and the term in `value` are a matching pair in the dictionary. If `value` ends in `CD`, `value` is taken as the code and `name` as the term instead.

with an `is_not_valid_` complement for each.

- Empty values are not valid, so the `is_not_valid_` operators are TRUE for them; add a `non_empty` condition if empty values are allowed.
- For term references, dictionary terms containing `,` or `;` are split, and the value is valid if it equals any part.
- For WHODrug, the value `MULTIPLE` in `name` is always valid.

Optional parameters:

- `case_insensitive: true` compares ignoring case
- `filter_attribute` and `filter_value` restrict the dictionary entries compared against, e.g. MedDRA `term_type` (`PT`, `LLT`, `HLT`, `HLGT`, `SOC`), or LOINC `status` / `version`. `filter_value` may be an operation result. With `filter_attribute: version`, entries whose version is less than or equal to `filter_value` are kept.

> --LLTCD is not a valid MedDRA lowest level term code

```yaml
- name: --LLTCD
  operator: is_not_valid_meddra_code_reference
  filter_attribute: term_type
  filter_value: LLT
```

> --DECOD and --TRT are not a valid WHODrug code/term pair

```yaml
- name: --DECOD
  operator: is_not_valid_whodrug_code_term_pair
  value: --TRT
```

## is_valid_meddra_code_reference

True if the value in `name` is a valid MedDRA code

## is_not_valid_meddra_code_reference

Complement of `is_valid_meddra_code_reference`

## is_valid_meddra_term_reference

True if the value in `name` is a valid MedDRA term

## is_not_valid_meddra_term_reference

Complement of `is_valid_meddra_term_reference`

## is_valid_meddra_code_term_pair

True if the MedDRA code in `name` and the term in `value` are a matching pair

## is_not_valid_meddra_code_term_pair

Complement of `is_valid_meddra_code_term_pair`

## is_valid_whodrug_code_reference

True if the value in `name` is a valid WHODrug code

## is_not_valid_whodrug_code_reference

Complement of `is_valid_whodrug_code_reference`

## is_valid_whodrug_term_reference

True if the value in `name` is a valid WHODrug term

## is_not_valid_whodrug_term_reference

Complement of `is_valid_whodrug_term_reference`

## is_valid_whodrug_code_term_pair

True if the WHODrug code in `name` and the term in `value` are a matching pair

## is_not_valid_whodrug_code_term_pair

Complement of `is_valid_whodrug_code_term_pair`

## is_valid_medrt_code_reference

True if the value in `name` is a valid MED-RT code

## is_not_valid_medrt_code_reference

Complement of `is_valid_medrt_code_reference`

## is_valid_medrt_term_reference

True if the value in `name` is a valid MED-RT term

## is_not_valid_medrt_term_reference

Complement of `is_valid_medrt_term_reference`

## is_valid_medrt_code_term_pair

True if the MED-RT code in `name` and the term in `value` are a matching pair

## is_not_valid_medrt_code_term_pair

Complement of `is_valid_medrt_code_term_pair`

## is_valid_loinc_code_reference

True if the value in `name` is a valid LOINC code

## is_not_valid_loinc_code_reference

Complement of `is_valid_loinc_code_reference`

## is_valid_loinc_term_reference

True if the value in `name` is a valid LOINC term

## is_not_valid_loinc_term_reference

Complement of `is_valid_loinc_term_reference`

## is_valid_loinc_code_term_pair

True if the LOINC code in `name` and the term in `value` are a matching pair

## is_not_valid_loinc_code_term_pair

Complement of `is_valid_loinc_code_term_pair`

## is_valid_snomed_code_reference

True if the value in `name` is a valid SNOMED CT code

## is_not_valid_snomed_code_reference

Complement of `is_valid_snomed_code_reference`

## is_valid_snomed_term_reference

True if the value in `name` is a valid SNOMED CT term

## is_not_valid_snomed_term_reference

Complement of `is_valid_snomed_term_reference`

## is_valid_snomed_code_term_pair

True if the SNOMED CT code in `name` and the term in `value` are a matching pair

## is_not_valid_snomed_code_term_pair

Complement of `is_valid_snomed_code_term_pair`

## is_valid_unii_code_reference

True if the value in `name` is a valid UNII code

## is_not_valid_unii_code_reference

Complement of `is_valid_unii_code_reference`

## is_valid_unii_term_reference

True if the value in `name` is a valid UNII term

## is_not_valid_unii_term_reference

Complement of `is_valid_unii_term_reference`

## is_valid_unii_code_term_pair

True if the UNII code in `name` and the term in `value` are a matching pair

## is_not_valid_unii_code_term_pair

Complement of `is_valid_unii_code_term_pair`

## is_valid_whodrug_level_reference

True if the value in `name` is a WHODrug ATC level text (any of ATC levels 1-4)

## is_not_valid_whodrug_level_reference

Complement of `is_valid_whodrug_level_reference`

## is_latest_available_external_dictionary_version

True if the version in `version` is the latest release of the dictionary in `external_dictionary_type` (`meddra`, `whodrug`, `medrt`, `loinc`, `snomed` or `unii`) that was available on the date in `name`. `name` must hold full dates (`YYYY-MM-DD`); partial dates make the rule fail with an error. `version` may be a variable, an operation result, or a literal with `value_is_literal: true`.

> The MedDRA version in define.xml was not the latest available at the study start date

```yaml
Check:
  all:
    - name: TSPARMCD
      operator: equal_to
      value: SSTDTC
      value_is_literal: true
    - name: TSVAL
      operator: is_not_latest_available_external_dictionary_version
      external_dictionary_type: meddra
      version: $define_meddra_version
Operations:
  - id: $define_meddra_version
    operator: get_define_external_dictionary_version
    external_dictionary_type: meddra
```

## is_not_latest_available_external_dictionary_version

Complement of `is_latest_available_external_dictionary_version`
