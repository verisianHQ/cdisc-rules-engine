In the examples below, `value_is_literal: true` is set wherever `value` is a literal string or a list of literal strings. The SQL engine treats a string `value` (or each string in a list) as a column name unless `value_is_literal: true` is set, and the condition returns False if no such column exists. This does not apply to numbers, `$` operation variables or the regular expression operators, whose `value` is always a pattern. In the SQL engine the condition is also False when `name`, `within` or `ordering` is not a column or a `$` operation variable (except for `exists`), and all of this applies to the negated operators too (e.g. `non_empty`, `is_not_contained_by`).

**SQL engine:** A condition can set `variable_regex_pattern: true` to make `name` a regular expression (Python syntax) matched against whole variable names. The rule is then run once for each matching variable of the dataset. Named groups such as `(?P<root>...)` can be used elsewhere in the rule as `{{root}}`. If no variable matches, the condition is False.

> For each variable whose name starts with AGE, the value contains 55

```yaml
Check:
  all:
    - name: "^(?P<age>AGE.*)$"
      variable_regex_pattern: true
      operator: matches_regex
      value: ".*55.*"
Outcome:
  Output Variables:
    - "{{age}}"
```

# Relational

## equal_to

Value comparison. Works for both string and number. True if the value in `name` is equal to `value`. If `value` matches the name of a column in the dataset, the value of that column in the same record is used for the comparison. Returns False when either value is null or empty.
Has optional parameters:

- 'value_is_literal' when true, `value` is always treated as a literal, even if a column with that name exists in the dataset. Set it whenever `value` is a literal string: the SQL engine treats a string `value` as a column name unless this is set, and returns False if no such column exists.
- 'value_is_reference' when true, the value parameter specifies a column name whose content determines which column to compare against dynamically.
- 'type_insensitive' when true, both values are converted to strings before comparison to handle type mismatches between string and numeric data.

**SQL engine:** 'type_insensitive' can also be set to `numeric`: both values are converted to numbers before comparing (e.g. `"1.50"` equals `1.5`), and a value that is not a number never equals a number.

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

Complement of `equal_to`. Also has the optional parameters 'value_is_literal', 'value_is_reference' and 'type_insensitive'. Note that, like `equal_to`, it returns False when both values are null or empty.

> --OCCUR ^= Y

```yaml
- name: --OCCUR
  operator: not_equal_to
  value: "Y"
  value_is_literal: true
```

## equal_to_case_insensitive

Case insensitive `equal_to`. Supports the optional parameter 'value_is_literal', but not 'value_is_reference' or 'type_insensitive'.

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

**SQL engine:** Returns False when the value in `name` or in `value` is empty, or when `regex` does not match `value`.

## matches_regex

Regular Expression value matching

- Determine if each string contains a match of a regular expression, using the PostgreSQL `~` operator: the regex matches if it is found anywhere in the text.
- To match only at the start of the text, prefix the regex with the anchor `^`
- To do a "fullmatch" of a regex with the entire text, use both anchors `^` and `$`
- The regex uses the PostgreSQL syntax, see the PostgreSQL documentation: [POSIX Regular Expressions](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP). Common constructs such as `\d`, `\w`, `[A-Z]`, `{0,7}` and `(a|b)` work as in other dialects, but some do not: e.g. named groups `(?P<name>...)` are not supported, and `\b` is a backspace (use `\y` for a word boundary).
- Suggestion for an online regular expression tester: https://regex101.com (PostgreSQL is not one of its dialects, so check any less common syntax against the PostgreSQL documentation).
- For regex token visualization, try https://www.debuggex.com.
- This also applies to the other regular expression operators (`prefix_matches_regex`, `suffix_matches_regex`, their complements and their case insensitive variants).
- The deprecated pandas engine instead matches from the start of the text and uses the Python syntax.

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

**SQL engine:** Returns False when the value in `name` is empty.

## matches_regex_case_insensitive

_SQL engine only._

Case insensitive `matches_regex`

> Dataset name is FA followed by 1 or 2 letters or digits, regardless of text case

```yaml
- name: dataset_name
  operator: matches_regex_case_insensitive
  value: ^FA[A-Za-z0-9]{1,2}$
```

## not_matches_regex_case_insensitive

_SQL engine only._

Complement of `matches_regex_case_insensitive`. Returns False when the value in `name` is empty.

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

**SQL engine:** Returns False when the value in `name` is empty.

## prefix_matches_regex_case_insensitive

_SQL engine only._

Case insensitive `prefix_matches_regex`

## not_prefix_matches_regex_case_insensitive

_SQL engine only._

Complement of `prefix_matches_regex_case_insensitive`. Returns False when the value in `name` is empty.

> The first 2 characters of the dataset name are not AP or FA, regardless of text case

```yaml
- name: dataset_name
  operator: not_prefix_matches_regex_case_insensitive
  prefix: 2
  value: (AP|FA)
```

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

**SQL engine:** Returns False when the value in `name` is empty.

## suffix_matches_regex_case_insensitive

_SQL engine only._

Case insensitive `suffix_matches_regex`

> Dataset name ends with CM, regardless of text case

```yaml
- name: dataset_name
  operator: suffix_matches_regex_case_insensitive
  suffix: 2
  value: CM
```

## not_suffix_matches_regex_case_insensitive

_SQL engine only._

Complement of `suffix_matches_regex_case_insensitive`. Returns False when the value in `name` is empty.

## starts_with

Substring matching. True if the value in `name` starts with the string in `value` (case sensitive).

> DOMAIN beginning with 'AP'

```yaml
- name: "DOMAIN"
  operator: "starts_with"
  value: "AP"
  value_is_literal: true
```

**SQL engine:** `value` can also be a list or a `$variable` collection; true if the value starts with any of them. When `value` is a variable, each record is compared with the value of that variable in the same record. An empty `value` never matches.

## not_starts_with

_SQL engine only._

Complement of `starts_with`

> DOMAIN not beginning with 'AP'

```yaml
- name: "DOMAIN"
  operator: "not_starts_with"
  value: "AP"
  value_is_literal: true
```

## ends_with

Substring matching. True if the value in `name` ends with the string in `value` (case sensitive).

> DOMAIN ending with 'FOOBAR'

```yaml
- name: "DOMAIN"
  operator: "ends_with"
  value: "FOOBAR"
  value_is_literal: true
```

**SQL engine:** `value` can also be a list or a `$variable` collection; true if the value ends with any of them. When `value` is a variable, each record is compared with the value of that variable in the same record. An empty `value` never matches.

## not_ends_with

_SQL engine only._

Complement of `ends_with`

> DOMAIN not ending with 'FOOBAR'

```yaml
- name: "DOMAIN"
  operator: "not_ends_with"
  value: "FOOBAR"
  value_is_literal: true
```

## prefix_equal_to

True if the `prefix` number of characters beginning a string in `name` match the string in `value`

```yaml
- name: dataset_name
  operator: prefix_equal_to
  prefix: 2
  value: DOMAIN
```

**SQL engine:** `value` can also be a list or a `$variable` collection; true if the prefix is equal to any of them. The comparison ignores case. When `value` is `DOMAIN` (without `value_is_literal`), the prefix is compared with the domain of the dataset being evaluated, which must have a DOMAIN variable. The deprecated pandas engine compares case sensitively.

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

**SQL engine:** `value` can also be a list or a `$variable` collection; true if the suffix is equal to any of them. The comparison ignores case. When `value` is `DOMAIN` (without `value_is_literal`), the suffix is compared with the domain of the dataset being evaluated, which must have a DOMAIN variable. The deprecated pandas engine compares case sensitively, and only resolves `DOMAIN` for `prefix_equal_to`.

## suffix_not_equal_to

Complement of `suffix_equal_to`

## contains

True if the value in `value` is a substring of the value in `name`

> --TOXGR contains 'GRADE'

```yaml
- name: "--TOXGR"
  operator: "contains"
  value: "GRADE"
  value_is_literal: true
```

**SQL engine:** When `value` is a variable, true if the value of that variable in the same record is a substring of the value in `name`. This also applies to `contains_case_insensitive`.

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

## is_substring_of

_SQL engine only._

True if the value in `name` is a substring of `value` (case sensitive). `value` can be a literal, a variable (its value in the same record is used), a list or a `$variable`; for a list or a collection `$variable`, true if the value in `name` is a substring of any of its values. Returns False when the value in `name` is empty.

> 'QT' is part of 'QT Studies'

```yaml
- name: --TESTCD
  operator: is_substring_of
  value: QT Studies
  value_is_literal: true
```

## is_not_substring_of

_SQL engine only._

Complement of `is_substring_of`

> FATESTCD is not part of any of the variable names in $fatest_lib_names

```yaml
Check:
  all:
    - name: FATESTCD
      operator: is_not_substring_of
      value: $fatest_lib_names
Operations:
  - id: $fatest_lib_names
    operator: label_referenced_variable_metadata
    name: FATEST
    attribute_name: name
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

**SQL engine:** Returns False when the value in `name` is empty.

## shorter_than_or_equal_to

Length comparison. Complement of `longer_than`: true if the length of the value in `name` is less than or equal to `value`.

> TSVAL value length <= 200

```yaml
- name: "TSVAL"
  operator: "shorter_than_or_equal_to"
  value: 200
```

**SQL engine:** Returns False when the value in `name` is empty.

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

**SQL engine:** Returns False when the value in `name` is empty.

# Date

**SQL engine:** In the `date_*` comparison operators, without `date_component` both values must be ISO 8601 dates or datetimes (otherwise the result is False), and the longer one is cut to the length of the shorter one before comparing: a partial date is compared only to the precision it has (`2023-01` is equal to `2023-01-15`), and time zones are not taken into account. With `date_component`, the result is False when either value does not contain that component, so `value` must be a full date or datetime (only a `year` can be given on its own, e.g. `"2021"`).

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
  value_is_literal: true
```

## date_greater_than_or_equal_to

Date comparison. Compare `name` to `value`. Compares partial dates if `date_component` is specified.

> Year part of BRTHDTC >= 2021

```yaml
- name: "BRTHDTC"
  operator: "date_greater_than_or_equal_to"
  date_component: "year"
  value: "2021"
  value_is_literal: true
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

## invalid_date_interval

_SQL engine only._

Date check. True if the value in `name` is not a valid ISO 8601 interval. A valid interval has exactly two parts separated by `/`: `<start>/<end>`, `<start>/<duration>` or `<duration>/<end>` (e.g. `2023-01-01/2023-01-02`, `2023-01-01T08:00/PT1.5H`, `P1DT2H/2023-01-02`). The date parts are validated as for `invalid_date` and the duration part as for `invalid_duration` with negative durations disallowed. Empty values are invalid.

> --EVLINT is not a valid interval

```yaml
- name: --EVLINT
  operator: invalid_date_interval
```

> variable_value is neither a valid date nor a valid interval

```yaml
- name: variable_value
  operator: invalid_date
- name: variable_value
  operator: invalid_date_interval
```

## invalid_duration

Duration ISO-8601 check, returns True if a duration is not in ISO-8601 format. The negative parameter must be specified to indicate if negative durations are either allowed (True) or disallowed (False)

> DURVAR is invalid (negative durations disallowed)

```yaml
- name: "DURVAR"
  operator: "invalid_duration"
  negative: False
```

**SQL engine:** Null values are not reported as invalid. If `negative` is not given, negative durations are invalid.

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

**SQL engine:** Values inside list-valued variables are not searched. `name` can also be a `$variable` of the rule (always true) or `define.xml` (true if a define.xml was provided).

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

## in_enumerated_columns

_SQL engine only._

True if `value` is equal to the value of any of the enumerated variables of `name` in the same record (`name`, `name`1, `name`2, etc., as for `inconsistent_enumerated_columns`). Empty values never match. `value` can be a literal, a variable or a `$variable`.
Has optional parameters:

- 'case_insensitive' when true, the values are compared ignoring case.
- 'regex' a regular expression that selects the variables to check instead of the enumeration of `name`. It is matched from the start of the variable names, ignoring case.

## not_in_enumerated_columns

_SQL engine only._

Complement of `in_enumerated_columns`

> RACE is MULTIPLE but none of RACE, RACE1, RACE2, ... is BLACK OR AFRICAN AMERICAN

```yaml
- name: RACE
  operator: equal_to
  value: MULTIPLE
  value_is_literal: true
- name: RACE
  operator: not_in_enumerated_columns
  value: BLACK OR AFRICAN AMERICAN
  value_is_literal: true
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

_Not supported by the SQL engine._

## variable_metadata_not_equal_to

Complement of `variable_metadata_equal_to`

_Not supported by the SQL engine._

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

> ACTARM in ('Screen Failure', 'Not Assigned', 'Not Treated', 'Unplanned Treatment'), regardless of text case

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

> ARM not in ('Screen Failure', 'Not Assigned'), regardless of text case

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

## prefix_is_contained_by_case_insensitive

_SQL engine only._

Case insensitive `prefix_is_contained_by`

## prefix_is_not_contained_by_case_insensitive

_SQL engine only._

Complement of `prefix_is_contained_by_case_insensitive`

> The first 2 characters of the dataset name are not the name of a dataset in the study, regardless of text case

```yaml
Check:
  all:
    - name: dataset_name
      operator: prefix_is_not_contained_by_case_insensitive
      prefix: 2
      value: $list_dataset_names
Operations:
  - id: $list_dataset_names
    operator: dataset_names
```

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

## suffix_is_contained_by_case_insensitive

_SQL engine only._

Case insensitive `suffix_is_contained_by`

## suffix_is_not_contained_by_case_insensitive

_SQL engine only._

Complement of `suffix_is_contained_by_case_insensitive`

> The last 2 characters of the dataset name are not the name of a dataset in the study, regardless of text case

```yaml
Check:
  all:
    - name: dataset_name
      operator: suffix_is_not_contained_by_case_insensitive
      suffix: 2
      value: $list_dataset_names
Operations:
  - id: $list_dataset_names
    operator: dataset_names
```

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
  value_is_literal: true
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
  value_is_literal: true
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

**SQL engine:** Has optional parameters:

- 'where_populated' when true, only records where `name` and the grouping variables are all populated are checked; the other records return False and are left out of the comparison.
- 'where_populated_columns' a list of variables that must also be populated, in the same way. Variables not in the dataset are ignored.

```yaml
- name: --ELTM
  operator: is_inconsistent_across_dataset
  value:
    - DOMAIN
    - VISITNUM
    - --TPTREF
    - --TPTNUM
  where_populated_columns:
    - --TPT
    - --TPTNUM
    - --ELTM
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

**SQL engine:** `name` cannot be a variable holding a list of columns (it is not expanded). To check the uniqueness of the define.xml key variables, use the `define_key_sequence_is_unique` column of the `Dataset Contents Check against Define XML` rule type.

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

**SQL engine:** `value` can also be a list of variables, whose combined values must have a one-to-one relationship with `name`. Null and empty values are treated as the same value.

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

**SQL engine:** The last record of each group always returns False.

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

**SQL engine:** Gives a result for each record: records in a group whose values of `name` are not in ascending order (in dataset order) return False, the others True. `value` can also be a list of variables. Values are compared as numbers when both are numbers, otherwise as text. Records where `name` is empty return False, also for `is_not_ordered_set`.

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

_Not supported by the SQL engine._

## is_not_ordered_by

Complement of `is_ordered_by`

_Not supported by the SQL engine._

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

**SQL engine:** `within` can also be a list of variables. Has optional parameters:

- 'strict_incremental_ordering' when true, checks instead that within each group the values in `name` are 1, 2, 3, ... (as integers, so `01` is 1) in the order given by `value`. Records where `name` is null return True.
- 'regex' a regular expression whose first capture group is taken from `name` and used as an integer.

> The number at the end of MIDS does not follow the order of SMSTDTC within USUBJID and MIDSTYPE

```yaml
- name: MIDS
  operator: target_is_not_sorted_by
  regex: .*?(\d+$)
  strict_incremental_ordering: true
  value:
    - name: SMSTDTC
      null_position: first
      sort_order: asc
  within:
    - USUBJID
    - MIDSTYPE
```

## target_is_not_sorted_by

Complement of `target_is_sorted_by`

## shares_at_least_one_element_with

Will raise an issue if at least one of the values in `name` is the same as one of the values in `value`

**SQL engine:** The check is made across the whole dataset: all non-empty values of `name` (in any record) are compared with all non-empty values of `value`, and the result is the same for every record.

## shares_exactly_one_element_with

Will raise an issue if exactly one of the values in `name` is the same as one of the values in `value`

**SQL engine:** The check is made across the whole dataset: all non-empty values of `name` (in any record) are compared with all non-empty values of `value`, and the result is the same for every record.

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

**SQL engine:** The check is made across the whole dataset: all non-empty values of `name` (in any record) are compared with all non-empty values of `value`, and the result is the same for every record.

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

_Not supported by the SQL engine._

## value_does_not_have_multiple_references

Complement of `value_has_multiple_references`

_Not supported by the SQL engine._

## is_ordered_subset_of

Checks if elements in the target list appear in the same relative order in the comparator list.

> Check if dataset column order is a correctly ordered subset of library column order

```yaml
- name: $column_order_from_dataset
  operator: is_ordered_subset_of
  value: $column_order_from_library
```

**SQL engine:** `name` and `value` can be lists, `$variable` collections or variables; a variable is read as the sequence of its non-empty values in record order. The result is the same for every record.

## is_not_ordered_subset_of

Complement of `is_ordered_subset_of`

# Define.XML

## conformant_value_data_type

Value Level Metadata Check against Define XML

True if the types in the row match the VLM types specified in the define.xml

_Not supported by the SQL engine._

## non_conformant_value_data_type

Complement of `conformant_value_data_type`

_Not supported by the SQL engine._

## conformant_value_length

Value Level Metadata Check against Define XML

True if the lengths in the row match the VLM lengths specified in the define.xml

_Not supported by the SQL engine._

## non_conformant_value_length

Complement of `conformant_value_length`

_Not supported by the SQL engine._

## references_correct_codelist

True if the codelist named within `value` is a valid codelist for the variable named within `name` in the define.xml.

_Not supported by the SQL engine._

## does_not_reference_correct_codelist

Complement of `references_correct_codelist`

_Not supported by the SQL engine._

# Terminology

## is_extensible_codelist_code

_SQL engine only._

True if the value in `name` is the code of a codelist (e.g. `C66742`) that is extensible in the controlled terminology loaded for the run. `name` can be a variable or a constant `$variable`.

## is_not_extensible_codelist_code

_SQL engine only._

Complement of `is_extensible_codelist_code`

## is*valid*<dictionary>\_<check>

_SQL engine only._

Checks values against an external dictionary loaded for the run (see [External Dictionaries](ExDictionary.md)). There is one operator for each dictionary and check below, and an `is_not_valid_...` complement of each (e.g. `is_not_valid_meddra_code_reference`).

| Dictionary | Term reference                    | Code reference                    | Code-term pair                    |
| ---------- | --------------------------------- | --------------------------------- | --------------------------------- |
| MedDRA     | `is_valid_meddra_term_reference`  | `is_valid_meddra_code_reference`  | `is_valid_meddra_code_term_pair`  |
| WHODrug    | `is_valid_whodrug_term_reference` | `is_valid_whodrug_code_reference` | `is_valid_whodrug_code_term_pair` |
| MED-RT     | `is_valid_medrt_term_reference`   | `is_valid_medrt_code_reference`   | `is_valid_medrt_code_term_pair`   |
| UNII       | `is_valid_unii_term_reference`    | `is_valid_unii_code_reference`    | `is_valid_unii_code_term_pair`    |
| LOINC      | `is_valid_loinc_term_reference`   | `is_valid_loinc_code_reference`   | `is_valid_loinc_code_term_pair`   |
| SNOMED CT  | `is_valid_snomed_term_reference`  | `is_valid_snomed_code_reference`  | `is_valid_snomed_code_term_pair`  |

- Term reference: true if the value in `name` is a term of the dictionary. Dictionary terms containing `,` or `;` are split at those characters, and the value must equal one of the parts.
- Code reference: true if the value in `name` is a code of the dictionary.
- Code-term pair: true if the values in `name` and `value` are the code and the term of the same dictionary entry. The variable whose name ends in `CD` is taken as the code: `value` if it ends in `CD`, otherwise `name`.

Empty values are not valid, so the `is_not_valid_...` operators are true for them; combine with `non_empty` where needed.

For WHODrug the code is a level 4 ATC code and the term is a drug name, and the value `MULTIPLE` is always valid. `is_valid_whodrug_level_reference` (complement `is_not_valid_whodrug_level_reference`) is true if the value in `name` is the text of an ATC class at any of levels 1 to 4.

Has optional parameters (except `is_valid_whodrug_level_reference`):

- 'case_insensitive' when true, values are compared ignoring case.
- 'filter_attribute' and 'filter_value' only use the dictionary entries whose `filter_attribute` is equal to `filter_value`, which can be a constant `$variable`. The attributes are MedDRA `term_type` (`SOC`, `HLGT`, `HLT`, `PT`, `LLT`), WHODrug `level_1` to `level_4`, MED-RT and SNOMED CT `term_tag`, LOINC `version` and `status`. For `version`, entries with a version less than or equal to `filter_value` are used.

> --LLTCD is not a valid MedDRA LLT code

```yaml
- name: --LLTCD
  operator: is_not_valid_meddra_code_reference
  filter_attribute: term_type
  filter_value: LLT
```

> --DECOD and --PTCD are not a valid MedDRA PT code-term pair

```yaml
- name: --DECOD
  operator: is_not_valid_meddra_code_term_pair
  value: --PTCD
  filter_attribute: term_type
  filter_value: PT
```

> CMDECOD is not a WHODrug drug name, regardless of text case

```yaml
- name: CMDECOD
  operator: is_not_valid_whodrug_term_reference
  case_insensitive: true
```

> CMCLAS is not a WHODrug ATC class

```yaml
- name: CMCLAS
  operator: is_not_valid_whodrug_level_reference
```

> --LOINC is not a valid code in the LOINC version given in the define.xml

```yaml
Check:
  all:
    - name: --LOINC
      operator: is_not_valid_loinc_code_reference
      filter_attribute: version
      filter_value: $define_loinc_version
Operations:
  - id: $define_loinc_version
    operator: get_define_external_dictionary_version
    external_dictionary_type: loinc
```

## is_latest_available_external_dictionary_version

_SQL engine only._

True if `version` is the latest version of the `external_dictionary_type` dictionary released on or before the date in `name`. `version` can be a constant `$variable`, a variable or a literal. Release dates are only known for MedDRA (versions 23.1 to 29.0), so `external_dictionary_type` must be `meddra`.

## is_not_latest_available_external_dictionary_version

_SQL engine only._

Complement of `is_latest_available_external_dictionary_version`

> The MedDRA version in the define.xml is not the latest one released at the study start date

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
