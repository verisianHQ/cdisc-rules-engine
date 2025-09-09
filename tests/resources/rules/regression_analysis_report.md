# CDISC Rules Engine Regression Analysis

=============================================

## Missing Operators (14 operators, 195 total failures across 52 rule occurrences)

1.  **longer_than**: 66 failures across 12 rules
2.  **is_unique_set**: 43 failures across 12 rules
3.  **not_matches_regex**: 30 failures across 12 rules
4.  **matches_regex**: 21 failures across 5 rules
5.  **longer_than_or_equal_to**: 9 failures across 1 rules
6.  **invalid_duration**: 4 failures across 1 rules
7.  **is_inconsistent_across_dataset**: 4 failures across 1 rules
8.  **invalid_date**: 4 failures across 2 rules
9.  **has_equal_length**: 3 failures across 1 rules
10. **starts_with**: 3 failures across 1 rules
11. **ends_with**: 2 failures across 1 rules
12. **has_next_corresponding_record**: 2 failures across 1 rules
13. **empty_within_except_last_row**: 2 failures across 1 rules
14. **prefix_equal_to**: 2 failures across 1 rules

## Missing Operations (11 operations, 314 total failures across 21 rule occurrences)

1.  **variable_exists**: 227 failures across 4 rules
2.  **variable_count**: 20 failures across 2 rules
3.  **dataset_names**: 20 failures across 2 rules
4.  **dy**: 16 failures across 3 rules
5.  **domain_label**: 8 failures across 2 rules
6.  **max_date**: 5 failures across 2 rules
7.  **get_model_column_order**: 5 failures across 1 rules
8.  **domain_is_custom**: 4 failures across 1 rules
9.  **extract_metadata**: 4 failures across 1 rules
10. **min_date**: 3 failures across 2 rules
11. **valid_codelist_dates**: 2 failures across 1 rules

## SQL vs Old Engine Discrepancies

### SQL Errors where Old Engine Skipped (206 cases)

_Indicates SQL engine running rules it shouldn't_

- [58] A postgres SQL error occurred
- [22] is_unique_set check_operator not implemented
- [18] longer_than check_operator not implemented
- [13] not_matches_regex check_operator not implemented
- [6] matches_regex check_operator not implemented
- [5] Operation max_date is not implemented
- [5] Rule contains invalid operator
- [4] Column AGETXT does not exist in the table dm.
- [4] Operation variable_exists is not implemented
- [4] invalid_duration check_operator not implemented

### SQL Success where Old Engine Skipped (211 cases)

_Indicates SQL engine not respecting rule applicability_
**Skip Types:**

- Class Not Applicable: 157
- Domain Not Applicable: 54

**Examples:**

- [4] Rule skipped - doesn't apply to class for rule id=CORE-00033...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00046...
- [4] Rule skipped - doesn't apply to class for rule id=CORE-00056...
- [3] Rule skipped - doesn't apply to class for rule id=CORE-00017...
- [3] Rule skipped - doesn't apply to class for rule id=CORE-00025...

### SQL Errors where Old Engine Succeeded (129 cases)

_Indicates actual regressions in SQL implementation_

- [19] Operation variable_exists is not implemented
- [17] is_unique_set check_operator not implemented
- [17] not_matches_regex check_operator not implemented
- [14] A postgres SQL error occurred
- [13] longer_than check_operator not implemented
- [9] Operation dataset_names is not implemented
- [8] Operation variable_count is not implemented
- [6] Operation dy is not implemented
- [4] is_inconsistent_across_dataset check_operator not implemente...
- [3] matches_regex check_operator not implemented

## Other Execution Errors (57 unique messages, 215 total)

- [ 99] A postgres SQL error occurred
- [ 21] Rule contains invalid operator
- [ 4] Column AGETXT does not exist in the table dm.
- [ 4] invalid input syntax for type double precision: "TV.VISITDY"
  LINE 1: ....
- [ 3] Column $trt_count does not exist in the table ts.
- [ 3] Column $stype_interventional does not exist in the table ts.
- [ 2] Column ecstat does not exist in the table ec.
- [ 2] Column ECSTAT does not exist in the table ec.
- [ 2] Column vsdrvfl does not exist in the table vs.
- [ 2] Column AGDOSTOT does not exist in the table ag.
- [ 2] Column ECDOSTOT does not exist in the table ec.
- [ 2] Column EXDOSTOT does not exist in the table ex.
- [ 2] Column MLDOSTOT does not exist in the table ml.
- [ 2] Column PRDOSTOT does not exist in the table pr.
- [ 2] Column IDVAR does not exist in the table co.
