# Regression tests:

## Sharepoint Quality Control

- download a copy of the sharepoint unitTesting folder
- run the sharepoint structure validation script against that folder (sp_structure_validation.py). The folder path is hardcoded at the bottom of the script (`~/Data/core-sharepoint`), update it if needed
- fix structural issues

## Rules preparation

- go to CDISC rule editor (https://rule-editor.cdisc.org/)
- remove all filters and get an export
- put it into: `home + "/data/core-rules/dump-2025-09-09.csv"` (or update the path in the notebook)
- run all_rules_prep.ipynb notebook, using the repo's pyenv as the kernel. It writes `sdtm_rules.csv`, `adam_rules.csv` and `define_rules.csv` to a hardcoded `tests/resources/rules/` path, update it to point to your checkout

## Run regression

- set `REGRESSION_PATH` in .env to the downloaded sharepoint unitTesting folder
- make sure the Postgres container is running (see the main README), or run `test_regression::test_regression_all_rules_pgserver` instead of `test_regression_all_rules` to use an in-memory Postgres instance
- run `test_regression::test_regression_all_rules` to update `tests/resources/rules/rules.json`
- run `test_regression::test_regression_single_rule_DEV` to run local regression on a specific rule, as set in .env under `CURRENT_RULE_DEV`. The result is written to `tests/resources/rules/dev/dev.json`.
- run `test_regression::test_regression_single_case_DEV` to run a single test case of that rule, as set in .env under `CURRENT_RULE_DEV_CASE`

e.g. `python -m pytest tests/rule_regression/test_regression.py::test_regression_single_rule_DEV`

## IGNORE ATM

- `remove_xlsx_formatting.py`
