# Rule regression tests

`regression.py` holds helpers that core-contributor's `test.py` uses to load and process test case data, so it must be kept working.

The pytest workflow in `test_regression.py` (with the rule editor export and the SharePoint dump of the CDISC test cases) is outdated. Rule regression testing is now done with `regression_results.py` in [core-contributor](https://github.com/verisianHQ/core-contributor): see its [`.regression/README.md`](https://github.com/verisianHQ/core-contributor/blob/main/.regression/README.md).
