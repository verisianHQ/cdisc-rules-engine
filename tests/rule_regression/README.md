# Rule regression tests

`regression.py` holds helpers that core-contributor's `test.py` uses to load and process test case data, so it must be kept working.

The pytest workflow in `test_regression.py` (with the rule editor export and the SharePoint dump of the CDISC test cases) is outdated. Rule regression testing is now done with `regression_results.py` in [core-contributor](https://github.com/verisianHQ/core-contributor): see its [`.regression/README.md`](https://github.com/verisianHQ/core-contributor/blob/main/.regression/README.md).

On a pull request in this repository that changes the engine (`cdisc_rules_engine/`, `resources/`, `scripts/`, `core.py` or the requirements files), the Rule Regression GitHub Action (`.github/workflows/rule-regression.yml`) can run it on request:

1. When the pull request is opened, or marked ready for review, the action posts a comment asking which rules to run.
2. After reviewing the pull request's code, someone with write access replies `/regression sdtm <commit sha>` or `/regression adam <commit sha>`, where `<commit sha>` is the last commit they reviewed. The run is refused if the pull request has newer commits.
3. The action runs all SDTM or ADaM rules of core-contributor's `main` branch twice, with the engine before the pull request's changes (the point where the pull request branched off its base branch) and with the engine of the pull request, compares the results, and posts the regression summary as a comment. The full summary is attached to the action run.
