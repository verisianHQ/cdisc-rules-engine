# Development

This page is for developers who intend to change the engine. To install the engine and run validations, see the [README](README.md).

Install the development requirements (they include _requirements.txt_):

`python -m pip install -r requirements-dev.txt`

## **Code formatter**

This project uses the `black` code formatter, `flake8` linter for python and `prettier` for JSON, YAML and MD.
It also uses `pre-commit` to run `black`, `flake8` and `prettier` when you commit.
`pre-commit` is included in _requirements.txt_ and `black` and `flake8` in _requirements-dev.txt_; `prettier` is installed automatically by `pre-commit`.

**Required**

Setting up `pre-commit` requires one extra step. After installing it you have to run

`pre-commit install`

This installs `pre-commit` in your `.git/hooks` directory.

## **Running the tests**

From the root of the project run the following command (the Postgres database must be running for the SQL engine tests: see Option B in the README's Database section):

`python -m pytest tests --ignore=tests/rule_regression`

The end-to-end tests in `tests/QARegressionTests` (marked `regression`) are skipped by default. To run only those, add `-m regression`.

The tests in `tests/rule_regression` need a rule editor export and a SharePoint dump of the CDISC test cases, so they are left out of the command above (see [tests/rule_regression/README.md](tests/rule_regression/README.md)).

## **Creating an executable version**

**Linux**

`pyinstaller core.py --add-data=venv/lib/python3.12/site-packages/xmlschema/schemas:xmlschema/schemas --add-data=resources/cache:resources/cache --add-data=resources/templates:resources/templates --add-data=resources/schema:resources/schema`

**Windows**

`pyinstaller core.py --add-data=".venv/Lib/site-packages/xmlschema/schemas;xmlschema/schemas" --add-data="resources/cache;resources/cache" --add-data="resources/templates;resources/templates" --add-data="resources/schema;resources/schema"`

_Note .venv should be replaced with path to python installation or virtual environment_

This will create an executable version in the `dist` folder. The version does not require having Python installed and
can be launched by running `core` script with all necessary CLI arguments.

## **Creating .whl file**

All non-python files should be listed in `MANIFEST.in` to be included in the distribution.
Files must be in python package.

**Unix / MacOS**

`python3 -m pip install --upgrade build`
`python3 -m build`

To install from dist folder
`pip3 install {path_to_file}/cdisc_rules_engine-{version}-py3-none-any.whl`

To upload built distributive to pypi

`python3 -m pip install --upgrade twine`
`python3 -m twine upload --repository {repository_name} dist/*`

**Windows (untested)**

`py -m pip install --upgrade build`
`py -m build`

To install from dist folder
`pip install {path_to_file}/cdisc_rules_engine-{version}-py3-none-any.whl`

To upload built distributive to pypi

`py -m pip install --upgrade twine`
`py -m twine upload --repository {repository_name} dist/*`
