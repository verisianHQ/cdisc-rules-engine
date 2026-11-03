# Development

This page is for developers who intend to change the engine. To install the engine and run validations, see the [README](README.md).

Install the development requirements (they include _requirements.txt_):

`python -m pip install -r requirements-dev.txt`

## **Updating dependencies**

_requirements.txt_ and _requirements-dev.txt_ pin the direct dependencies. _constraints.txt_ pins their indirect dependencies, and _requirements.txt_ applies it with `-c constraints.txt`, so every install gets the same versions.

When you add, remove or bump a package in _requirements.txt_:

1. Make the same change in the `dependencies` list of _pyproject.toml_ (`tests/unit/test_dependencies.py` fails if they differ).
2. Regenerate _constraints.txt_ in a fresh virtual environment, with the `-c constraints.txt` line of _requirements.txt_ temporarily removed so the old pins don't apply:

   ```
   python3.12 -m venv /tmp/deps-venv
   /tmp/deps-venv/bin/python -m pip install -r requirements-dev.txt
   /tmp/deps-venv/bin/python -m pip freeze
   ```

   Replace the pins in _constraints.txt_ with the output, leaving out every package already pinned in _requirements.txt_ or _requirements-dev.txt_, then restore the `-c constraints.txt` line.

## **Code formatter**

This project uses the `black` code formatter, `flake8` linter for python and `prettier` for JSON, YAML and MD.
It also uses `pre-commit` to run `black`, `flake8` and `prettier` when you commit.
`pre-commit`, `black` and `flake8` are included in _requirements-dev.txt_; `prettier` is installed automatically by `pre-commit`.

**Required**

Setting up `pre-commit` requires one extra step. After installing it you have to run

`pre-commit install`

This installs `pre-commit` in your `.git/hooks` directory.

## **Running the tests**

From the root of the project run the following command (the Postgres database must be running for the SQL engine tests: see Option B in the README's Database section):

`python -m pytest tests`

The end-to-end tests in `tests/QARegressionTests` (marked `regression`) are skipped by default. To run only those, add `-m regression`.

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
