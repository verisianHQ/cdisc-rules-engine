# cdisc-rules-engine

Open source offering of the CDISC Rules Engine, a tool designed for validating clinical trial data against data standards.
To learn more, visit our official CDISC website:  
<br>  
[CDISC Website](https://www.cdisc.org/)

## Supported python versions

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120)

## Windows Command Compatibility

Note: The Windows commands provided in this README are written for PowerShell. While most commands are compatible with both PowerShell and Command Prompt, some adjustments may be necessary when using Command Prompt. If you encounter any issues running these commands in Command Prompt, try using PowerShell or consult the Command Prompt documentation for equivalent commands.

## **Command-line Interface**

### **Getting Started**

In the terminal, navigate to the directory you intend to install CORE rules engine in

1. Clone the repository:

   ```
   git clone https://github.com/verisianHQ/cdisc-rules-engine
   ```

2. Ensure you have Python 3.12 installed:
   You can check your Python version with:
   ```
   python --version
   ```
   If you don't have Python 3.12, please download and install it from [python.org](https://www.python.org/downloads/) or using your system's package manager.

### **Code formatter**

This project uses the `black` code formatter, `flake8` linter for python and `prettier` for JSON, YAML and MD.
It also uses `pre-commit` to run `black`, `flake8` and `prettier` when you commit.
`pre-commit` is included in _requirements.txt_ and `black` and `flake8` in _requirements-dev.txt_; `prettier` is installed automatically by `pre-commit`.

**Required**

Setting up `pre-commit` requires one extra step. After installing it you have to run

`pre-commit install`

This installs `pre-commit` in your `.git/hooks` directory.

### **Installing dependencies**

These steps should be run before running any tests or core commands using the non-compiled version.

- **Option 1: Using `pyenv` (Recommended)**

  This approach is ideal for managing multiple projects that require different Python versions.

  1. **Install the required Python version:**

     ```bash
     pyenv install 3.12.5
     ```

  2. **Create and activate a virtual environment for this project:**
     ```bash
     pyenv virtualenv 3.12.5 venv_cdisc
     pyenv local venv_cdisc
     ```
     This will create a new virtual environment named `venv_cdisc` based on Python 3.12.5 and automatically activate it for this project directory.

- **Option 2: Using standard `venv`**

  This approach works if you have Python 3.12 as your default system version or if you prefer to manage virtual environments manually.

  1. **Create a virtual environment:**
     `python -m venv <virtual_environment_name>`

     NOTE: if you have multiple versions of python on your machine, you can call python 3.12 for the virtual environment's creation instead of the above command:
     `python3.12 -m venv <virtual_environment_name>`

  2. **Activate the virtual environment:**
     `source ./<virtual_environment_name>/bin/activate` -- on Linux/Mac
     `.\<virtual_environment_name>\Scripts\Activate` -- on Windows

- Create the required `.env` files by duplicating the example files:
  `./.env.example`
  `./environment/local/.env.example`
- In ./environment/local/.env, update the POSTGRES_DATA_DIR variable to point to the location where you want your Postgres database files to be stored.
- The `DATABASE_*` variables in `./.env` are used by the engine to connect to Postgres and must match the `POSTGRES_*` values in `./environment/local/.env` (the example files already match).

- Ensure you have the docker desktop app running and then run these commands to set-up docker for this environment:
  `cd ./environment/local`
  `docker-compose up -d`

- Install the requirements.

`python -m pip install -r requirements.txt` # From the root directory

To run the tests or use the development tools (`pytest`, `black`, `flake8`, `pgserver`), install the development requirements instead (this includes _requirements.txt_):

`python -m pip install -r requirements-dev.txt`

### **Running The Tests**

From the root of the project run the following command (the Postgres container from the previous section must be running for the SQL engine tests):

`python -m pytest tests --ignore=tests/rule_regression`

Tests marked as `regression` are skipped by default. To run them, add `-m regression`.

The tests in `tests/rule_regression` run every rule against the CDISC SharePoint test cases and need extra setup; see [tests/rule_regression/README.md](tests/rule_regression/README.md).

### **Running a validation**

#### From the command line

Clone the repository and run `python core.py --help` to see the full list of commands.

Run `python core.py validate --help` to see the list of validation options.

```
  -ca, --cache TEXT               Relative path to cache files containing pre
                                  loaded metadata and rules
  -ps, --pool-size INTEGER        Number of parallel processes for validation
  -d, --data TEXT                 Path to directory containing data files
  -dp, --dataset-path TEXT        Absolute path to dataset file. Can be specified multiple times.
  -l, --log-level [info|debug|error|critical|disabled|warn]
                                  Sets log level for engine logs, logs are
                                  disabled by default
  -rt, --report-template TEXT     File path of report template to use for
                                  excel output
  -s, --standard TEXT             CDISC standard to validate against
                                  [required]
  -v, --version TEXT              Standard version to validate against
                                  [required]
  -ss, --substandard TEXT         Substandard to validate against
                                  [required for TIG]
  -ct, --controlled-terminology-package TEXT
                                  Controlled terminology package to validate
                                  against, can provide more than one
                                  NOTE: if a defineXML is provided, if it is version 2.1
                                  engine will use the CT laid out in the define.  If it is
                                  version 2.0, -ct is expected to specify the CT package
  -o, --output TEXT               Report output file destination and name. Path will be
                                  relative to the validation execution directory
                                  and should end in the desired output filename
                                  without file extension
                                  '/user/reports/result' will be 'user/reports' directory
                                  with the filename as 'result'
  -of, --output-format [JSON|XLSX]
                                  Output file format. Can be specified multiple times.
  -rr, --raw-report               Report in a raw format as it is generated by
                                  the engine. This flag must be used only with
                                  --output-format JSON.
  -dv, --define-version [2-1|2-0|2.0|2.1]
                                  Define-XML version used for validation
  --whodrug TEXT                  Path to directory with WHODrug dictionary
                                  files
  --meddra TEXT                   Path to directory with MedDRA dictionary
                                  files
  --loinc TEXT                    Path to directory with LOINC dictionary
                                  files
  --medrt TEXT                    Path to directory with MEDRT dictionary
                                  files
  --unii TEXT                     Path to directory with UNII dictionary
                                  files
  --snomed-version TEXT           Version of snomed to use. (ex. 2024-09-01)
  --snomed-url TEXT               Base url of snomed api to use. (ex. https://snowstorm.snomedtools.org/snowstorm/snomed-ct)
  --snomed-edition TEXT           Edition of snomed to use. (ex. SNOMEDCT-US)
  --snomed TEXT                   Path to directory with SNOMED dictionary
                                  files (SQL engine only)
  -r, --rules TEXT                Specify rule core ID ex. CORE-000001. Can be specified multiple times.
  -lr, --local_rules PATH         Specify relative path to directory or file containing
                                  local rule yml and/or json rule files.
  -cs, --custom_standard          Adding this flag tells engine to use a custom standard specified with -s and -v
                                  that has been uploaded to the cache using update-cache
  -p, --progress [bar|verbose_output|percents|disabled]
                                  Defines how to display the validation
                                  progress. By default a progress bar like
                                  "[████████████████████████████--------]
                                  78%"is printed. verbose_output prints rules
                                  as they are completed.
  -dxp, --define-xml-path TEXT    Path to Define-XML
  -stf, --stf-file-path TEXT      Path to STF file
  -vx, --validate-xml TEXT        Enable XML validation (default 'y' to enable, otherwise disable)
  -sql, --sql-engine              Flag to run validation using the SQL engine
  -ns, --sql-namespace TEXT       A prefix for all tables in the database when
                                  using the SQL engine
  -mem, --in-memory-postgres      Run validation using an in-memory Postgres
                                  instance
  --help                          Show this message and exit.
```

##### Available log levels

- `debug` - Display all logs
- `info` - Display info, warnings, and error logs
- `warn` - Display warnings and errors
- `error` - Display only error logs
- `critical` - Display critical logs
- `disabled` - Disable engine logs (default)

##### **Validate folder**

To validate a folder using rules for SDTM-IG version 3.4 use the following command:

`python core.py validate -s sdtmig -v 3-4 -d path/to/datasets`

**_NOTE:_** Before running a validation in the CLI, you must first populate the cache with rules to validate against. See the update-cache command below.

##### **Validate single rule**

`python core.py validate -s sdtmig -v 3-4 -dp <path to dataset json file> -lr <path to rule json file> --meddra ./meddra/ --whodrug ./whodrug/`
Note: JSON dataset should match the format provided by the rule editor:

```json
{
  "datasets": [
    {
      "filename": "cm.xpt",
      "label": "Concomitant/Concurrent medications",
      "domain": "CM",
      "variables": [
        {
          "name": "STUDYID",
          "label": "Study Identifier",
          "type": "Char",
          "length": 10
        }
      ],
      "records": {
        "STUDYID": ["CDISC-TEST", "CDISC-TEST", "CDISC-TEST", "CDISC-TEST"]
      }
    }
  ]
}
```

##### **Validate using the SQL engine**

This fork also includes a PostgreSQL-based SQL engine. Add the `-sql` flag to run a validation with it instead of the default engine:

`python core.py validate -s sdtmig -v 3-4 -d path/to/datasets -sql`

- By default the SQL engine connects to the Postgres database configured by the `DATABASE_*` variables in `./.env` (see the docker set-up in [Installing dependencies](#installing-dependencies)).
- Add `-mem` (`--in-memory-postgres`) to use a temporary in-memory Postgres instance instead. This requires the `pgserver` package from _requirements-dev.txt_.
- `-ns` (`--sql-namespace`) sets the prefix used for the database tables (default `uid`).
- The SQL engine accepts a mix of XPT and SAS7BDAT files in the same run, and uses `--snomed` (a path to SNOMED dictionary files) instead of the `--snomed-version`, `--snomed-edition` and `--snomed-url` options.
- The SQL engine runs rules one at a time, so `-ps` has no effect.

##### **Understanding the Rules Report**

The rules report tab displays the run status of each rule selected for validation

The possible rule run statuses are:

- `SUCCESS` - The rule ran and data was validated against the rule. May or may not produce results
- `SKIPPED` - The rule was unable to be run. Usually due to missing required data, but could also be caused by rule execution errors.

## Additional Core Commands

**- update-cache** - update locally stored cache data (Requires an environment variable - `CDISC_LIBRARY_API_KEY`, or the `--apikey` option) This is stored in the `.env` file in the root directory, the API key does not need quotations around it.

```bash
  python core.py update-cache
```

**NOTE:** When running a validation, CORE uses rules in the cache unless -lr is specified. Running the above command populates the cache with controlled terminology, rules, metadata, etc.

To obtain an api key, please follow the instructions found here: <https://wiki.cdisc.org/display/LIBSUPRT/Getting+Started%3A+Access+to+CDISC+Library+API+using+API+Key+Authentication>. Please note it can take up to an hour after sign up to have an api key issued

**- version** - print the version of the engine:

```bash
python core.py version
```

**- list-dataset-metadata** - list the metadata (domain, filename, full path, file size, label, modification date) of the given datasets:

```bash
python core.py list-dataset-metadata -dp path/to/ae.xpt -dp path/to/ex.xpt
```

## Custom Standards and Rules

### Custom Rules Storage

- **Custom rules** are stored in a flat file in the cache, indexed by their core ID (e.g., 'COMPANY-000123' or 'CUSTOM-000123').
- Each rule is stored independently in this file, allowing for efficient lookup and management.

### Custom Standards Management

- **Custom standards** act as a lookup mechanism that maps a standard identifier to a list of applicable rule IDs.
- When adding a custom standard, you need to provide a JSON file with the following structure:

  ```json
  {
    "standard_id/version": ["RULE_ID1", "RULE_ID2", "RULE_ID3", ...]
  }
  ```

  For example:

  ```json
  {
    "cust_standard/1-0": [
      "CUSTOM-000123",
      "CUSTOM-000456",
      "CUSTOM-001",
      "CUSTOM-002"
    ]
  }
  ```

- To add or update a custom standard, use:

  ```bash
  python core.py update-cache --custom_standard 'path/to/standard.json'
  ```

- To remove custom standards, use the `--remove_custom_standard` or `-rcs` flag:

  ```bash
  python core.py update-cache --remove_custom_standard 'mycustom/1-0'
  ```

- When executing validation against a custom standard, the system will use the standard as a lookup to determine which rules to apply from the rule cache. Custom standards which match CDISC standard names and versions can be used to get library metadata for the standard while still utilizing custom rules. If a custom name does not match a CDISC standard, library metadata will not be populated.

  ```json
  {
    "sdtmig/3-4": ["CUSTOM-000123", "CUSTOM-000456", "CUSTOM-001", "CUSTOM-002"]
  }
  ```

  This rule will get metadata from SDTMIG version 3.4 but utilize the custom rules listed in the custom standard that need this library metadata.

### Relationship Between Custom Rules and Standards

- You should first add your custom rules to the cache, then create a custom standard that references those rules.
- Custom standards can reference both core CDISC rules and your own custom rules in the same standard definition.
- This two-level architecture allows for flexible rule reuse across multiple standards.

### Custom Rules Management

- **Add custom rules**: Use the `--custom_rules_directory` or `-crd` flag to specify a directory containing local rules, or `--custom_rule` or `-cr` flag to specify a single rule file:
  ```bash
  python core.py update-cache --custom_rules_directory 'path/to/directory'
  python core.py update-cache --custom_rule 'path/to/rule.json' --custom_rule 'path/to/rule.yaml'
  ```
- **Update a custom rule**: Use the `--update_custom_rule` or `-ucr` flag to update an existing rule in the cache:

  ```bash
  python core.py update-cache --update_custom_rule 'path/to/updated_rule.yaml'
  ```

- **Remove custom rules**: Use the `--remove_custom_rules` or `-rcr` flag to remove rules from the cache. Can be a single rule ID, a comma-separated list of IDs, or ALL to remove all custom rules:
  ```bash
  python core.py update-cache --remove_custom_rules 'RULE_ID'
  python core.py update-cache --remove_custom_rules 'RULE_ID1,RULE_ID2,RULE_ID3'
  python core.py update-cache --remove_custom_rules ALL
  ```

## List Commands

**- list-rules** - list published rules available in the cache

- list all published rules:

      `python core.py list-rules`

- list rules for standard:

      `python core.py list-rules -s sdtmig -v 3-4`

- list rules for integrated standard (substandard: "SDTM", "SEND", "ADaM", "CDASH"):

      `python core.py list-rules -s tig -v 1-0 -ss SDTM`

- list rules by ID:

      `python core.py list-rules -r CORE-000351 -r CORE-000591`

- List all custom rules:

  ```bash
  python core.py list-rules --custom_rules
  ```

- List custom rules for a custom standard:
  ```bash
  python core.py list-rules --custom_rules -s custom_standard -v 1-0
  ```

**- list-rule-sets** - lists all standards and versions for which rules are available:

```bash
python core.py list-rule-sets
```

To list custom standards and versions instead:

```bash
python core.py list-rule-sets --custom
# or using the short form:
python core.py list-rule-sets -o
```

**Options:**

- `-c, --cache_path` - Relative path to cache files containing pre-loaded metadata and rules
- `-o, --custom` - Flag to list all custom standards and versions in the cache instead of CDISC standards & rules

**- list-ct** - list ct packages available in the cache

```
Usage: python core.py list-ct [OPTIONS]

  Command to list the ct packages available in the cache.

Options:
  -c, --cache_path TEXT  Relative path to cache files containing pre loaded
                         metadata and rules
  -s, --subsets TEXT     CT package subset type. Ex: sdtmct. Multiple values
                         allowed
  --help                 Show this message and exit.
```

## PyPI Integration

The CDISC Rules Engine is available as a Python package through PyPI. This allows you to:

- Import the rules engine library directly into your Python projects
- Validate data without requiring .xpt format files
- Integrate rules validation into your existing data pipelines

```bash
pip install cdisc-rules-engine
```

For implementation instructions, see [PYPI.md](PYPI.md).

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

**Unix/MacOS**

`python3 -m pip install --upgrade build`
`python3 -m build`

To install from dist folder
`pip3 install {path_to_file}/cdisc_rules_engine-{version}-py3-none-any.whl`

To upload built distributive to pypi

`python3 -m pip install --upgrade twine`
`python3 -m twine upload --repository {repository_name} dist/*`

**Windows(Untested)**

`py -m pip install --upgrade build`
`py -m build`

To install from dist folder
`pip install {path_to_file}/cdisc_rules_engine-{version}-py3-none-any.whl`

To upload built distributive to pypi

`py -m pip install --upgrade twine`
`py -m twine upload --repository {repository_name} dist/*`

## Submit an Issue

If you encounter any bugs, have feature requests, or need assistance, please submit an issue on our GitHub repository:

[https://github.com/verisianHQ/cdisc-rules-engine/issues](https://github.com/verisianHQ/cdisc-rules-engine/issues)

When submitting an issue, please include:

- A clear description of the problem or request
- Steps to reproduce the issue (for bugs)
- Your operating system and environment details
- Any relevant logs or error messages
