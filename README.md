# cdisc-rules-engine (Verisian fork)

This is Verisian's version ("fork") of the open source CDISC Rules Engine (CORE), a tool designed for validating clinical trial data against data standards. It runs the rules with an engine built on a PostgreSQL database.

> **Are you writing or testing rules?** You don't need to set up this repository yourself: follow the [core-contributor](https://github.com/verisianHQ/core-contributor) guide instead, which installs and runs this engine for you.

To learn more about CDISC, visit the official [CDISC Website](https://www.cdisc.org/). 


## Supported python versions

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120)

## Windows Command Compatibility

Note: The Windows commands provided in this README are written for PowerShell. While most commands are compatible with both PowerShell and Command Prompt, some adjustments may be necessary when using Command Prompt. If you encounter any issues running these commands in Command Prompt, try using PowerShell or consult the Command Prompt documentation for equivalent commands.

## **Command-line Interface**

The engine has no graphical interface: you run it by typing commands in a terminal (on Windows, PowerShell; on Mac, the Terminal app; or the terminal built into VSCode, for instance). Unless stated otherwise, every command below must be run from the folder of this repository (the `cdisc-rules-engine` folder created in step 1 below). Use `cd <path>` to move to it.

> On Mac and Linux, if `python` is not found, use `python3` instead (e.g. `python3 core.py --help`).

### **Getting Started**

In the terminal, navigate to the directory you intend to install CORE rules engine in

1. Clone (download) the repository:

   ```
   git clone https://github.com/verisianHQ/cdisc-rules-engine
   ```

2. Ensure you have Python 3.12 installed:
   You can check your Python version with:
   ```
   python --version
   ```
   If you don't have Python 3.12, please download and install it from [python.org](https://www.python.org/downloads/) or using your system's package manager.

### **Installing dependencies**

These steps only need to be done once. They set up a "virtual environment": a private copy of Python for this project, so that the packages the engine needs don't interfere with anything else on your computer.

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

     You need to activate the virtual environment again every time you open a new terminal. When it is active, its name is shown in brackets at the start of the terminal line.

- Install the requirements (from the root directory of the repository):

  `python -m pip install -r requirements-dev.txt`

#### Database

The engine stores the data it validates in a PostgreSQL database while it runs. You don't need to install PostgreSQL yourself: choose one of these two options.

- **Option A: temporary database (simplest)**. Add the `-mem` flag to every `validate` command (see [Running a validation](#running-a-validation)). A temporary database is created for each run and deleted at the end. Nothing else needs to be set up.
- **Option B: Docker**. Use this if you run many validations or want the database to keep running between runs.
  1. Install and start the [Docker Desktop](https://www.docker.com/products/docker-desktop/) app.
  2. Create the two required `.env` files by copying the example files and removing `.example` from the copy's name:
     `./.env.example` → `./.env`
     `./environment/local/.env.example` → `./environment/local/.env`
  3. In `./environment/local/.env`, change `POSTGRES_DATA_DIR` to the folder where the database files should be stored (any empty folder on your computer).
  4. The `DATABASE_*` variables in `./.env` tell the engine how to connect to the database and must match the `POSTGRES_*` values in `./environment/local/.env` (the example files already match, so you can leave them as they are).
  5. Start the database:
     `cd ./environment/local`
     `docker-compose up -d`
     then go back to the root of the repository (`cd ../..`). The database keeps running in Docker Desktop until you stop it there.

### **Running a validation**

Every validation uses this fork's engine, so **always add the `-sql` flag** to `validate` commands, as in the examples below (without it, the older pandas engine of the original CDISC project is used instead).

#### From the command line

Clone the repository and run `python core.py --help` to see the full list of commands.

Run `python core.py validate --help` to see the list of validation options.

```
  -ca, --cache TEXT               Relative path to cache files containing pre
                                  loaded metadata and rules
  -ps, --pool-size INTEGER        Number of parallel processes for validation
                                  (no effect with -sql)
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
                                  (pandas engine only)
  --snomed-url TEXT               Base url of snomed api to use. (ex. https://snowstorm.snomedtools.org/snowstorm/snomed-ct)
                                  (pandas engine only)
  --snomed-edition TEXT           Edition of snomed to use. (ex. SNOMEDCT-US)
                                  (pandas engine only)
  --snomed TEXT                   Path to directory with SNOMED dictionary
                                  files
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
  -sql, --sql-engine              Run the validation with this fork's engine.
                                  Always add this flag (see below)
  -ns, --sql-namespace TEXT       A prefix for all tables in the database when
                                  using the SQL engine
  -mem, --in-memory-postgres      Run validation using an in-memory Postgres
                                  instance
  --help                          Show this message and exit.
```

- `-s` and `-v` are the standard and its version, with the version written with a dash instead of a dot (e.g. `-s sdtmig -v 3-4` for SDTMIG 3.4).
- `-ns` (`--sql-namespace`) sets the prefix used for the database tables (default `uid`).
- XPT and SAS7BDAT files can be mixed in the same run.
- By default the report is written to the folder you run the command from, as an Excel (XLSX) file named `CORE-Report-<date and time>`; use `-o` and `-of` to change this.

##### Available log levels

- `debug` - Display all logs
- `info` - Display info, warnings, and error logs
- `warn` - Display warnings and errors
- `error` - Display only error logs
- `critical` - Display critical logs
- `disabled` - Disable engine logs (default)

##### **Validate folder**

To validate all the datasets in a folder using rules for SDTM-IG version 3.4, with a temporary database, use the following command:

`python core.py validate -s sdtmig -v 3-4 -d path/to/datasets -sql -mem`

(If you set up the Docker database, leave out `-mem`.)

**_NOTE:_** The rules, controlled terminology and other standards metadata are read from the cache in `resources/cache`, which is included in the repository. To refresh it with the latest data from the CDISC Library, see the update-cache command below.

##### **Validate single rule**

`python core.py validate -s sdtmig -v 3-4 -dp <path to dataset json file> -lr <path to rule json file> --meddra ./meddra/ --whodrug ./whodrug/ -sql -mem`
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

> **NOTE:** the `cdisc-rules-engine` package on PyPI is the original CDISC engine, not this fork: it does not include the engine run with `-sql`.

The CDISC Rules Engine is available as a Python package through PyPI. This allows you to:

- Import the rules engine library directly into your Python projects
- Validate data without requiring .xpt format files
- Integrate rules validation into your existing data pipelines

```bash
pip install cdisc-rules-engine
```

For implementation instructions, see [PYPI.md](PYPI.md).

## Development

If you want to change the engine itself (code formatting, running the tests, building an executable or a package), see [DEVELOPMENT.md](DEVELOPMENT.md).

## Submit an Issue

If you encounter any bugs, have feature requests, or need assistance, please submit an issue on our GitHub repository:

[https://github.com/verisianHQ/cdisc-rules-engine/issues](https://github.com/verisianHQ/cdisc-rules-engine/issues)

When submitting an issue, please include:

- A clear description of the problem or request
- Steps to reproduce the issue (for bugs)
- Your operating system and environment details
- Any relevant logs or error messages
