# External Dictionaries

External dictionaries provide standardized terminology and coding systems for medical terms, drugs, and laboratory tests. This guide covers how to provide the dictionaries to the engine from the Command Line Interface (CLI) and how rules check values against them.

The dictionaries are loaded into the database at the start of the run, and a dictionary that cannot be read stops the run. Rules check dictionary values with the terminology operators (e.g. `is_valid_meddra_term_reference`, see [Operator](Operator.md#terminology)). Of the operations in [Operations & Rule Editor](#operations--rule-editor), only `whodrug_code_hierarchy` is supported by this engine; the others are only supported by the pandas engine.

## Supported Dictionaries

- **MedDRA**: Medical terminology for regulatory activities
- **WHODrug**: Global drug reference dictionary
- **LOINC**: Laboratory test terminology
- **MEDRT**: Medication Reference Terminology
- **UNII**: Unique Ingredient Identifier
- **SNOMED CT**: Clinical healthcare terminology

## CLI Usage

> **IMPORTANT**: All dictionary paths must point to the _directory containing_ the dictionary files, not to specific files.

### Medical Terminology Dictionaries

#### MedDRA

```
--meddra TEXT      Path to directory containing MedDRA dictionary files
                   Required files in directory:
                    Term files (*.asc format, only the code and name - the first two fields - are read):
                        - pt.asc (Preferred Terms)
                        - llt.asc (Lowest Level Terms)
                        - hlt.asc (High Level Terms)
                        - hlgt.asc (High Level Group Terms)
                        - soc.asc (System Organ Classes)
                    Version file:
                        - meddra_release.asc containing version number followed by language and additional fields
                            Example: 27.0$English$$$$
```

In the pandas engine, the relationship files `soc_hlgt.asc`, `hlgt_hlt.asc` and `hlt_pt.asc` are also required, and `meddra_release.asc` is optional (the version is left empty if it is missing).

#### SNOMED CT

```
--snomed TEXT      Path to directory (searched with its subdirectories) containing the SNOMED CT RF2 files:
                     - sct2_Description_Full-<language>_<edition>_<date>.txt
                     - sct2_TextDefinition_Full-<language>_<edition>_<date>.txt
                       (e.g. sct2_Description_Full-en_INT_20240901.txt; the edition code
                       must contain capital letters only)
                   The version is the date in the file names (e.g. 20240901)
```

Browse terms at [SNOMED Browser](https://browser.ihtsdotools.org).

In the pandas engine, SNOMED CT is read from an API instead, using `--snomed-version` (e.g. `2024-09-01`), `--snomed-url` (default: https://snowstorm.snomedtools.org/snowstorm/snomed-ct/) and `--snomed-edition` (e.g. `SNOMEDCT-US`). API access requires hosting your own instance using [Snowstorm](https://github.com/IHTSDO/snowstorm) or an alternative implementation, as https://snowstorm.snomedtools.org/snowstorm/snomed-ct is not for commercial use.

### Drug and Substance Dictionaries

#### WHODrug

```
--whodrug TEXT     Path to directory containing WHODrug dictionary files
                   Required files in directory (comma-separated, without a header row):
                     - Version.csv, whose first value gives the product, format and date
                       (e.g. "WHODRUG GLOBAL B3 September 1, 2020")
                     - B3 format: INA.csv, DDA.csv and DD.csv
                     - C3 format: ATC.csv, ThG.csv and MP.csv
```

In the pandas engine, the files are `DD.txt` (Drug Dictionary), `DDA.txt` (ATC Classification) and `INA.txt` (ATC Text), with an optional `version.txt` in the Vault Safety label format (e.g. `GLOBALC3Mar24`).

#### Other Drug Dictionaries

```
--medrt TEXT      Path to directory containing MED-RT dictionary files
                  Required files in directory:
                    - MEDRT.txt (tab-separated: "Name [TAG]", code)
                    - MEDRT_Release_Notes.txt (the version is read from its first line,
                      e.g. "September 2024" from "September 2024 MED-RT (version name 2024.09.03)")

--unii TEXT       Path to directory containing UNII dictionary files
                  Required files in directory:
                    - UNII_Records_*.txt (tab-delimited file containing UNII codes and terms)
                  Format: Tab-delimited file with a header row and the following columns:
                    1. UNII code
                    2. UNII term
                  Note: Version is extracted from filename (e.g., "UNII_Records_2024.txt" → version "2024")
```

In the pandas engine, MED-RT is read from a `Core_MEDRT_*_DTS.xml` file containing term and concept elements, and its version from the `<namespace><version>` element.

### Laboratory Dictionaries

```
--loinc TEXT      Path to directory containing LOINC dictionary files
                  The directory (or its LoincTable subdirectory) must contain `Loinc.csv` with capital 'L'
                  Name the directory Loinc_<version> (e.g. Loinc_2.82): the version is the part of
                  the directory name after the last underscore. If the name has no underscore, the
                  version is taken from the Loinc_<version>_DifferenceReport.pdf file name, or is
                  "unknown" if there is no such file
```

In the pandas engine, the version is the highest `VersionLastChanged` value in the file.

## Operations & Rule Editor

### Dictionary Version Validation

#### valid_define_external_dictionary_version

_Not supported by this engine._

Validates dictionary versions against define.xml specifications.

```yaml
Operations:
  - operator: valid_define_external_dictionary_version
    id: $is_valid_loinc_version
    external_dictionary_type: loinc
```

### Value and Code Validation

#### valid_external_dictionary_value

_Not supported by this engine._

Validates dictionary values with optional case sensitivity.

```yaml
Operations:
  - operator: valid_external_dictionary_value
    name: --DECOD
    id: $is_valid_decod_value
    external_dictionary_type: meddra
    dictionary_term_type: PT
    case_sensitive: false
```

#### valid_external_dictionary_code

_Not supported by this engine._

Validates dictionary codes.

```yaml
Operations:
  - operator: valid_external_dictionary_code
    name: --COD
    id: $is_valid_cod_code
    external_dictionary_type: meddra
    dictionary_term_type: PT
```

#### valid_external_dictionary_code_term_pair

_Not supported by this engine._

Validates matching of code-term pairs.

```yaml
Operations:
  - operator: valid_external_dictionary_code_term_pair
    name: --COD
    id: $is_valid_loinc_code_term_pair
    external_dictionary_type: loinc
    external_dictionary_term_variable: --DECOD
```

### MedDRA-Specific Operations

#### valid_meddra_code_references

_Not supported by this engine._

Validates MedDRA codes across all levels:

- `--SOCCD` (System Organ Class Code)
- `--HLGTCD` (High Level Group Term Code)
- `--HLTCD` (High Level Term Code)
- `--PTCD` (Preferred Term Code)
- `--LLTCD` (Lowest Level Term Code)

Example:

```yaml
Operations:
  - id: $is_valid_meddra_codes
    operator: valid_meddra_code_references
```

#### valid_meddra_code_term_pairs

_Not supported by this engine._

Validates the code-term pair containing the variable given in `name`. Supported pairs:

- `--SOCCD`, `--SOC` (System Organ Class Code and Term)
- `--HLGTCD`, `--HLGT` (High Level Group Term Code and Term)
- `--HLTCD`, `--HLT` (High Level Term Code and Term)
- `--PTCD`, `--DECOD` (Preferred Term Code and Dictionary-Derived Term)
- `--LLTCD`, `--LLT` (Lowest Level Term Code and Term)

Example:

```yaml
Operations:
  - id: $is_valid_meddra_pairs
    name: --DECOD
    operator: valid_meddra_code_term_pairs
```

#### valid_meddra_term_references

_Not supported by this engine._

Validates terms at each MedDRA level:

- `--SOC` (System Organ Class)
- `--HLGT` (High Level Group Term)
- `--HLT` (High Level Term)
- `--DECOD` (Dictionary-Derived Term)
- `--LLT` (Lowest Level Term)

Example:

```yaml
Operations:
  - id: $is_valid_meddra_terms
    operator: valid_meddra_term_references
```

### WHODrug-Specific Operations

#### valid_whodrug_references

_Not supported by this engine._

Validates that the values of the variable given in `name` exist as codes in the ATC Text (INA) file.

Example:

```yaml
Operations:
  - id: $whodrug_refs_valid
    name: --CLASCD
    operator: valid_whodrug_references
```

#### whodrug_code_hierarchy

Validates hierarchical relationships between:

- `--DECOD`
- `--CLAS`
- `--CLASCD`

Example:

```yaml
Operations:
  - id: $valid_whodrug_codes
    operator: whodrug_code_hierarchy
```
