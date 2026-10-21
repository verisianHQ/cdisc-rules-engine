# External Dictionaries

External dictionaries provide standardized terminology and coding systems for medical terms, drugs, and laboratory tests. This guide covers how to provide the dictionaries to the engine from the Command Line Interface (CLI) and how rules check values against them.

The dictionaries are loaded into the database at the start of the run, and a dictionary that cannot be read stops the run. How rules check values against them is described in [Operations & Rule Editor](#operations--rule-editor).

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

### Laboratory Dictionaries

```
--loinc TEXT      Path to directory containing LOINC dictionary files
                  The directory (or its LoincTable subdirectory) must contain `Loinc.csv` with capital 'L'
                  Name the directory Loinc_<version> (e.g. Loinc_2.82): the version is the part of
                  the directory name after the last underscore. If the name has no underscore, the
                  version is taken from the Loinc_<version>_DifferenceReport.pdf file name, or is
                  "unknown" if there is no such file
```

## Operations & Rule Editor

Rules check dictionary values with the terminology operators, such as `is_valid_meddra_term_reference` (see [Operator](Operator.md#terminology)). These operations are also available:

- `get_define_external_dictionary_version` and `get_external_dictionary_version` return the dictionary version given in the define.xml and the one read from the dictionary files (see [Operations](Operations.md#get_define_external_dictionary_version)).
- `whodrug_code_hierarchy`, described below.

### whodrug_code_hierarchy

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
