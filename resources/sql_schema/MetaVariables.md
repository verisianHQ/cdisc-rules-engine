## dataset_label

Label for the dataset

## dataset_location

Path to file

## dataset_name

Name of the dataset

## dataset_size

File size

## define_dataset_class

ItemGroupDef.Class.Name

## define_dataset_is_non_standard

ItemGroupDef.IsNonStandard

## define_dataset_key_sequence

[ItemGroupDef/ValueListDef].ItemRef.KeySequence

## define_dataset_label

ItemGroupDef.Description.TranslatedText

## define_dataset_location

ItemGroupDef.leaf.href

## define_dataset_name

ItemGroupDef.Name

## define_dataset_structure

ItemGroupDef.Structure

## define_variable_allowed_terms

ItemGroupDef.ItemDef.CodeList.CodeListItem.Decode.TranslatedText

## define_variable_ccode

ItemGroupDef.ItemDef.CodeList.Alias.Name

## define_variable_codelist_coded_values

ItemGroupDef.ItemDef.CodeList.[CodeListItem/EnumeratedItem].CodedValue

## define_variable_codelist_coded_codes

ItemGroupDef.ItemDef.CodeList.[CodeListItem/EnumeratedItem].Alias.Name

## define_variable_data_type

ItemGroupDef.ItemDef.DataType

## define_variable_format

[Not Implemented]

## define_variable_has_codelist

ItemGroupDef.ItemDef.CodeListRef exists

## define_variable_has_comment

ItemGroupDef.ItemDef.CommentOID exists

## define_variable_has_no_data

ItemGroupDef.ItemRef.HasNoData

## define_variable_is_collected

ItemGroupDef.ItemDef.Origin.Type = \"Collected\" (2.1) or \"CRF\" (2.0)

## define_variable_label

ItemGroupDef.ItemDef.Description.TranslatedText

## define_variable_length

ItemGroupDef.ItemDef.Length

## define_variable_mandatory

ItemGroupDef.ItemRef.Mandatory

## define_variable_name

ItemGroupDef.ItemDef.Name

## define_variable_order_number

ItemGroupDef.ItemRef.OrderNumber

## define_variable_origin_type

ItemGroupDef.ItemDef.Origin.Type

## define_variable_role

ItemGroupDef.ItemRef.Role

## define_vlm_allowed_terms

ValueListDef.ItemDef.CodeList.CodeListItem.Decode.TranslatedText

## define_vlm_ccode

ValueListDef.ItemDef.CodeList.Alias.Name

## define_vlm_codelist_coded_values

ValueListDef.ItemDef.CodeList.[CodeListItem/EnumeratedItem].CodedValue

## define_vlm_data_type

ValueListDef.ItemDef.DataType

## define_vlm_format

[Not Implemented]

## define_vlm_has_codelist

ValueListDef.ItemDef.CodeListRef exists

## define_vlm_has_comment

ValueListDef.ItemDef.CommentOID exists

## define_vlm_has_no_data

ValueListDef.ItemRef.HasNoData

## define_vlm_is_collected

ValueListDef.ItemDef.Origin.Type = \"Collected\" (2.1) or \"CRF\" (2.0)

## define_vlm_label

ValueListDef.ItemDef.Description.TranslatedText

## define_vlm_length

ValueListDef.ItemDef.Length

## define_vlm_mandatory

ValueListDef.ItemRef.Mandatory

## define_vlm_name

ValueListDef.ItemDef.Name

## define_vlm_order_number

ValueListDef.ItemRef.OrderNumber

## define_vlm_origin_type

ValueListDef.ItemDef.Origin.Type

## define_vlm_role

ValueListDef.ItemRef.Role

## define_vlm_size

ValueListDef.ItemDef.Size

## filename

Name of file

## library_variable_core

core attribute of a variable from the CDISC Library

## library_variable_data_type

simpleDatatype attribute of a variable from the CDISC Library

## library_variable_label

label attribute of a variable from the CDISC Library

## library_variable_name

name attribute of a variable from the CDISC Library

## library_variable_order_number

ordinal attribute of a variable from the CDISC Library

## library_variable_role

role attribute of a variable from the CDISC Library

## row_number

1-based index of record number

## variable_data_type

Variable data type (Char or Num)

## variable_format

Variable format

## variable_has_empty_values

TRUE only when the variable has no non-empty values (every value null/blank, or the variable is not in the dataset); despite the name it does not flag a variable with only some empty values, so it equals `variable_is_empty`. Variable Metadata Check rule types only.

## variable_label

Variable long label

## variable_name

Variable short name

## variable_order_number

Order of variable within dataset

## variable_length

Variable size

## variable_value

Value at `row_number` and `variable_name`

## variable_value_length

Calculated length of the value at `row_number` and `variable_name`

## categories_summary

STF: study-identifier categories as `name:value|name:value...` (same on every row)

## dataset_domain

Domain of the dataset

## define_dataset_domain

ItemGroupDef.Domain

## define_dataset_has_no_data

ItemGroupDef.HasNoData

## define_dataset_variable_order

Ordered list of the ItemGroupDef ItemRef names

## define_dataset_variables

List of the ItemGroupDef ItemRef names

## define_key_sequence_is_unique

Per record: whether its combination of key variables (`define_dataset_key_sequence`, with the record's SUPP-- qualifiers merged in when a SUPP-- dataset exists) is unique within the dataset; TRUE when no key variables are defined. Dataset Contents Check against Define XML only.

## define_variable_source_type

ItemDef.Origin.Source

## doc_index

STF: position of the document in the STF

## document_href

STF: document leaf href

## document_href_fragment

STF: fragment (#...) of the document href

## document_href_path

STF: path of the document href

## document_operation

STF: document operation (e.g. new, append, replace)

## document_properties_json

STF: document properties as JSON

## document_properties_summary

STF: document properties as `name@info-type=value|...`

## document_title

STF: document title

## domain

ItemGroupDef Name, same as `define_dataset_name` (Domain Presence Check against Define XML)

## dtd_version

STF: DTD version

## file_tag_info_type

STF: file tag info-type

## file_tag_name

STF: file tag name

## language

STF: root `xml:lang` attribute (same on every row)

## library_variable_ccode

C-code of the codelist the library assigns to the variable (comma-separated when it assigns several)

## library_variable_ccode_values

Submission values of the terms of the codelist in `library_variable_ccode` (ARRAY_AGG over the loaded CT, stored as text, e.g. `{N,Y}`); empty when `library_variable_ccode` holds several codes. Value Check against Library Metadata only

## library_variable_codelist_name

Name of the codelist in `library_variable_ccode` (single value); empty when `library_variable_ccode` holds several codes. Value Check against Library Metadata only

## library_variable_codelist_value

Submission value of the codelist itself in `library_variable_ccode` (single value, not its terms); empty when `library_variable_ccode` holds several codes. Value Check against Library Metadata only

## record_count

Number of records in the dataset (Dataset Metadata Check, Value Check with Dataset Metadata)

## study_id

STF: study identifier

## study_title

STF: study title

## variable_count

Number of non-empty (non-null, non-blank) values of the variable; 0 when the variable is not in the dataset. Variable Metadata Check against Define XML and Library Metadata only

## variable_is_empty

Whether every value of the variable is empty, i.e. `variable_count` is 0 (TRUE also when the variable is not in the dataset). Variable Metadata Check against Define XML and Library Metadata only
