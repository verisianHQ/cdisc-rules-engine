List values are stored as one text value with the items separated by commas (e.g. `define_variable_codelist_coded_values` is `Y,N`), so match an item with a regular expression such as `(^|,)Y(,|$)`. `define_dataset_has_no_data` and the `has_codelist`, `has_comment`, `has_no_data`, `is_collected` and `mandatory` variables (`define_variable_...` and `define_vlm_...`) are booleans (`true` or `false`, empty when absent).

## categories_summary

STF study-identifier categories as `name:value`, separated by `|`

## dataset_domain

Domain of the dataset

## dataset_label

Label for the dataset

## dataset_location

File name of the dataset (e.g. `ae.xpt`)

## dataset_name

Name of the dataset

## dataset_size

File size in bytes

## define_dataset_class

ItemGroupDef.Class.Name (2.1) or ItemGroupDef.Class (2.0)

## define_dataset_domain

ItemGroupDef.Domain

## define_dataset_has_no_data

ItemGroupDef.HasNoData = \"Yes\"

## define_dataset_is_non_standard

ItemGroupDef.IsNonStandard (2.1 only; blank for 2.0)

## define_dataset_key_sequence

Names of the key variables, ordered by [ItemGroupDef/ValueListDef].ItemRef.KeySequence

## define_dataset_label

ItemGroupDef.Description.TranslatedText

## define_dataset_location

ItemGroupDef.leaf.href

## define_dataset_name

ItemGroupDef.Name

## define_dataset_structure

ItemGroupDef.Structure

## define_dataset_variable_order

Names of the dataset's variables in upper case, ordered by ItemGroupDef.ItemRef.OrderNumber and separated by commas

## define_dataset_variables

ItemGroupDef.ItemRef.ItemDef.Name (list of the dataset's variable names)

## define_key_sequence_is_unique

True/False value indicating whether no other record of the dataset has the same values of the key variables in `define_dataset_key_sequence` (true for every record when the Define-XML has no key variables)

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

ItemGroupDef.ItemRef.OrderNumber (or the position of the ItemRef when OrderNumber is absent)

## define_variable_origin_type

ItemGroupDef.ItemDef.Origin.Type

## define_variable_role

ItemGroupDef.ItemRef.Role

## define_variable_source_type

ItemGroupDef.ItemDef.Origin.Source

## define_vlm_allowed_terms

ValueListDef.ItemDef.CodeList.CodeListItem.Decode.TranslatedText

## define_vlm_ccode

ValueListDef.ItemDef.CodeList.Alias.Name

## define_vlm_codelist_coded_values

ValueListDef.ItemDef.CodeList.[CodeListItem/EnumeratedItem].CodedValue

## define_vlm_data_type

ValueListDef.ItemDef.DataType

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

## doc_index

1-based position of the STF document (`doc-content`), in file order

## document_href

STF document `xlink:href`

## document_href_fragment

Part of `document_href` after `#`

## document_href_path

Part of `document_href` before `#`

## document_operation

`operation` attribute of the `leaf` whose ID is `document_href_fragment`, in the file at `document_href_path` (relative to the STF file)

## document_properties_json

STF document properties as a JSON list of `name`, `info_type` and `value`

## document_properties_summary

STF document properties as `name@info-type=value`, separated by `|`

## document_title

STF document title

## domain

Define-XML dataset name (ItemGroupDef.Name), in `Domain Presence Check against Define XML`

## dtd_version

STF `dtd-version`

## file_tag_info_type

STF file tag `info-type`

## file_tag_name

STF file tag name

## filename

File name of the submitted dataset (e.g. `ae.xpt`)

## language

STF `xml:lang`

## library_variable_ccode

C-code of the codelist that the CDISC Library links to the variable (codes separated by commas when there is more than one)

## library_variable_ccode_values

Submission values of the terms of the codelist in `library_variable_ccode`, from the CT packages of the run

## library_variable_codelist_name

Name of the codelist in `library_variable_ccode`

## library_variable_codelist_value

Submission value of the codelist in `library_variable_ccode`

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

## record_count

Number of records in the dataset

## row_number

Position of the record in the dataset, starting from 1

## study_id

STF study-identifier study-id

## study_title

STF study-identifier title

## variable_count

Number of records with a non-empty value of the variable (0 when the variable is not in the dataset)

## variable_data_type

Variable data type (Char or Num)

## variable_format

Variable format

## variable_has_empty_values

True/False value indicating whether a variable has all empty values

## variable_is_empty

True/False value indicating whether the variable has no non-empty values (true when the variable is not in the dataset)

## variable_label

Variable long label

## variable_max_value_length

Length of the longest value of the variable in the dataset

## variable_name

Variable short name

## variable_order_number

Order of variable within dataset

## variable_length

Variable length

## variable_value

Value at `row_number` and `variable_name`

## variable_value_length

Length of the value at `row_number` and `variable_name`, as text (number of characters)
