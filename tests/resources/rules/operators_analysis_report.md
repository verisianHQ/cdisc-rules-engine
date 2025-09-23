# 🔍 Check Operators Analysis Report

## 📊 Summary

- Total rules without check operators: 762
- Total unique operators found in rules: 58
- Total implemented PostgreSQL operators: 91
- Missing operators (in rules but not implemented): 1
- Extra operators (implemented but not used in rules): 34

## ✅ Implementation Status

### ❌ Missing Operators (Need Implementation)

| Operator                 | Status             |
| ------------------------ | ------------------ |
| `is_not_valid_reference` | ❌ Not Implemented |

## 🔍 Unused Implemented Operators

The following operators are implemented but not currently used in any rules:

```
- conformant_value_data_type
- conformant_value_length
- contains_all
- contains_case_insensitive
- does_not_contain_case_insensitive
- does_not_reference_correct_codelist
- equals_string_part
- has_different_values
- has_equal_length
- has_next_corresponding_record
- is_not_ordered_by
- is_not_ordered_set
- is_not_ordered_subset_of
- is_ordered_by
- is_ordered_set
- is_ordered_subset_of
- is_unique_relationship
- longer_than_or_equal_to
- non_conformant_value_data_type
- non_conformant_value_length
- non_empty_within_except_last_row
- prefix_is_contained_by
- references_correct_codelist
- shares_at_least_one_element_with
- shares_exactly_one_element_with
- shorter_than_or_equal_to
- suffix_equal_to
- suffix_is_contained_by
- suffix_not_equal_to
- target_is_sorted_by
- value_does_not_have_multiple_references
- value_has_multiple_references
- variable_metadata_equal_to
- variable_metadata_not_equal_to
```

## 📋 Rules Without Check Operators

| Core ID              | CDISC Rule ID                                             | Standard                                                                         | Status    | In Cache |
| -------------------- | --------------------------------------------------------- | -------------------------------------------------------------------------------- | --------- | -------- |
| CDISC.ADAMIG.111     | 111                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.252     | 252                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0019  | 19                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0020  | 20                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0021  | 21                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0022  | 22                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0023  | 23                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0024  | 24                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0025  | 25                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0026  | 26                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0027  | 27                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0028  | 28                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0029  | 29                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0030  | 30                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0031  | 31                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0032  | 32                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0039  | AD0039                                                    | ADAMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0041  | 41                                                        | ADAMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0046  | 46                                                        | ADAMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0047  | 47                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0053  | 53                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0061  | AD0061                                                    | ADAMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0088  | 88                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0089  | 89                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0261  | 261                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0262  | 262                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0278  | 278                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0620  | 620                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0621  | 621                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0623  | 623                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0624  | 624                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0625  | 625                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0626  | 626                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0627  | 627                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0628  | 628                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0629  | 629                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0630  | 630                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0631  | 631                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0632  | 632                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0633  | 633                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0005  | 5                                                         | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0006  | 6                                                         | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0007  | 7                                                         | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0013  | 13                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0014  | 14                                                        | Model v2.1, ADaMIG                                                               | Draft     | ❌       |
| CDISC.ADaMIG.AD0016  | 16                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0017  | 17                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0049  | 49                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0054  | 54                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0060  | 60                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0143  | 143                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0144  | 144                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0145  | 145                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0159  | 159                                                       | ADaMIG, ADaMIG-MD                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0160  | 160                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0163  | 163                                                       | ADaMIG, ADaMIG-MD                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0164  | 164                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0166  | 166                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0167  | 167                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0169  | 169                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0176  | 176                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0178  | 178                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0194  | 194                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0195  | 195                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0196  | 196                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0197  | 197                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0198  | 198                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0204  | 204                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0211  | 211                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0252b | 252                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0254  | 254                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0254b | 254                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0268  | 268                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0269  | 269                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0270  | 270                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0271  | 271                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0279  | 279                                                       | ADaMIG, ADaMIG-MD, OCCDS                                                         | Draft     | ❌       |
| CDISC.ADaMIG.AD0363  | 363                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                                       | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0373  | 373                                                       | ADTTE                                                                            | Draft     | ❌       |
| CDISC.ADaMIG.AD050   | 50                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD051   | 51                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD052   | 52                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0522b | 522                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0523b | 523                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0524  | 524                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0525  | 525                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0529  | 529                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0530  | 530                                                       | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD055   | 55                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0619  | 619                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD064   | 64                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD071   | 71                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD072   | 72                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD080   | 80                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0800  | 800                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0802  | 802                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0804  | 804                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0805  | 805                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0806  | 806                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0807  | 807                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0809  | 809                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0811  | 811                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0812  | 812                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0813  | 813                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0814  | 814                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0815  | 815                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0816  | 816                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0817  | 817                                                       | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0001  | CG0001, TIG0289, SEND16, TIG0090                          | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                 | Draft     | ❌       |
| CDISC.SDTMIG.CG0002  | CG0002                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0010  | CG0010                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0012  | CG0012, SEND4                                             | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART                                      | Draft     | ❌       |
| CDISC.SDTMIG.CG0015  | CG0015                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0019  | CG0019                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0096  | CG0096                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0097  | CG0097                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0103  | CG0103                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0104  | CG0104                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0155  | CG0155                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0156  | CG0156                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0157  | CG0157                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0158  | CG0158                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0159  | CG0159                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0160  | CG0160                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0161  | CG0161                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0162  | CG0162                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0167  | CG0167                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0169  | CG0169                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0173  | CG0173                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0184  | CG0184                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0186  | CG0186                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0187  | CG0187                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0188  | CG0188                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0189  | CG0189                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0190  | CG0190                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0192  | CG0192                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0196  | CG0196                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0197  | CG0197                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0198  | CG0198                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0236  | CG0236                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0237  | CG0237                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0238  | CG0238                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0249  | CG0249                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0250  | CG0250                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0251  | CG0251                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0252  | CG0252                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0253  | CG0253                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0254  | CG0254                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0267  | CG0267                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0288  | CG0288                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0295  | CG0295                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0296  | CG0296                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0298  | CG0298                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0312  | CG0312                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0320  | CG0320                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0321  | CG0321                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0322  | CG0322                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0323  | CG0323                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0324  | CG0324                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0347  | CG0347                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0351  | CG0351                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0375  | CG0375                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0377  | CG0377                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0378  | CG0378                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0379  | CG0379                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0380  | CG0380                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0381  | CG0381                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0382  | CG0382                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0383  | CG0383                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0384  | CG0384                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0385  | CG0385                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0386  | CG0386                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0400  | CG0400                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0418  | CG0418                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0425  | CG0425                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0431  | CG0431                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0436  | CG0436                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0437  | CG0437                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0437  | CG0442                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0443  | CG0443                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0445  | CG0445                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0446  | CG0446                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0448  | CG0448                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0449  | CG0449                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0450  | CG0450                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0451  | CG0451                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0453  | CG0453                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0454  | CG0454                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0460  | CG0460                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0461  | CG0461                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0463  | CG0463                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0464  | CG0464                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0466  | CG0466                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0523  | CG0523                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0524  | CG0524                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0525  | CG0525                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0526  | CG0526                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0527  | CG0527                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0532  | CG0532                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0546  | CG0546                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0548  | CG0548                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0551  | CG0551                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0555  | CG0555                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0556  | CG0556                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0557  | CG0557                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0558  | CG0558                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0559  | CG0559                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0560  | CG0560                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0562  | CG0562, TIG0648                                           | SDTMIG, TIG                                                                      | Draft     | ❌       |
| CDISC.SDTMIG.CG0563  | CG0563                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0604  | CG0604                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0605  | CG0605                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0607  | CG0607                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0608  | CG0608                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0609  | CG0609                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0610  | CG0610                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0614  | CG0614                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0615  | CG0615                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0616  | CG0616                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0617  | CG0617                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0651  | CG0651                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0663  | CG0663                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SENDIG.296     | FB1701, SEND296                                           | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| CDSIC.ADaMIG.AD0010  | 10                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDSIC.ADaMIG.AD0070  | 70                                                        | ADaMIG                                                                           | Draft     | ❌       |
| CDSIC.ADaMIG.AD0112  | 112                                                       | ADaMIG, ADaMIG-MD                                                                | Draft     | ❌       |
| CDSIC.ADaMIG.AD0113  | 113                                                       | ADaMIG, ADaMIG-MD                                                                | Draft     | ❌       |
| CORE-000001          | CG0176, TIG0405                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000002          | CG0208                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000003          | CG0299                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000004          | CG0101, TIG0366                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000005          | CG0102                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000006          | CG0131, TIG0381                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000007          | CG0435, TIG0587, FB0606                                   | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000008          | CG0132, FB0601                                            | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000009          | CG0152, SEND124.1, TIG0393, TIG0061                       | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000010          | CG0153, TIG0394                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000011          | CG0175, TIG0404                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000012          | CG0040, TIG0319                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000013          | CG0044, TIG0323                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000014          | CG0087, TIG0352                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000015          | CG0088, TIG0353                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000016          | CG0089, TIG0354                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000017          | CG0166, TIG0398                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000018          | CG0086, TIG0351                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000019          | CG0311, SEND3, TIG0486, TIG0211                           | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000020          | CG0206, TIG0426                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000021          | CG0397, TIG0559                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000022          | CG0041, TIG0320                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000023          | CG0084, TIG0349                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000024          | CG0082, TIG0347                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000025          | CG0177, TIG0406                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000026          | CG0468, TIG0598                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000027          | CG0328, TIG0496, CG0329, TIG0497, SEND214, TIG0141        | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                 | Published | ✅       |
| CORE-000028          | CG0008, TIG0293                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000029          | CG0661, TIG0697                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000030          | CG0053, TIG0329                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000031          | CG0659                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000032          | CG0660                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000033          | CG0065, TIG0337                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000034          | CG0069, TIG0339                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000035          | CG0658, TIG0696                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000036          | CG0657, TIG0695                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000037          | CG0653, TIG0691                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000038          | CG0654, TIG0692                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000039          | CG0655, TIG0693                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000040          | CG0656, TIG0694                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000041          | CG0649, TIG0689, CG0459                                   | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000042          | CG0647, TIG0687                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000043          | CG0648, TIG0688                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000044          | CG0646, TIG0686                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000045          | CG0517, TIG0612                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000046          | CG0519, TIG0614                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000047          | CG0518, TIG0613                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000048          | CG0621, TIG0662                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000049          | CG0507, CG0622, TIG0602, TIG0663                          | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000050          | CG0623, TIG0664                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000051          | CG0624, TIG0665                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000052          | CG0625, TIG0666                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000054          | CG0627, TIG0668                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000055          | CG0628, TIG0669                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000056          | CG0629, TIG0670                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000057          | CG0509, CG0630, TIG0604, TIG0671                          | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000058          | CG0631, TIG0672                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000059          | CG0632, TIG0673                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000060          | CG0633, TIG0674                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000061          | CG0634, TIG0675                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000064          | CG0637, TIG0678                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000065          | CG0638, TIG0679                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000066          | CG0639, TIG0680                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000067          | CG0640, TIG0681                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000068          | CG0641, TIG0682                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000069          | CG0642, TIG0683                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000070          | CG0643, TIG0684                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000071          | CG0644, TIG0685                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000072          | CG0542                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000073          | CG0533                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000074          | CG0508, TIG0603                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000075          | CG0304, TIG0481                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000076          | CG0302                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000077          | CG0301                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000078          | CG0300                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000079          | CG0095                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000080          | CG0093, TIG0358                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000081          | CG0056, TIG0330                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000082          | CG0561                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000083          | CG0553, TIG0641                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000084          | CG0057, TIG0331                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000085          | CG0059, TIG0333                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000086          | CG0075, TIG0341                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000087          | CG0387, TIG0549                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000088          | CG0149, SEND25, TIG0169, TIG0390                          | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000089          | CG0106                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000090          | CG0164, TIG0397                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000091          | CG0108                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000092          | CG0110, CG0111, TIG0374, TIG0375                          | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000093          | CG0114, TIG0377                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000094          | CG0112, TIG0376                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000095          | CG0211, TIG0430                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000096          | CG0115, TIG0378                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000097          | CG0218, TIG0432                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000098          | CG0116, TIG0379                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000099          | CG0422, TIG0577                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000100          | CG0423, TIG0578                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000101          | CG0427, TIG0581                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000102          | CG0428, TIG0582                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000103          | CG0429, TIG0583                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000104          | CG0430, TIG0584                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000105          | CG0569, TIG0653                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000106          | CG0045, TIG0324                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000107          | CG0554, TIG0642                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000108          | CG0133, TIG0382, FB0602                                   | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000109          | CG0547                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000110          | CG0549, TIG0637                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000111          | CG0564, TIG0649                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000112          | CG0565, TIG0650                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000113          | CG0566, TIG0651                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000114          | CG0567, TIG0652                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000115          | CG0570                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000116          | CG0619, TIG0660                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000117          | CG0081, TIG0346                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000118          | CG0404, TIG0562                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000119          | CG0521, TIG0616                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000120          | CG0522, TIG0617                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000121          | CG0520, TIG0615                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000122          | CG0434, TIG0586                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000123          | CG0388, TIG0550                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000124          | CG0389, TIG0551                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000125          | CG0390, TIG0552                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000126          | CG0391, TIG0553                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000127          | CG0392, TIG0554                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000128          | CG0393, TIG0555                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000129          | CG0394, TIG0556                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000130          | CG0395, TIG0557                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000131          | CG0396, TIG0558                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000132          | CG0154, SEND213, FB0914                                   | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, SENDIG-AR                           | Published | ✅       |
| CORE-000133          | CG0426, TIG0580                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000134          | CG0203, TIG0423                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000135          | CG0204, TIG0424                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000136          | CG0201, TIG0422                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000137          | CG0100, TIG0365                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000138          | CG0221, TIG0435                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000139          | CG0223, TIG0437                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000140          | CG0225, TIG0438                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000141          | CG0240, TIG0448, FB0922                                   | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000142          | CG0241, TIG0449                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000143          | CG0246, SEND24, TIG0450, TIG0165                          | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000144          | CG0247, TIG0451                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000145          | CG0255                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000146          | CG0256                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000147          | CG0257, SEND26, TIG0178, TIG0461                          | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000148          | CG0258, TIG0462                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000149          | CG0259                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000150          | CG0260                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000151          | CG0261, SEND281                                           | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                      | Published | ✅       |
| CORE-000152          | CG0265, TIG0467                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000153          | CG0266                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000154          | CG0268, SEND246, TIG0167, TIG0470                         | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000155          | CG0293, TIG0475                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000156          | CG0294, TIG0476                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000157          | CG0297, TIG0479                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000158          | CG0163, TIG0396                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000159          | CG0341, TIG0506                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000160          | CG0342, TIG0507                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000161          | CG0343, TIG0508                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000162          | CG0344, TIG0509                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000163          | CG0345, TIG0510                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000164          | CG0346, TIG0511                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000165          | CG0090, TIG0355, FB3701                                   | SDTMIG, TIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                      | Published | ✅       |
| CORE-000166          | CG0091, TIG0356                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000167          | CG0092, TIG0357                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000168          | CG0034, TIG0315                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000169          | CG0185, TIG0414                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000170          | CG0541, TIG0634                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000171          | CG0058, TIG0332                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000172          | CG0409, SEND249.1, TIG0168, TIG0565                       | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000173          | CG0414, TIG0569                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000174          | CG0356                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000175          | CG0357                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000176          | CG0358                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000177          | CG0227, TIG0440                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000178          | CG0172                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000179          | CG0307, TIG0482, FB0916                                   | SDTMIG, TIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                      | Published | ✅       |
| CORE-000180          | CG0308, TIG0483                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000181          | CG0309                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000182          | CG0310, SEND2, TIG0485, TIG0128                           | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000183          | CG0318, TIG0489                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000184          | CG0083, TIG0348                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000185          | CG0123, TIG0380                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000186          | CG0150, TIG0391                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000187          | CG0168, TIG0400                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000188          | CG0191                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000189          | CG0665, TIG0699                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000190          | CG0666, TIG0700                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000191          | CG0529, TIG0622                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000192          | CG0530, TIG0623                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000193          | CG0503                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000195          | CG0338, TIG0503                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000196          | CG0337, TIG0502                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000197          | CG0339, TIG0504                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000198          | CG0340, TIG0505                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000199          | CG0406, SEND64, TIG0563                                   | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000200          | CG0348, TIG0513                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000201          | CG0029, TIG0311, SEND109, TIG0046                         | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                 | Published | ✅       |
| CORE-000202          | CG0419, TIG0574                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000203          | CG0411, TIG0567                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000204          | CG0410, TIG0566                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000205          | CG0399                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000206          | CG0371, TIG0535, SEND121, TIG0058                         | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                 | Published | ✅       |
| CORE-000207          | CG0467, SEND78, TIG0597, TIG0276                          | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000208          | CG0512, TIG0607                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000209          | CG0514, TIG0609                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000210          | CG0516, TIG0611                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000211          | CG0528                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000212          | CG0535, TIG0628                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000213          | CG0536, TIG0629                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000214          | CG0538, TIG0631                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000215          | CG0539                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000216          | CG0545                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000217          | CG0462, TIG0592                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000218          | CG0465, SEND118, TIG0595, TIG0055                         | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000219          | CG0350, SEND44, TIG0515                                   | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, TIG                                 | Published | ✅       |
| CORE-000220          | CG0372, TIG0536                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000221          | CG0417, TIG0572                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000222          | CG0416, TIG0571                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000223          | CG0513, TIG0608                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000224          | CG0515, TIG0610                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000225          | CG0094, TIG0359                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000226          | CG0352                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000227          | CG0178, TIG0407                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000228          | CG0179, TIG0408                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000229          | CG0361                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000230          | CG0362                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000231          | CG0363                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000232          | CG0364                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000233          | CG0365                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000234          | CG0366                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000235          | CG0367                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000236          | CG0079, TIG0345                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000237          | CG0027, SEND43, TIG0309                                   | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, TIG                                 | Published | ✅       |
| CORE-000238          | CG0147, TIG0388                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000239          | CG0148, TIG0389                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000240          | CG0420, TIG0575                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000241          | CG0421, TIG0576                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000242          | CG0353, TIG0518                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000243          | CG0354, TIG0519                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000244          | CG0355, TIG0520                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000245          | CG0510, TIG0605, CG0635, TIG0676                          | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000246          | CG0511, TIG0606, CG0636, TIG0677                          | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000247          | CG0552, TIG0640, CG0626, TIG0667                          | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000248          | CG0026, SEND63, TIG0265, TIG0308, FB3702                  | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG, SENDIG-AR                      | Published | ✅       |
| CORE-000249          | CG0032, TIG0313                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000250          | CG0078, TIG0344                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000251          | CG0067, FB0614                                            | SDTMIG                                                                           | Published | ✅       |
| CORE-000252          | CG0136, TIG0385, FB0605                                   | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000253          | CG0135, TIG0384, FB0604                                   | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000254          | CG0134, TIG0383, FB0603                                   | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000256          | CG0210, TIG0429, SEND125.1, TIG0062                       | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                 | Published | ✅       |
| CORE-000257          | CG0061, TIG0335, FB4401                                   | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                      | Published | ✅       |
| CORE-000258          | CG0062, TIG0336                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000259          | CG0171, FB0613                                            | SDTMIG                                                                           | Published | ✅       |
| CORE-000260          | CG0085, TIG0350                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000261          | CG0060, TIG0334                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000262          | CG0226, TIG0439                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000264          | CG0039                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000265          | CG0046, TIG0325, FB4402                                   | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                      | Published | ✅       |
| CORE-000266          | CG0042, TIG0321                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000267          | CG0050, TIG0328                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000268          | CG0049, TIG0327                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000269          | CG0031, TIG0312                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000270          | CG0033, TIG0314                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000271          | CG0009, TIG0294                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000272          | CG0336, SEND46, TIG0501                                   | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000289          | CG0181, TIG0410                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000290          | CG0180, TIG0409                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000291          | CG0105, TIG0370                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000292          | CG0107                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000293          | CG0205, TIG0425                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000294          | CG0270, TIG0471                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000295          | CG0550, TIG0638                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000296          | CG0540, TIG0633                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000297          | CG0501                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000298          | CG0182, TIG0411                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000299          | CG0183, TIG0412                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000302          | FB0920                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000303          | FB0901                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000305          | FB4005                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000308          | FB4002                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000310          | FB4001                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000318          | FB0902                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000321          | FB3201                                                    | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART, SENDIG-GENETOX                           | Published | ✅       |
| CORE-000322          | CG0066                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000324          | CG0234, TIG0443                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000326          | CG0568                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000328          | FB3202                                                    | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART, SENDIG-GENETOX                           | Published | ✅       |
| CORE-000329          | CG0063                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000330          | CG0122                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000331          | CG0124, CG0125                                            | SDTMIG                                                                           | Published | ✅       |
| CORE-000332          | CG0193                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000333          | CG0215                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000334          | CG0016, TIG0301, SEND13, TIG0065                          | SDTMIG, TIG, SENDIG, SENDIG-GENETOX, SENDIG-DART                                 | Published | ✅       |
| CORE-000335          | CG0280                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000337          | CG0140, TIG0386                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000338          | CG0457                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000339          | CG0456                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000340          | CG0440                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000343          | CG0011                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000344          | CG0020                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000345          | CG0021                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000346          | CG0047                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000347          | CG0071, TIG0340                                           | SDTMIG, TIG                                                                      | Draft     | ❌       |
| CORE-000348          | CG0076                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000349          | CG0098                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000350          | CG0099                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000351          | CG0151, TIG0392, SEND37, TIG0255                          | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                 | Published | ✅       |
| CORE-000352          | CG0207, SEND126, TIG0427                                  | SDTMIG, SENDIG, TIG                                                              | Published | ✅       |
| CORE-000354          | CG0007, TIG0292                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000355          | CG0014, SEND12, TIG0299, TIG0057                          | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000356          | CG0014, SEND12, TIG0299, TIG0057                          | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000357          | CG0018, TIG0303                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000358          | CG0022, TIG0306                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000361          | CG0035, TIG0316, FB0919                                   | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000362          | CG0037                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000363          | CG0068, TIG0338                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000364          | CG0073                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000365          | CG0077                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000366          | CG0109                                                    | SDTMIG                                                                           | Published | ❌       |
| CORE-000367          | CG0117, CG0118                                            | SDTMIG                                                                           | Published | ✅       |
| CORE-000368          | CG0120, CG0121                                            | SDTMIG                                                                           | Published | ✅       |
| CORE-000369          | CG0141                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000370          | CG0143, TIG0387                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000371          | CG0244                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000372          | CG0269                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000373          | CG0271                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000374          | CG0537, TIG0630                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000375          | CG0202                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000376          | CG0349, TIG0514                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000377          | CG0129                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000378          | CG0142                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000379          | CG0145                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000380          | CG0146                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000384          | CG0374, TIG0538                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000398          | CG0303, SEND276                                           | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                      | Published | ✅       |
| CORE-000452          | CG0432                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000453          | CG0433                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000457          | CG0373, TIG0537                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000467          | CG0441                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000468          | CG0276                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000469          | CG0277                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000470          | CG0278                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000473          | CG0272                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000483          | CG0455                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000484          | CG0200, TIG0421                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000497          | CG0279                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000501          | CG0216                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000502          | CG0369, TIG0533                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000504          | CG0283                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000505          | CG0285, TRC1734c                                          | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000506          | CG0286                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000510          | CG0017                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000515          | CG0412                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000516          | FB3601                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000517          | FB4003                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000518          | FB4004                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000522          | FB1401                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000527          | CG0209, TIG0428                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000529          | CG0006, TIG0291, SEND73, TIG0274, SEND74, TIG0275, FB1603 | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, SENDIG-AR                      | Published | ✅       |
| CORE-000530          | CG0119                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000531          | CG0126, CG0128                                            | SDTMIG                                                                           | Published | ✅       |
| CORE-000532          | CG0275                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000533          | CG0138                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000534          | CG0248, SEND221, TIG0452, TIG0148                         | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000535          | CG0662, CG0620, SEND130.1, SEND130, TIG0066, TIG0661      | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000538          | CG0334, TIG0500                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000539          | CG0332, TIG0498                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000540          | CG0333, TIG0499                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000541          | CG0372, TIG0536                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000543          | CG0127                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000544          | CG0028                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000550          | CG0013, TIG0298                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000552          | CG0220, TIG0434                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000553          | CG0222, TIG0436                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000560          | 1                                                         | ADaMIG                                                                           | Published | ❌       |
| CORE-000562          | CG0273                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000563          | CG0282                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000564          | CG0458                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000571          | CG0024, TIG0307                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000572          | CG0235, TIG0444                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000575          | CG0219, SEND65, TIG0266, TIG0433                          | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000579          | CG0408                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000580          | CG0325, TIG0495                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000581          | CG0368, TIG0532, TRC1736a, TRC1736c                       | SDTMIG, TIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                      | Published | ✅       |
| CORE-000582          | CG0262                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000585          | CG0313                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000586          | CG0398                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000587          | CG0284                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000594          | CG0359, SEND29, TIG0524, TIG0205                          | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, TIG                                 | Published | ✅       |
| CORE-000597          | CG0043, TIG0322                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000598          | CG0413, TIG0568, SEND1, TIG0037                           | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                 | Published | ✅       |
| CORE-000612          | FB7001                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000616          | FB4403                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Published | ✅       |
| CORE-000642          | FB4404                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Published | ✅       |
| CORE-000643          | FB2601                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000655          | FB2501                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000656          | FB2502                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000657          | FB3409                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000658          | FB3412                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000659          | FB3410                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000672          | FB3901                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, SENDIG-AR                           | Published | ✅       |
| CORE-000675          | FB4201                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000679          | FB0608                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000685          | CG0571, FB0923                                            | SDTMIG                                                                           | Published | ✅       |
| CORE-000686          | CG0572, FB0924                                            | SDTMIG                                                                           | Published | ✅       |
| CORE-000689          | CG0573, FB0925                                            | SDTMIG                                                                           | Published | ✅       |
| CORE-000699          | FB3001                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000700          | FB3204                                                    | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART, SENDIG-GENETOX                           | Published | ✅       |
| CORE-000701          | FB2201                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000705          | FB0610                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000706          | FB0906                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000707          | FB3208                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000708          | FB3207                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000709          | FB0607                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000710          | CG0233, TIG0442                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000711          | FB3404                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000712          | CG0370, TIG0534                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000713          | FB3411                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000714          | FB3408                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000716          | FB0911                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000717          | FB3401                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000718          | FB3209                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000719          | FB0908                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000720          | FB3406                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000723          | FB0907                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000724          | FB0910                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000725          | FB0909                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000726          | FB5701                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000728          | FB0904                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000729          | FB0912                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000730          | CG0438                                                    | SDTMIG                                                                           | Draft     | ✅       |
| CORE-000731          | CG0439                                                    | SDTMIG                                                                           | Draft     | ✅       |
| CORE-000732          | FB3103                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Published | ✅       |
| CORE-000733          | CG0281                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000734          | FB1104                                                    | SDTMIG                                                                           | Draft     | ✅       |
| CORE-000735          | CG0444                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000736          | FB1116                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000737          | FB1102                                                    | SDTMIG                                                                           | Draft     | ✅       |
| CORE-000738          | FB1106                                                    | SDTMIG                                                                           | Draft     | ✅       |
| CORE-000739          | CG0407                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000740          | CG0287                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000741          | FB1111                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000742          | FB1110                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000743          | FB0918                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000744          | CG0174, TIG0403                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000745          | FB0915                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000746          | FB5801                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000747          | FB6001                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000748          | FB2504                                                    | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000757          | CG0602, TIG0656                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000758          | CG0534, TIG0627                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000760          | FB3413                                                    | SDTMIG, SENDIG-AR                                                                | Published | ✅       |
| CORE-000761          | CG0289                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000763          | CG0232, TIG0441                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000765          | FB2401                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000766          | CG0601                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000767          | CG0603                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000774          | FB4202                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000776          | FB3203                                                    | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART, SENDIG-GENETOX                           | Published | ✅       |
| CORE-000777          | CG0502                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000778          | CG0650                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000779          | CG0376                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000783          | CG0314, SEND274, SEND274.1                                | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART                                      | Published | ✅       |
| CORE-000784          | CG0217, TIG0431                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000785          | CG0429, TIG0583                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000786          | CG0430, TIG0584                                           | SDTMIG, TIG                                                                      | Published | ✅       |
| CORE-000787          | CG0291                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000790          | FB2505                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000791          | FB0903                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000792          | FB0905                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000793          | FB1601                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ✅       |
| CORE-000841          | FB0612                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000842          | CG0213                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000843          | CG0214                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000844          | CG0531                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000845          | CG0531                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000846          | CG0531                                                    | SDTMIG                                                                           | Published | ✅       |
| CORE-000852          | CG0330, CG0664, TIG0698, SEND48                           | SDTMIG, TIG, SENDIG, SENDIG-GENETOX, SENDIG-DART                                 | Published | ❌       |
| CORE-000853          | FB1602                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ❌       |
| CORE-000862          | FB3205                                                    | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART, SENDIG-GENETOX                           | Published | ❌       |
| CORE-000863          | FB3101                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Published | ❌       |
| CORE-000864          | FB3206                                                    | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART, SENDIG-GENETOX                           | Published | ❌       |
| CORE-000865          | FB4405                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Published | ❌       |
| CORE-000866          | FB3210                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ❌       |
| CORE-000867          | FB1501                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Published | ❌       |
| FB2602               | FB2602                                                    | SDTMIG                                                                           | Draft     | ❌       |
| FDA.SDTMIG.CT2001    | FDAB017                                                   | SDTMIG                                                                           | Draft     | ❌       |
| FDA.SDTMIG.SD1097    | FB0101                                                    | SDTMIG                                                                           | Draft     | ❌       |
| FDA.TRC1734a         | TRC1734a                                                  | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| FDA.TRC1734b         | TRC1734                                                   | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART                                           | Draft     | ❌       |
| NicTest              | Demo                                                      | SDTMIG                                                                           | Draft     | ❌       |
| WHODrug              | WHODrug                                                   | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1502                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Draft     | ❌       |
| unknown              | FB3102                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Draft     | ❌       |
| unknown              | FB2303                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1107                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2305                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB0611                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2301                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2302                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB4301                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2304                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2306                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB3211                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB6901                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB2503                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB0609                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB4006                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB3801                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB1801                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB1901                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB0405                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1112                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1109                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1108                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB7101                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB3405                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB3407                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB3403                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Draft     | ❌       |
| unknown              | FB3402                                                    | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Draft     | ❌       |
| unknown              | FB0501                                                    | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB0913                                                    | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB2604J                                                   | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FULL_TEMPLATE,                                            | ADaMIG, USDM, SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, SENDIG-AR, Define-XML | Draft     | ❌       |
| unknown              | NICRULE.MEDDRA                                            | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | 895                                                       | ADaMIG                                                                           | Draft     | ❌       |
| unknown              | FULL_TEMPLATE,                                            | ADaMIG, USDM, SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, SENDIG-AR, Define-XML | Draft     | ❌       |
| unknown              | FULL_TEMPLATE,                                            | ADaMIG, USDM, SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, SENDIG-AR, Define-XML | Draft     | ❌       |
