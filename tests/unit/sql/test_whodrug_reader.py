import csv
import pytest

from cdisc_rules_engine.readers.external_dictionary_readers.whodrug_reader import WhoDrugReader

VERSION_LINE = "WHODRUG DUMMY B3 January 1, 2000 DUMMYB3Jan00"

INA_ROWS = [
    ("X", "1", "DUMMYLEVEL1"),
    ("X99", "2", "DUMMYLEVEL2"),
    ("X99X", "3", "DUMMYLEVEL3, WITH COMMA"),
    ("X99XX", "4", "DUMMYLEVEL4"),
]
DDA_ROWS = [("000001", "01", "001", "6", "X99XX")]
DD_ROWS = [("000001", "01", "001", "6", "DUMMYDRUG, WITH COMMA")]

EXPECTED = {
    "term_code": "X99XX",
    "level_1": "DUMMYLEVEL1",
    "level_2": "DUMMYLEVEL2",
    "level_3": "DUMMYLEVEL3, WITH COMMA",
    "level_4": "DUMMYLEVEL4",
    "drug_rec_num": "000001",
    "term_name": "DUMMYDRUG, WITH COMMA",
}


def write_fixed_width(directory):
    (directory / "Version.txt").write_text(VERSION_LINE + "\n")
    (directory / "INA.txt").write_text("".join(f"{code:<7}{level}{text:<110}\n" for code, level, text in INA_ROWS))
    (directory / "DDA.txt").write_text(
        "".join(f"{rec}{seq1}{seq2}{check}{atc:<7}  111*\n" for rec, seq1, seq2, check, atc in DDA_ROWS)
    )
    (directory / "DD.txt").write_text(
        "".join(f"{rec}{seq1}{seq2}{check}N  001      01 854{name}\n" for rec, seq1, seq2, check, name in DD_ROWS)
    )


def write_tab_delimited(directory):
    (directory / "Version.txt").write_text(VERSION_LINE + "\n")
    (directory / "INA.txt").write_text("".join("\t".join(row) + "\n" for row in INA_ROWS))
    (directory / "DDA.txt").write_text("".join("\t".join(row) + "\n" for row in DDA_ROWS))
    (directory / "DD.txt").write_text(
        "".join(
            "\t".join([rec, seq1, seq2, check] + [""] * 7 + [name]) + "\n" for rec, seq1, seq2, check, name in DD_ROWS
        )
    )


def write_tab_delimited_without_level(directory):
    write_tab_delimited(directory)
    (directory / "INA.txt").write_text("".join(f"{code}\t{text}\n" for code, _, text in INA_ROWS))


def write_fixed_width_dd_with_extra_field(directory):
    write_fixed_width(directory)
    (directory / "DD.txt").write_text(
        "".join(
            f"{rec}{seq1}{seq2}{check}N  001      01 854{name}\t{name}\n" for rec, seq1, seq2, check, name in DD_ROWS
        )
    )


def write_comma_delimited_txt(directory):
    def write_rows(path, rows):
        with open(path, "w", newline="") as f:
            csv.writer(f).writerows(rows)

    write_rows(directory / "Version.txt", [[VERSION_LINE.rsplit(" ", 1)[0], VERSION_LINE.rsplit(" ", 1)[1]]])
    write_rows(directory / "INA.txt", INA_ROWS)
    write_rows(directory / "DDA.txt", [row + ("111", "*") for row in DDA_ROWS])
    write_rows(
        directory / "DD.txt", [[rec, seq1, seq2, check] + [""] * 7 + [name] for rec, seq1, seq2, check, name in DD_ROWS]
    )


@pytest.mark.parametrize(
    "write_files",
    [
        write_fixed_width,
        write_tab_delimited,
        write_tab_delimited_without_level,
        write_fixed_width_dd_with_extra_field,
        write_comma_delimited_txt,
    ],
)
def test_whodrug_reader_b3_txt(tmp_path, write_files):
    write_files(tmp_path)
    reader = WhoDrugReader(None, str(tmp_path))

    metadata = reader._extract_version_metadata()
    assert (metadata.product_type, metadata.format, metadata.version_date) == ("DUMMY", "B3", "20000101")

    df = reader.process_data(metadata)
    assert df.to_dict(orient="records") == [EXPECTED]


def test_whodrug_reader_b3_dd_names_only(tmp_path):
    write_fixed_width(tmp_path)
    (tmp_path / "INA.txt").write_text("".join(f"{code}\t{text}\n" for code, _, text in INA_ROWS))
    (tmp_path / "DD.txt").write_text("".join(f"{name}\t{name}\n" for *_, name in DD_ROWS))
    reader = WhoDrugReader(None, str(tmp_path))

    df = reader.process_data(reader._extract_version_metadata())
    records = df.astype(object).where(df.notna(), None).to_dict(orient="records")
    assert records == [
        {**EXPECTED, "term_name": None},
        {**{key: None for key in EXPECTED}, "term_name": "DUMMYDRUG, WITH COMMA"},
    ]
