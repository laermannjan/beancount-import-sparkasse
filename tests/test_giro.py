#!/usr/bin/env python3

from pytest import fixture

from beancount_import_sparkasse.giro import DEFAULT_FIELDS, SparkasseCSVCAMTImporter


@fixture
def csv_camt_file(tmp_path):
    csv_file = tmp_path / "sample-camt.csv"

    header = ";".join([f'"{field}"' for field in DEFAULT_FIELDS])

    rows = [header, ""]
    csv_file.write_text("\n".join(rows))
    return csv_file


def test_identify(csv_camt_file):
    importer = SparkasseCSVCAMTImporter(
        iban="DE00 1234 5678 9012 3456 78", account="Assests:DE:Sparkasse:Giro"
    )

    assert importer.identify(csv_camt_file)
