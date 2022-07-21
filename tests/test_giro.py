#!/usr/bin/env python3

from datetime import datetime

from pytest import fixture

from beancount_import_sparkasse.giro import DEFAULT_FIELDS, SparkasseCSVCAMTImporter


@fixture
def csv_camt_file(tmp_path):
    csv_file = tmp_path / "sample-camt.csv"

    header = ";".join([f'"{field}"' for field in DEFAULT_FIELDS.values()])

    rows = [
        header,
        '"DE12345678901234567890";"01.01.99";"01.01.99";"KARTENZAHLUNG";\
        "1998-12-29T19:13 Debitk.0 2003-12 ";"";"";"28374929581093855282838499";\
        "";"";"";"EDEKA THAUT, BERLIN//BERLIN/DE";"DE23394949202952160028";"\
        EDEKDEHHXXX";"-23,91";"EUR";"Umsatz gebucht"',
    ]
    csv_file.write_text("\n".join(rows))
    return csv_file


@fixture
def default_importer():
    importer = SparkasseCSVCAMTImporter(
        iban="DE00 1234 5678 9012 3456 78", account="Assests:DE:Sparkasse:Giro"
    )
    return importer


def test_identify(default_importer, csv_camt_file):
    assert default_importer.identify(csv_camt_file)


def test_extract(default_importer, csv_camt_file):
    extracted_transactions = default_importer.extract(csv_camt_file)
    assert (
        extracted_transactions[0].date
        == datetime.strptime("01.01.99", "%d.%m.%y").date()
    )
