#!/usr/bin/env python3

from datetime import datetime

from pytest import fixture, yield_fixture

from beancount_import_sparkasse.giro import DEFAULT_FIELDS, SparkasseCSVCAMTImporter


@yield_fixture
def csv_camt_file(tmp_path):
    csv_file = tmp_path / "sample-camt.csv"

    header = ";".join([f'"{field}"' for field in DEFAULT_FIELDS.values()])

    rows = [
        header,
        # excluded_literal debitk.0
        '"DE12345678901234567890";"01.01.99";"01.01.99";"KARTENZAHLUNG";\
        "1998-12-29T19:13 Debitk.0 2003-12 ";"";"";"28374929581093855282838499";\
        "";"";"";"EDEKA THAUT, BERLIN//BERLIN/DE";"DE20293852183742160028";"\
        EDEKDEHHXXX";"-23,91";"EUR";"Umsatz gebucht"',
        # audible + order_id
        '"";"02.07.20";"02.07.20";"FOLGELASTSCHRIFT";\
        "D01-4242420-1337337 Audible Gmbh AKCOCLWMGENLCLDT ";"DE32ZNENCL21039369";\
        "6-)bI2039420mdbajdkdneGejl9vd(";"AKCOCLWMGENLCLDT";"";"";"";"AUDIBLE GMBH";\
        "DE04300308800700054004";"TUBDDEDD";"-9,95";"EUR";"Umsatz gebucht"',
        # paypal + via
        '"";"03.07.20";"03.07.20";"FOLGELASTSCHRIFT";\
        "PP.4994.PP . URBANSPORTS, Ihr Einkauf bei URBANSPORTS ";\
        "LU96ZZZ0000000000000000058";"ALENPGHL024NX";"2095261239852 PP.4994.PP PAYPAL";\
        "";"";"";"PayPal (Europe) S.a.r.l. et Cie., S.C.A.";\
        "DE83920275324985729423";"DEUTDEFFXXX";"-49,00";"EUR";"Umsatz gebucht"',
        # amazon _ order id
        '"";"16.07.20";"16.07.20";"FOLGELASTSCHRIFT";\
        "304-4242420-1337337 Amazon.de 231GADENOCKDLWWU ";"DE24ZZZ00000561652";\
        "6-)bI8NO,k2U4Thaldkvn#dAGE9vd(";"231GADENOCKDLWWU";"";"";"";"\
        AMAZON EU S.A R.L., NIEDERLASSUNG DEUTSCHLAND";"DE22398221409871234982";\
        "TUBDDEDD";"-56,04";"EUR";"Umsatz gebucht"',
        # amazon prime + order_id
        '"";"16.07.20";"16.07.20";"FOLGELASTSCHRIFT";\
        "D01-4242420-1337337 AMZNPrime DE FG20CJENCOIFKLEO ";"DE24AJDK2820223552";\
        "6-)bI20923ldkljlamnenvalyl9vd(";"FG20CJENCOIFKLEO";"";"";"";\
        "AMAZON EU S.A R.L., NIEDERLASSUNG DEUTSCHLAND";"DE20928012348723495336";\
        "TUBDDEDD";"-34,00";"EUR";"Umsatz gebucht"',
    ]
    csv_file.write_text("\n".join(rows))
    with csv_file.open() as f:
        yield f


@fixture
def default_importer():
    importer = SparkasseCSVCAMTImporter(
        iban="DE00 1234 5678 9012 3456 78", account="Assests:DE:Sparkasse:Giro"
    )
    return importer


def test_identify(default_importer, csv_camt_file):
    assert default_importer.identify(csv_camt_file)


def test_extract_date(default_importer, csv_camt_file):
    extracted_transactions = default_importer.extract(csv_camt_file)
    assert (
        extracted_transactions[0].date
        == datetime.strptime("01.01.99", "%d.%m.%y").date()
    )


def test_extract_exclude_literal(default_importer, csv_camt_file):
    default_importer.ref_excluded_regex = [
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2} Debitk\.0 \d{4}-\d{2}"
    ]  # EC-Karten Zahlung
    extracted_transactions = default_importer.extract(csv_camt_file)
    assert extracted_transactions[0].narration == ""


def test_extract_parse_paypal(default_importer, csv_camt_file):
    extracted_transactions = default_importer.extract(csv_camt_file)
    assert extracted_transactions[2].payee == "URBANSPORTS"
    assert extracted_transactions[2].meta["via"] == "paypal"


def test_extract_parse_audible(default_importer, csv_camt_file):
    extracted_transactions = default_importer.extract(csv_camt_file)
    assert extracted_transactions[1].payee == "Audible Gmbh"
    assert extracted_transactions[1].meta["order_number"] == "D01-4242420-1337337"


def test_extract_parse_amazon(default_importer, csv_camt_file):
    extracted_transactions = default_importer.extract(csv_camt_file)
    assert extracted_transactions[3].payee == "Amazon.de"
    assert extracted_transactions[3].meta["order_number"] == "304-4242420-1337337"


def test_extract_parse_amazon_prime(default_importer, csv_camt_file):
    extracted_transactions = default_importer.extract(csv_camt_file)
    assert extracted_transactions[4].payee == "AMZNPrime DE"
    assert extracted_transactions[4].meta["order_number"] == "D01-4242420-1337337"
