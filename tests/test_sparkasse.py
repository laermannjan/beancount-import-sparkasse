#!/usr/bin/env python3

import random
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal

import pytest
from tests.utils import fake_iban

from beancount_import_sparkasse import sparkasse


def make_csv_row(fields, kwargs):
    csv_row = OrderedDict()
    for field in fields:
        if field == "Betrag":
            csv_row[field] = "0,00"
        else:
            csv_row[field] = ""
    csv_row.update(kwargs)
    return csv_row


def csv_row_to_str(csv_row: dict[str, str]):
    return ";".join([f'"{field}"' for field in csv_row.values()])

    return


@pytest.fixture
def csv_file(tmp_path):
    file = tmp_path / "test_file.csv"
    file.write_text(
        sparkasse.CsvCamtImporter(iban="foo", account="foo").expected_header + "\n"
    )
    return file


def test_identify(tmp_path):
    file = tmp_path / "identify.csv"
    iban = fake_iban()
    importer = sparkasse.CsvCamtImporter(iban=iban, account="irrelevant")
    csv_row = csv_row_to_str(
        make_csv_row(fields=importer.fields, kwargs={"Auftragskonto": iban})
    )

    importer.expected_header
    file.write_text(importer.expected_header + "\n" + csv_row)
    with open(file) as f:
        assert importer.identify(f)

    fields = importer.expected_header.split(importer.delimiter)
    random.shuffle(fields)
    unexpected_header = importer.delimiter.join(fields)
    file.write_text(unexpected_header + "\n" + csv_row)
    with open(file) as f:
        assert not importer.identify(f)


def test_extract_date(csv_file):
    importer = sparkasse.CsvCamtImporter(
        iban="irrelevant", account="irrelevant", file_encoding="utf-8"
    )
    booking_date = "01.01.99"
    row = csv_row_to_str(
        make_csv_row(fields=importer.fields, kwargs={"Buchungstag": booking_date})
    )

    with open(csv_file, "a") as f:
        f.write(row)

    with open(csv_file) as f:
        txns = importer.extract(f)
        assert txns[0].date == datetime.strptime(booking_date, "%d.%m.%y").date()


def test_csv_to_txn():
    date_format = "%Y-%m-%d"
    owner_iban = fake_iban()
    booking_date = "2000-01-01"
    posting_type = "KARTENZAHLUNG"
    reference = "reference"
    payee_name = "name"
    payee_iban = fake_iban()
    payee_bic = "bic"
    amount = "1,23"
    currency = "EUR"

    importer = sparkasse.CsvCamtImporter(
        iban=owner_iban, account="irrelevant", date_format=date_format
    )
    csv_row = make_csv_row(
        fields=importer.fields,
        kwargs={
            "Auftragskonto": owner_iban,
            "Buchungstag": booking_date,
            "Buchungstext": posting_type,
            "Verwendungszweck": reference,
            "Beguenstigter/Zahlungspflichtiger": payee_name,
            "Kontonummer/IBAN": payee_iban,
            "BIC (SWIFT-Code)": payee_bic,
            "Betrag": amount,
            "Waehrung": currency,
        },
    )

    txn = importer.csv_to_txn(csv_row=csv_row)
    assert txn.owner_iban == owner_iban
    assert txn.booking_date == datetime.strptime(booking_date, date_format).date()
    assert txn.posting_type == posting_type
    assert txn.reference == reference
    assert txn.payee_name == payee_name
    assert txn.payee_iban == payee_iban
    assert txn.payee_bic == payee_bic
    assert txn.amount == Decimal("1.23")
    assert txn.currency == currency


def test_extract(csv_file):
    date_format = "%Y-%m-%d"
    owner_iban = fake_iban()
    booking_date = "2000-01-01"
    posting_type = "KARTENZAHLUNG"
    reference = "reference"
    payee_name = "name"
    payee_iban = fake_iban()
    payee_bic = "bic"
    amount = "1,23"
    currency = "EUR"

    importer = sparkasse.CsvCamtImporter(
        iban="irrelevant",
        account="irrelevant",
        file_encoding="utf-8",
        date_format=date_format,
    )
    csv_row = csv_row_to_str(
        make_csv_row(
            fields=importer.fields,
            kwargs={
                "Auftragskonto": owner_iban,
                "Buchungstag": booking_date,
                "Buchungstext": posting_type,
                "Verwendungszweck": reference,
                "Beguenstigter/Zahlungspflichtiger": payee_name,
                "Kontonummer/IBAN": payee_iban,
                "BIC (SWIFT-Code)": payee_bic,
                "Betrag": amount,
                "Waehrung": currency,
            },
        )
    )

    with open(csv_file, "a") as f:
        f.write(csv_row)

    with open(csv_file) as f:
        txns = importer.extract(f)
        assert len(txns) == 1
        assert txns[0].date == datetime.strptime(booking_date, date_format).date()
        assert txns[0].narration == reference
        assert txns[0].payee == payee_name
        assert len(txns[0].postings) == 1
        assert txns[0].postings[0].units.number == Decimal("1.23")
        assert txns[0].postings[0].units.currency == currency
