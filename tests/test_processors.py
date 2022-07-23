#!/usr/bin/env python3

from datetime import datetime
from decimal import Decimal

import pytest
from utils import (
    fake_amazon_order_number,
    fake_audible_order_number,
    fake_iban,
    random_string,
)

from beancount_import_sparkasse import processors
from beancount_import_sparkasse.models import TXN


def test_process_payee_iban():
    payee_iban = fake_iban()
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type="irrelevant",
        reference="irrelevant",
        payee_name="irrelevant",
        payee_iban=payee_iban,
        payee_bic="irrelevant",
        amount=Decimal("-1.23"),
        currency="EUR",
    )
    processors.process_payee_iban(txn)
    assert txn.meta["payee_iban"] == payee_iban


def test_process_payee_bic():
    payee_bic = "MADJEC23CJE"
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type="irrelevant",
        reference="irrelevant",
        payee_name="irrelevant",
        payee_iban="irrelevant",
        payee_bic=payee_bic,
        amount=Decimal("-1.23"),
        currency="EUR",
    )
    processors.process_payee_bic(txn)
    assert txn.meta["payee_bic"] == payee_bic


def test_process_posting_type():
    posting_type = "KARTENZAHLUNG"
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type=posting_type,
        reference="irrelevant",
        payee_name="irrelevant",
        payee_iban="irrelevant",
        payee_bic="irrelevant",
        amount=Decimal("-1.23"),
        currency="EUR",
    )
    processors.process_posting_type(txn)
    assert txn.meta["posting_type"] == posting_type


def test_process_withdrawal():
    withdrawal_date = "2000-01-01T19:13"
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type="BARGELDAUSZAHLUNG",
        reference=f"{withdrawal_date} Debitk.0 2003-12 ",
        payee_name="EDEKA",
        payee_iban="irrelevant",
        payee_bic="irrelevant",
        amount=Decimal("-1.23"),
        currency="EUR",
    )

    processors.process_withdrawal(txn=txn)
    assert txn.reference == ""
    assert txn.meta["withdrawal_date"] == withdrawal_date


@pytest.mark.parametrize(
    "reference, payee_name, via",
    [
        ("{} Debitk.0 2003-12 ", "SumUp .MACHETE Burrit//Berlin/DE/0", "sumup"),
        ("{} Debitk.0 2003-12 ", "EDEKA THAUT, BERLIN//BERLIN/DE", None),
    ],
)
def test_process_debit_payment(reference, payee_name, via):
    payment_date = "2000-01-01T19:13"
    reference = reference.format(payment_date)
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type="KARTENZAHLUNG",
        reference=reference,
        payee_name=payee_name,
        payee_iban="irrelevant",
        payee_bic="irrelevant",
        amount=Decimal("-1.23"),
        currency="EUR",
    )

    processors.process_debit_payments(txn=txn)
    assert txn.reference == ""
    assert txn.meta["payment_date"] == payment_date
    if via is None:
        assert "via" not in txn.meta
    else:
        assert txn.meta["via"] == via


@pytest.mark.parametrize(
    "order_number, payee_name, reference_name, expected_platform",
    [
        (
            fake_amazon_order_number(),
            "AMAZON EU S.A R.L., NIEDERLASSUNG DEUTSCHLAND",
            "AMZNPrime DE",
            "prime",
        ),
        (
            fake_amazon_order_number(),
            "AMAZON EU S.A R.L., NIEDERLASSUNG DEUTSCHLAND",
            "AMZN Mktp DE",
            "marketplace",
        ),
        (
            fake_amazon_order_number(),
            "AMAZON EU S.A R.L., NIEDERLASSUNG DEUTSCHLAND",
            "Amazon.de",
            None,
        ),
        (fake_audible_order_number(), "AUDIBLE GMBH", "Audible Gmbh", None),
    ],
)
def test_process_amazon(order_number, payee_name, reference_name, expected_platform):
    reference = (
        f"{order_number} {reference_name}"
        f"{random_string(size=16, letters=True, digits=True)}"
    )
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type="FOLGELASTSCHRIFT",
        reference=reference,
        payee_name=payee_name,
        payee_iban="irrelevant",
        payee_bic="irrelevant",
        amount=Decimal("-1.23"),
        currency="EUR",
    )

    processors.process_amazon(txn=txn)
    assert txn.reference == ""
    assert txn.meta["order_number"] == order_number
    if expected_platform:
        assert txn.meta["amazon_platform"] == expected_platform


@pytest.mark.parametrize(
    "reference, payee_name, posting_type, expected_payee, expected_reference, via",
    [
        (
            "PP.4994.PP . URBANSPORTS, Ihr Einkauf bei URBANSPORTS ",
            "PayPal (Europe) S.a.r.l. et Cie., S.C.A.",
            "FOLGELASTSCHRIFT",
            "URBANSPORTS",
            "",
            "paypal",
        ),
        (
            ". ADOBESYSTEM, Ihr Einkauf bei ADOBESYSTEM ",
            "PayPal (Europe) S.a.r.l. et Cie., S.C.A.",
            "FOLGELASTSCHRIFT",
            "ADOBESYSTEM",
            "",
            "paypal",
        ),
        (
            "PP.4994.PP . , Ihr Einkauf bei , Artikel-164272882595 ",
            "PayPal (Europe) S.a.r.l. et Cie., S.C.A.",
            "FOLGELASTSCHRIFT",
            None,
            "Artikel-164272882595",
            "paypal",
        ),
        (
            "AWV-MELDEPFLICHT BEACHTEN HOTLINE BUNDESBANK (0800) 1234-123 ",
            "PAYPAL EUROPE SARL ET CIE SCA                                         \
            22 24 BOULEVARD ROY",
            "GUTSCHR. UEBERWEISUNG",
            None,
            None,
            None,
        ),
    ],
)
def test_process_paypal(
    reference, payee_name, posting_type, expected_payee, expected_reference, via
):
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type=posting_type,
        reference=reference,
        payee_name=payee_name,
        payee_iban="irrelevant",
        payee_bic="irrelevant",
        amount=Decimal("-1.23"),
        currency="EUR",
    )

    processors.process_paypal(txn=txn)
    if via is None:
        assert "via" not in txn.meta
    else:
        assert txn.meta["via"] == via
    if expected_payee is None:
        assert txn.payee_name == payee_name
    else:
        assert txn.payee_name == expected_payee
    if expected_reference is None:
        assert txn.reference == reference
    else:
        assert txn.reference == expected_reference


@pytest.mark.parametrize(
    "reference, payee_name, expected_ticket_number",
    [
        ("something Fahrstein", "DB Vertrieb GmbH", None),
        (
            "Fahrschein 39BYXH ",
            "DB Vertrieb GmbH                              \
            Stephensonstr. 1",
            "39BYXH",
        ),
    ],
)
def test_process_db(reference, payee_name, expected_ticket_number):
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type="irrelevant",
        reference=reference,
        payee_name=payee_name,
        payee_iban="irrelevant",
        payee_bic="irrelevant",
        amount=Decimal("-1.23"),
        currency="EUR",
    )
    processors.process_db(txn)
    assert txn.payee_name == "DB Vertrieb GmbH"
    if expected_ticket_number is None:
        assert "ticket_number" not in txn.meta
    else:
        assert txn.meta["ticket_number"] == expected_ticket_number


def process_telefonica():
    reference = "Tarifrechnung"
    invoice_number = "1234567890/1"
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type="irrelevant",
        reference=f"Kd-Nr.: 1234567890, Rg-Nr.: {invoice_number}, Ihre {reference} ",
        payee_name="Telefonica Germany GmbH + Co. OHG",
        payee_iban="irrelevant",
        payee_bic="irrelevant",
        amount=Decimal("-1.23"),
        currency="EUR",
    )
    processors.process_telefonica(txn)
    assert txn.reference == reference
    assert txn.meta["invoice_number"] == invoice_number


def process_telecolumbus():
    invoice_number = "123456789"
    txn = TXN(
        owner_iban="irrelevant",
        booking_date=datetime.strptime("2000-01-01", "%Y-%m-%d"),
        posting_type="irrelevant",
        reference=(
            f"KD-NR . 00001234567, RG-NR . {invoice_number}, "
            f"Faelligkeit 01 . 01 . 2001, Mandats-ID 1234567, Glaeubiger-ID "
            f"DE12ZZZ00000123456 "
        ),
        payee_name=(
            "Tele Columbus Berlin-Brandenburg GmbH + Co . KG         "
            "              Kaiserin-Augusta-Allee 108"
        ),
        payee_iban="irrelevant",
        payee_bic="irrelevant",
        amount=Decimal("-1.23"),
        currency="EUR",
    )
    processors.process_telefonica(txn)
    assert txn.reference == ""
    assert txn.meta["invoice_number"] == invoice_number
    assert txn.payee_name == "Tele Columbus Berlin-Brandenburg GmbH + Co . KG"
