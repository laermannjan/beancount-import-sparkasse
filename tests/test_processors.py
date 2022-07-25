#!/usr/bin/env python3

from datetime import datetime
from decimal import Decimal
from textwrap import dedent

import pytest
import yaml

from beancount_import_sparkasse import processors
from beancount_import_sparkasse.models import TXN


@pytest.mark.parametrize(
    "file_content",
    [
        """
    Expenses:
      Housing:
        Rent:
          - payee_name: Landlord
    """,
        """
    Expenses:
      Housing:
        Rent:
          - payee_name: Landlord
            reference: rent
    """,
        """
    Expenses:
      Housing:
        Rent:
          - payee_iban: landlord_iban
    """,
        """
    Expenses:
      Housing:
        Rent:
          - payee_name: Landlord
            reference: rent
          - payee_iban: landlord_iban
    """,
    ],
)
def test_account_processor(file_content):
    rule_sets = processors.flatten_dict(yaml.safe_load(file_content))
    account_processor = processors.AccountProcessor(rule_sets=rule_sets)

    affected_txn = TXN(
        owner_iban="owner_iban",
        booking_date=datetime.now().date(),  # type: ignore
        posting_type="posting_type",
        reference="rent",
        payee_name="Mr Landlord and Mrs Landlordess",
        payee_iban="landlord_iban",
        payee_bic="payee_bic",
        amount=Decimal("-1.23"),
        currency="EUR",
    )

    augmented_txn = account_processor(affected_txn)
    assert augmented_txn.induced_postings[0].account == "Expenses:Housing:Rent"
    assert augmented_txn.induced_postings[0].flag == "!"

    unaffected_txn = TXN(
        owner_iban="owner_iban",
        booking_date=datetime.now().date(),  # type: ignore
        posting_type="posting_type",
        reference="something special",
        payee_name="someone else",
        payee_iban="someone_elses_iban",
        payee_bic="payee_bic",
        amount=Decimal("-1.23"),
        currency="EUR",
    )
    still_unaffected_txn = account_processor(unaffected_txn)
    assert still_unaffected_txn == unaffected_txn


@pytest.mark.parametrize(
    "file_content, expected_meta",
    [
        (
            """
            zero:
                something:
                    - payee_name: 'payee_name_affected'
            """,
            {"zero": "SOMETHING"},
        ),
        (
            """
            one:
                something:
                    - payee_name: 'something that doesnt match'
                    - reference: 'reference_affected'
            """,
            {"one": "SOMETHING"},
        ),
        (
            """
            two:
                something:
                    - payee_name: 'something that doesnt match'
                      reference: 'reference_affected'
            """,
            {},
        ),
        (
            """
            three:
                something:
                    - payee_name: 'payee_name_affected'
                      reference: 'reference_affected'
            """,
            {"three": "SOMETHING"},
        ),
        (
            """
            four:
                - payee_name: '(?P<meta>payee_name)_affected'
            """,
            {"four": "PAYEE_NAME"},
        ),
        (
            """
            five:
                - reference: '(?P<meta>reference)_affected'
                - payee_iban: '(?P<meta>payee_iban)_affected'
                - payee_name: '(?P<meta>payee_name)_affected'
            """,
            {"five": "PAYEE_NAME"},
        ),
        (
            """
            six:
                - reference: '(?P<meta>reference)_affected'
                  payee_iban: '(?P<meta>payee_iban)_affected'
                  payee_name: '(?P<meta>payee_name_affected)'
            """,
            {"six": "REFERENCE PAYEE_IBAN PAYEE_NAME_AFFECTED"},
        ),
        (
            """
            seven:
                - payee_name: '(payee)_name'
            """,
            {},
        ),
    ],
)
def test_meta_processor_metatags(file_content, expected_meta):
    rule_sets = processors.flatten_dict(yaml.safe_load(dedent(file_content)))
    account_processor = processors.MetaProcessor(rule_sets=rule_sets)

    txn = TXN(
        owner_iban="owner_iban_affected",
        booking_date=datetime.now().date(),  # type: ignore
        posting_type="posting_type_affected",
        reference="reference_affected",
        payee_name="payee_name_affected",
        payee_iban="payee_iban_affected",
        payee_bic="payee_bic_affected",
        amount=Decimal("-1.23"),
        currency="EUR_affected",
    )

    augmented_txn = account_processor(txn)
    assert augmented_txn.meta == expected_meta

    unaffected_txn = TXN(
        owner_iban="unaffected_owner_iban",
        booking_date=datetime.now().date(),  # type: ignore
        posting_type="unaffected_posting_type",
        reference="unaffected_reference",
        payee_name="unaffected_payee_name",
        payee_iban="unaffected_payee_iban",
        payee_bic="unaffected_payee_bic",
        amount=Decimal("3.21"),
        currency="unaffected_EUR",
    )
    still_unaffected_txn = account_processor(unaffected_txn)
    assert still_unaffected_txn == unaffected_txn


@pytest.mark.parametrize(
    "file_content, expected_attrs",
    [
        (
            """
            zero:
                something:
                    - payee_name: '(?P<payee_name>payee_name)_affected'
            """,
            {"payee_name": "payee_name"},
        ),
        (
            """
            one:
                paypal:
                    - reference: 'reference_(?P<reference>affected)'
            """,
            {"reference": "affected"},
        ),
        (
            """
            two:
                - payee_iban: '(?P<reference>payee_iban)_affected'
            """,
            {"reference": "payee_iban"},
        ),
        (
            """
            three:
                - owner_iban: '(?P<payee_iban>owner_iban)_affected'
                  payee_iban: '(?P<owner_iban>payee_iban)_affected'
            """,
            {"owner_iban": "payee_iban", "payee_iban": "owner_iban"},
        ),
        (
            """
            four:
                - owner_iban: '(?P<reference>owner_iban)_affected'
                - payee_iban: '(?P<reference>payee_iban)_affected'
            """,
            {"reference": "payee_iban"},
        ),
        (
            """
            five:
                - owner_iban: '(?P<reference>owner_iban)_affected'
                  payee_iban: '(?P<reference>payee_iban)_affected'
            """,
            {"reference": "owner_iban payee_iban"},
        ),
        (
            """
            six:
                - ona_iban: '(?P<reference>owner_iban)_affected'
            """,
            None,
        ),
        (
            """
            seven:
                - owner_iban: '(?P<rfrnce>owner_iban)_affected'
            """,
            None,
        ),
    ],
)
def test_meta_processor_txnattributes(file_content, expected_attrs):
    rule_sets = processors.flatten_dict(yaml.safe_load(file_content))
    account_processor = processors.MetaProcessor(rule_sets=rule_sets)

    txn = TXN(
        owner_iban="owner_iban_affected",
        booking_date=datetime.now().date(),  # type: ignore
        posting_type="posting_type_affected",
        reference="reference_affected ",
        payee_name="payee_name_affected",
        payee_iban="payee_iban_affected",
        payee_bic="payee_bic_affected",
        amount=Decimal("-1.23"),
        currency="EUR_affected",
    )

    # if isinstance(expected_attrs, ValueError):
    if expected_attrs is None:
        with pytest.raises(expected_exception=ValueError):
            account_processor(txn)
        return

    augmented_txn = account_processor(txn)
    assert expected_attrs.items() <= augmented_txn.__dict__.items()

    unaffected_txn = TXN(
        owner_iban="unaffected_owner_iban",
        booking_date=datetime.now().date(),  # type: ignore
        posting_type="unaffected_posting_type",
        reference="unaffected_reference",
        payee_name="unaffected_payee_name",
        payee_iban="unaffected_payee_iban",
        payee_bic="unaffected_payee_bic",
        amount=Decimal("-1.23"),
        currency="unaffected_EUR",
    )
    still_unaffected_txn = account_processor(unaffected_txn)
    assert still_unaffected_txn == unaffected_txn
