#!/usr/bin/env python3

import csv
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Sequence

from beancount.core.amount import Amount
from beancount.core.data import EMPTY_SET, Decimal, Posting, Transaction, new_metadata
from beancount.ingest.importer import ImporterProtocol

DEFAULT_FIELDS = OrderedDict(
    account="Auftragskonto",
    booking_date="Buchungstag",
    value_date="Valutadatum",
    posting_type="Buchungstext",
    reference="Verwendungszweck",
    creditor="Glaeubiger ID",
    mandate_reference="Mandatsreferenz",
    customer_refernce="Kundenreferenz (End-to-End)",
    bulk_reference="Sammlerreferenz",
    debit_charge_original_amount="Lastschrift Ursprungsbetrag",
    reimbursement_return_debit="Auslagenersatz Ruecklastschrift",
    payee_name="Beguenstigter/Zahlungspflichtiger",
    payee_iban="Kontonummer/IBAN",
    payee_bic="BIC (SWIFT-Code)",
    amount="Betrag",
    currency="Waehrung",
    info="Info",
)


@dataclass
class SparkasseCSVCAMTImporter(ImporterProtocol):
    """Beancount importer for CSV-CAMT exports of the German Sparkasse."""

    iban: str
    account: str
    currency: str = "EUR"
    date_format: str = "%d.%m.%y"
    file_encoding: str = "ISO-8859-1"
    fields: OrderedDict[str, str] = field(default_factory=lambda: DEFAULT_FIELDS)
    meta_payee_iban: bool = True
    meta_via: bool = True
    meta_order_id: bool = True
    meta_posting_type: bool = True
    clean_strings: bool = True
    payee_sumup_regex: str = r"SumUp \.(\w.*//.+/.+)/\d+"
    ref_paypal_regex: str = r"Ihr Einkauf bei (\w+)"
    ref_amazon_order_regex: str = r"(\w\d{2}-\d{7}-\d{7}) (\w.+) \w{16}"
    ref_excluded_literal: Sequence[str] = ()
    ref_excluded_regex: Sequence[str] = ()
    add_dummy_expense: bool = True

    def __post_init__(self):
        self.expected_header = ";".join(
            [f'"{field}"' for field in self.fields.values()]
        )

    def _fmt_amount(self, amount: str) -> Decimal:
        """Removes German thousands separator and converts decimal point to US."""
        return Decimal(amount.replace(".", "").replace(",", "."))

    def _parse_paypal(self, txn) -> None:
        """Retrieves the payee/debitor behind a PayPal transaction.

        Checks whether the stated payee is "PayPal".
        If the transaction reference also contains the
        `self.paypal_reference_identifier` string, the actual
        payee/debitor is the next string slice.

        This works as paypal usually uses standardized references like:
        "somehting . PP 3925 PP somthing your order at payee_name"
        In the same case and if `self.meta_via` is True, a "via" meta tag
        will also be added, noting that the transaction was wired via paypal.
        """
        if txn["payee"].startswith("PayPal"):
            match = re.search(self.ref_paypal_regex, txn["narration"])
            if match:
                if self.meta_via:
                    txn["meta"]["via"] = "paypal"
                txn["payee"] = match.group(1).strip()
                txn["narration"] = ""

    def _parse_sumup(self, txn) -> None:
        match = re.search(self.payee_sumup_regex, txn["payee"])
        if match:
            if self.meta_via:
                txn["meta"]["via"] = "sumup"
            txn["payee"] = match.group(1)
            txn["narration"] = ""

    def _parse_amazon(self, txn) -> None:
        """Parse referece for order id and amazon name"""
        match = re.search(self.ref_amazon_order_regex, txn["narration"])
        if match:
            txn["meta"]["order_number"] = match.group(1)
            txn["payee"] = match.group(2)

    def _excluded_narration_maybe(self, txn) -> None:
        for excluded_str in self.ref_excluded_literal:
            if excluded_str.lower() in txn["narration"].lower():
                txn["narration"] = ""
                return

        for excluded_regex in self.ref_excluded_regex:
            if re.search(excluded_regex, txn["narration"]):
                txn["narration"] = ""
                return

    def _clean(self, s: str) -> str:
        return " ".join(s.split())

    def identify(self, file) -> bool:
        """Return true if this importer matches the given file.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          A boolean, true if this importer can handle this file.
        """
        with open(file.name, encoding=self.file_encoding) as f:
            header = f.readline().strip()
        return header.lower() == self.expected_header.lower()

    def extract(self, file, existing_entries=None):
        """Extract transactions from a file.

        If the importer would like to flag a returned transaction as a known
        duplicate, it may opt to set the special flag "__duplicate__" to True,
        and the transaction should be treated as a duplicate by the extraction
        code. This is a way to let the importer use particular information about
        previously imported transactions in order to flag them as duplicates.
        For example, if an importer has a way to get a persistent unique id for
        each of the imported transactions. (See this discussion for context:
        https://groups.google.com/d/msg/beancount/0iV-ipBJb8g/-uk4wsH2AgAJ)

        Args:
          file: A cache.FileMemo instance.
          existing_entries: An optional list of existing directives loaded from
            the ledger which is intended to contain the extracted entries. This
            is only provided if the user provides them via a flag in the
            extractor program.
        Returns:
          A list of new, imported directives (usually mostly Transactions)
          extracted from the file.
        """
        with open(file.name, encoding=self.file_encoding) as f:
            transactions = csv.DictReader(
                f, fieldnames=list(self.fields.keys()), delimiter=";", quotechar='"'
            )
            extracted_transactions = []
            for i, txn in enumerate(transactions):
                if i == 0:
                    # skip header
                    continue

                # base values
                txn_kwargs = dict(
                    meta=dict(),
                    date=datetime.strptime(
                        txn["booking_date"], self.date_format
                    ).date(),
                    payee=txn["payee_name"],
                    narration=txn["reference"],
                )

                # augmentation
                if self.meta_payee_iban and txn["payee_iban"]:
                    txn_kwargs["meta"]["payee_iban"] = txn["payee_iban"]

                if self.payee_sumup_regex:
                    self._parse_sumup(txn=txn_kwargs)

                if self.ref_paypal_regex:
                    self._parse_paypal(txn=txn_kwargs)

                if self.ref_amazon_order_regex:
                    self._parse_amazon(txn=txn_kwargs)

                self._excluded_narration_maybe(txn=txn_kwargs)

                if self.meta_posting_type and txn["posting_type"]:
                    txn_kwargs["meta"]["posting_type"] = txn["posting_type"]

                extracted_txn = Transaction(
                    meta=new_metadata(
                        filename=file.name, lineno=i + 1, kvlist=txn_kwargs["meta"]
                    ),
                    date=txn_kwargs["date"],
                    flag=self.FLAG,
                    payee=self._clean(txn_kwargs["payee"]),
                    narration=self._clean(txn_kwargs["narration"]),
                    tags=EMPTY_SET,
                    links=EMPTY_SET,
                    postings=[
                        Posting(
                            account=self.account,
                            units=Amount(
                                number=self._fmt_amount(txn["amount"]),
                                currency=txn["currency"],
                            ),
                            cost=None,
                            price=None,
                            flag=None,
                            meta=None,
                        ),
                        Posting(
                            account="Expenses:Dummy",
                            units=None,
                            cost=None,
                            price=None,
                            flag=None,
                            meta=None,
                        ),
                    ],
                )
                extracted_transactions.append(extracted_txn)
        return extracted_transactions

    def file_account(self, file):
        """Return an account associated with the given file.

        Note: If you don't implement this method you won't be able to move the
        files into its preservation hierarchy; the bean-file command won't
        work.

        Also, normally the returned account is not a function of the input
        file--just of the importer--but it is provided anyhow.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          The name of the account that corresponds to this importer.
        """
        return self.account

    def file_name(self, file):
        """A filter that optionally renames a file before filing.

        This is used to make tidy filenames for filed/stored document files. If
        you don't implement this and return None, the same filename is used.
        Note that if you return a filename, a simple, RELATIVE filename must be
        returned, not an absolute filename.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          The tidied up, new filename to store it as.
        """
        match = re.search(r"\d{8}-(\d{7})-umsatz", file.name)
        if match:
            return f"{match.group(1)}.camt.csv"
        return None

    def file_date(self, file):
        """Attempt to obtain a date that corresponds to the given file.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          A date object, if successful, or None if a date could not be extracted.
          (If no date is returned, the file creation time is used. This is the
          default.)
        """
        return max(map(lambda entry: entry.date, self.extract(file)))
