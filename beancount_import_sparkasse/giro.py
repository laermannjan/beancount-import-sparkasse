#!/usr/bin/env python3

import csv
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime

from beancount.core.amount import Amount
from beancount.core.data import EMPTY_SET, Decimal, Posting, Transaction, new_metadata
from beancount.ingest.importer import ImporterProtocol

DEFAULT_FIELDS = OrderedDict(
    account="Auftragskonto",
    booking_date="Buchungstag",
    value_date="Valutadatum",
    posting_text="Buchungstext",
    reference="Verwendungszweck",
    creditor="Glaeubiger ID",
    mandate_reference="Mandatsreferenz",
    customer_refernce="Kundenreferenz (End-to-End)",
    bulk_reference="Sammlerreferenz",
    debit_charge_original_amount="Lastschrift Ursprungsbetrag",
    reimbursement_return_debit="Auslagenersatz Ruecklastschrift",
    payee="Beguenstigter/Zahlungspflichtiger",
    iban="Kontonummer/IBAN",
    bic="BIC (SWIFT-Code)",
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

    def __post_init__(self):
        self.expected_header = ";".join(
            [f'"{field}"' for field in self.fields.values()]
        )

    def _fmt_amount(self, amount: str) -> Decimal:
        """Removes German thousands separator and converts decimal point to US."""
        return Decimal(amount.replace(",", "").replace(".", ","))

    def name(self):
        """Return a unique id/name for this importer.

        Returns:
          A string which uniquely identifies this importer.
        """
        cls = self.__class__
        return "{}.{}".format(cls.__module__, cls.__name__)

    __str__ = name

    def identify(self, file) -> bool:
        """Return true if this importer matches the given file.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          A boolean, true if this importer can handle this file.
        """
        with open(file, encoding=self.file_encoding) as f:
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
        with open(file, encoding=self.file_encoding) as f:
            transactions = csv.DictReader(
                f, fieldnames=list(self.fields.keys()), delimiter=";", quotechar='"'
            )
            extracted_transactions = []
            for i, txn in enumerate(transactions):
                if i == 0:
                    # skip header
                    continue
                meta = new_metadata(filename=file.name, lineno=i + 1)
                date = datetime.strptime(txn["booking_date"], self.date_format).date()
                amount = Amount(
                    number=self._fmt_amount(txn["amount"]),
                    currency=txn["currency"],
                )
                extracted_txn = Transaction(
                    meta=meta,
                    date=date,
                    flag=self.FLAG,
                    payee=txn["payee"],
                    narration=txn["reference"],
                    tags=EMPTY_SET,
                    links=EMPTY_SET,
                    postings=[
                        Posting(
                            account=self.account,
                            units=amount,  # type: ignore
                            cost=None,
                            price=None,
                            flag=None,
                            meta=None,
                        )
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

    def file_date(self, file):
        """Attempt to obtain a date that corresponds to the given file.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          A date object, if successful, or None if a date could not be extracted.
          (If no date is returned, the file creation time is used. This is the
          default.)
        """
