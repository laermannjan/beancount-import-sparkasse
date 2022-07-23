#!/usr/bin/env python3

import csv
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from beancount.ingest.importer import ImporterProtocol

from beancount_import_sparkasse.models import TXN, Importer
from beancount_import_sparkasse.utils import make_transaction

DEFAULT_FIELDS = (
    "Auftragskonto",
    "Buchungstag",
    "Valutadatum",
    "Buchungstext",
    "Verwendungszweck",
    "Glaeubiger ID",
    "Mandatsreferenz",
    "Kundenreferenz (End-to-End)",
    "Sammlerreferenz",
    "Lastschrift Ursprungsbetrag",
    "Auslagenersatz Ruecklastschrift",
    "Beguenstigter/Zahlungspflichtiger",
    "Kontonummer/IBAN",
    "BIC (SWIFT-Code)",
    "Betrag",
    "Waehrung",
    "Info",
)


@dataclass
class SparkasseCSVCAMTImporter(ImporterProtocol, Importer):
    """Beancount importer for CSV-CAMT exports of the German Sparkasse."""

    iban: str
    account: str
    currency: str = "EUR"
    date_format: str = "%d.%m.%y"
    file_encoding: str = "ISO-8859-1"
    fields: Sequence[str] = DEFAULT_FIELDS
    process_callbacks: Sequence[Callable[[TXN], None]] = ()
    dummy_account: str = "Expenses:Dummy"

    def _fmt_amount(self, amount: str) -> Decimal:
        """Removes German thousands separator and converts decimal point to US."""
        return Decimal(amount.replace(".", "").replace(",", "."))

    def identify(self, file) -> bool:
        """Return true if this importer matches the given file.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          A boolean, true if this importer can handle this file.
        """
        with open(file.name, encoding=self.file_encoding) as f:
            header = f.readline().strip()
            csv_row = f.readline().strip()

        expected_header = ";".join([f'"{field}"' for field in self.fields])

        header_match = header == expected_header
        iban_match = csv_row.split(";")[0].replace('"', "") == self.iban
        return header_match and iban_match

    def csv_to_txn(self, csv_row: dict[str, str]):
        txn = TXN(
            owner_iban=csv_row["Auftragskonto"],
            booking_date=datetime.strptime(
                csv_row["Buchungstag"], self.date_format
            ).date(),  # type: ignore
            posting_type=csv_row["Buchungstext"],
            reference=csv_row["Verwendungszweck"],
            payee_name=csv_row["Beguenstigter/Zahlungspflichtiger"],
            payee_iban=csv_row["Kontonummer/IBAN"],
            payee_bic=csv_row["BIC (SWIFT-Code)"],
            amount=self._fmt_amount(csv_row["Betrag"]),
            currency=csv_row["Waehrung"],
        )
        return txn

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
            csv_rows = csv.DictReader(f, delimiter=";", quotechar='"')
            extracted_transactions = []
            for i, row in enumerate(csv_rows):

                txn = self.csv_to_txn(csv_row=row)
                for cb in self.process_callbacks:
                    cb(txn)

                extracted_transactions.append(
                    make_transaction(
                        account=self.account,
                        txn=txn,
                        fname=file.name,
                        lineno=i,
                        flag=self.FLAG,
                    )
                )
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
