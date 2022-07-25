#!/usr/bin/env python3

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import ClassVar

from beancount_import_sparkasse.importers import BaseImporter
from beancount_import_sparkasse.models import TXN


@dataclass
class CsvCamtImporter(BaseImporter):
    """Beancount importer for CSV-CAMT exports of the German Sparkasse."""

    DEFAULT_FIELDS: ClassVar[tuple[str, ...]] = (
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
    DEFAULT_DELIMITER: ClassVar[str] = ";"
    DEFAULT_QUOTECHAR: ClassVar[str] = '"'

    delimiter: str = DEFAULT_DELIMITER
    quotechar: str = DEFAULT_QUOTECHAR
    fields: Sequence[str] = DEFAULT_FIELDS

    def parse_amount(self, amount: str) -> Decimal:
        """Removes German thousands separator and converts decimal point to US."""
        return Decimal(amount.replace(".", "").replace(",", "."))

    def identify(self, file) -> bool:
        with open(file.name, encoding=self.file_encoding) as f:
            header = f.readline().strip()
            csv_row = f.readline().strip()

        header_match = header == self.expected_header
        iban_match = (
            csv_row.split(self.delimiter)[0].replace(self.quotechar, "") == self.iban
        )
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
            amount=self.parse_amount(csv_row["Betrag"]),
            currency=csv_row["Waehrung"],
        )
        return txn

    def file_name(self, file):
        match = re.search(r"\d{8}-(\d{7})-umsatz", file.name)
        if match:
            return f"{match.group(1)}.camt.csv"
        return None
