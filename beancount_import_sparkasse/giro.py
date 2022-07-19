#!/usr/bin/env python3

from dataclasses import dataclass

from beancount.ingest.importer import ImporterProtocol

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
class SparkasseCSVCAMTImporter(ImporterProtocol):
    """Beancount importer for CSV-CAMT exports of the German Sparkasse."""

    iban: str
    account: str
    currency: str = "EUR"
    date_format: str = "%d.%m.%y"
    file_encoding: str = "ISO-8859-1"
    fields: tuple[str, ...] = DEFAULT_FIELDS

    def __post_init__(self):
        self.expected_header = ";".join([f'"{field}"' for field in self.fields])

    def identify(self, file) -> bool:
        """Return True on a file that can be imported via this importer."""
        with open(file, encoding=self.file_encoding) as f:
            header = f.readline().strip()
        return header.lower() == self.expected_header.lower()
