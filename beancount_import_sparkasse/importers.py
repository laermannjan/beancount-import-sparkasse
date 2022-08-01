#!/usr/bin/env python3

import csv
import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from beancount.core.data import (
    EMPTY_SET,
    Amount,
    Balance,
    Directive,
    Posting,
    Transaction,
    new_metadata,
)
from beancount.ingest.importer import ImporterProtocol

from beancount_import_sparkasse.models import TXN

logger = logging.getLogger(__name__)


@dataclass
class BaseImporter(ABC, ImporterProtocol):
    iban: str
    account: str
    fields: Sequence[str]
    date_format: str
    first_data_row: int = 1
    currency: str = "EUR"
    file_encoding: str = "ISO-8859-1"
    delimiter: str = ";"
    quotechar: str = '"'
    dates_ascending: bool = True

    @abstractmethod
    def parse_amount(self, amount: str) -> Decimal:
        ...

    @abstractmethod
    def csv_to_txn(self, csv_row: dict[str, str]) -> TXN:
        ...

    @abstractmethod
    def get_final_balance(self, file) -> Directive | None:
        ...

    @property
    def expected_header(self) -> str:
        return self.delimiter.join(
            [f"{self.quotechar}{field}{self.quotechar}" for field in self.fields]
        )

    def extract(self, file, existing_entries=None) -> list[Transaction]:  # type: ignore
        with open(file.name, encoding=self.file_encoding) as f:
            csv_rows = csv.DictReader(
                f,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                fieldnames=self.fields,
            )
            extracted_directives = []
            header_parsed = False
            for i, row in enumerate(csv_rows):
                if None in row:
                    del row[None]
                logger.debug(f"looking at {row=}")
                if not header_parsed:
                    if list(row.values()) != list(self.fields):
                        logger.debug("head NOT found")
                        continue
                    header_parsed = True
                    logger.debug("header FOUND, continue one more time")
                    continue

                logger.debug(f"Parsing {row=}")
                txn = self.csv_to_txn(csv_row=row)
                logger.debug(f"Converted to {txn=}")

                transaction = make_transaction(
                    account=self.account,
                    txn=txn,
                    fname=file.name,
                    lineno=i + 2,
                    flag=self.FLAG,
                )
                logger.info(f"New {transaction=}")
                extracted_directives.append(transaction)

        final_balance = self.get_final_balance(file=file)
        if final_balance:
            logger.info(f"New {final_balance=}")
            extracted_directives.append(final_balance)

        return extracted_directives

    def file_account(self, _):
        return self.account

    def file_date(self, file):
        return max(map(lambda entry: entry.date, self.extract(file)))


@dataclass
class SparkasseCsvCamtImporter(BaseImporter):
    """Beancount importer for CSV-CAMT exports of the German Sparkasse."""

    date_format: str = "%d.%m.%y"
    delimiter: str = ";"
    quotechar: str = '"'
    fields: Sequence[str] = (
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

    def parse_amount(self, amount: str) -> Decimal:
        """Removes German thousands separator and converts decimal point to US."""
        return Decimal(amount.replace(".", "").replace(",", "."))

    def get_final_balance(self, file) -> Directive | None:
        return None

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
            date=datetime.strptime(
                csv_row["Valutadatum"], self.date_format
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


@dataclass
class DKBCsvImporter(BaseImporter):
    """Beancount importer for CSV exports of the German DKB."""

    date_format: str = "%d.%m.%Y"
    delimiter: str = ";"
    quotechar: str = '"'
    fields: Sequence[str] = (
        "Buchungstag",
        "Wertstellung",
        "Buchungstext",
        "Auftraggeber / Begünstigter",
        "Verwendungszweck",
        "Kontonummer",
        "BLZ",
        "Betrag (EUR)",
        "Gläubiger-ID",
        "Mandatsreferenz",
        "Kundenreferenz",
    )

    def parse_amount(self, amount: str) -> Decimal:
        """Removes German thousands separator and converts decimal point to US."""
        return Decimal(amount.replace(".", "").replace(",", "."))

    def get_final_balance(self, file) -> tuple[datetime, Decimal] | None:
        with open(file.name, encoding=self.file_encoding) as f:
            for i, line in enumerate(f.readlines()):
                regex = r'Kontostand vom (\d+.\d+.\d+):";"([\d.,]+) (\w+)";'
                logger.debug(f"Trying to match {regex=} in {line=}")
                match = re.search(regex, line)
                if match:
                    bal_date = datetime.strptime(match.group(1), self.date_format)
                    amount = Decimal(self.parse_amount(match.group(2)))
                    currency = match.group(3)
                    final_balance = make_balance(
                        fname=file.name,
                        lineno=i + 1,
                        date=bal_date,
                        account=self.account,
                        currency=currency,
                        amount=amount,
                    )
                    return final_balance

    def identify(self, file) -> bool:
        logger.info(f"Looking at {file.name}")
        with open(file.name, encoding=self.file_encoding) as f:
            while line := f.readline():
                line = line.strip()[:-1]
                regex = r'"Kontonummer:";"(\w+) / Girokonto'
                logger.debug(f"Trying to match {regex=} in {line=}")
                match = re.search(regex, line)
                if match:
                    iban = match.group(1)
                    if iban == self.iban:
                        logger.info(f"{iban=} found.")
                        break
                    else:
                        logger.debug(f"{iban=} != {self.iban}")

            while line := f.readline():
                line = line.strip()[:-1]
                logger.debug(f"Expected header {self.expected_header}")
                logger.debug(f"Current line    {line}")
                if line == self.expected_header:
                    logger.info("Header matched.")
                    return True
        return False

    def csv_to_txn(self, csv_row: dict[str, str]):
        txn = TXN(
            owner_iban="",
            date=datetime.strptime(
                csv_row["Wertstellung"], self.date_format
            ).date(),  # type: ignore
            posting_type=csv_row["Buchungstext"],
            reference=csv_row["Verwendungszweck"],
            payee_name=csv_row["Auftraggeber / Begünstigter"],
            payee_iban=csv_row["Kontonummer"],
            payee_bic=csv_row["BLZ"],
            amount=self.parse_amount(csv_row["Betrag (EUR)"]),
            currency="EUR",
        )
        return txn

    def file_name(self, file):
        match = re.search(r"(\d+)", file.name)
        if match:
            return f"{match.group(1)}.csv"
        return None


@dataclass
class GLSCsvImporter(BaseImporter):
    """Beancount importer for CSV exports of the German DKB."""

    date_format: str = "%d.%m.%Y"
    delimiter: str = ";"
    quotechar: str = ""
    fields: Sequence[str] = (
        "Bezeichnung Auftragskonto",
        "IBAN Auftragskonto",
        "BIC Auftragskonto",
        "Bankname Auftragskonto",
        "Buchungstag",
        "Valutadatum",
        "Name Zahlungsbeteiligter",
        "IBAN Zahlungsbeteiligter",
        "BIC (SWIFT-Code) Zahlungsbeteiligter",
        "Buchungstext",
        "Verwendungszweck",
        "Betrag",
        "Waehrung",
        "Saldo nach Buchung",
        "Bemerkung",
        "Kategorie",
        "Steuerrelevant",
        "Glaeubiger ID",
        "Mandatsreferenz",
    )

    def parse_amount(self, amount: str) -> Decimal:
        """Removes German thousands separator and converts decimal point to US."""
        return Decimal(amount.replace(".", "").replace(",", "."))

    def get_final_balance(self, file) -> Directive | None:
        with open(file, encoding=self.file_encoding) as f:
            csv_rows = csv.DictReader(
                f, delimiter=self.delimiter, quotechar=self.quotechar
            )
            csv_row = next(csv_rows)
            return make_balance(
                fname=file.name,
                lineno=2,
                date=datetime.strptime(csv_row["Buchungstag"], self.date_format),
                account=self.account,
                currency=csv_row["Waehrung"],
                amount=Decimal(self.parse_amount(csv_row["Saldo nach Buchung"])),
            )

    def identify(self, file) -> bool:
        with open(file.name, encoding=self.file_encoding) as f:
            for _ in range(10):
                header = f.readline().strip()
                if header == self.expected_header:
                    csv_row = f.readline().strip()
                    return (
                        csv_row.split(self.delimiter)[0].replace(self.quotechar, "")
                        == self.iban
                    )
        return False

    def csv_to_txn(self, csv_row: dict[str, str]):
        txn = TXN(
            owner_iban="IBAN Auftragskonto",
            date=datetime.strptime(
                csv_row["Valutadatum"], self.date_format
            ).date(),  # type: ignore
            posting_type=csv_row["Buchungstext"],
            reference=csv_row["Verwendungszweck"],
            payee_name=csv_row["Name Zahlungsbeteiligter"],
            payee_iban=csv_row["IBAN Zahlungsbeteiligter"],
            payee_bic=csv_row["BIC (SWIFT-Code) Zahlungsbeteiligter"],
            amount=self.parse_amount(csv_row["Betrag"]),
            currency=csv_row["Waehrung"],
        )
        return txn

    def file_name(self, file):
        match = re.search(r"(\d+)", file.name)
        if match:
            return f"{match.group(1)}.csv"
        return None


def make_transaction(
    account: str, txn: TXN, fname: str, lineno: int, flag: str
) -> Transaction:  # type: ignore
    postings = [make_posting(account=account, amount=txn.amount, currency=txn.currency)]
    for posting in txn.induced_postings:
        postings.append(
            make_posting(
                account=posting.account, amount=None, currency=None, flag=posting.flag
            )
        )

    t = Transaction(
        meta=new_metadata(filename=fname, lineno=lineno, kvlist=txn.meta),
        date=txn.date,
        flag=flag,
        payee=txn.payee_name,
        narration=txn.reference,
        tags=EMPTY_SET,
        links=EMPTY_SET,
        postings=postings,
    )
    return t


def make_posting(
    amount: Decimal | None,
    currency: str | None,
    account: str,
    flag: str | None = None,
):
    if amount is not None:
        units = Amount(number=amount, currency=currency)
    else:
        units = None
    posting = Posting(
        account=account,
        units=units,  # type: ignore
        cost=None,
        price=None,
        flag=flag,
        meta=None,
    )
    return posting


def make_balance(
    fname: str,
    lineno: int,
    date: datetime,
    account: str,
    currency: str,
    amount: Decimal,
):
    return Balance(
        meta=new_metadata(filename=fname, lineno=lineno),
        date=date.date() + timedelta(days=1),
        account=account,
        amount=Amount(number=amount, currency=currency),
        tolerance=None,
        diff_amount=None,
    )
