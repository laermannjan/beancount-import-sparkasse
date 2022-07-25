#!/usr/bin/env python3

import csv
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from beancount.core.data import EMPTY_SET, Amount, Posting, Transaction, new_metadata
from beancount.ingest.importer import ImporterProtocol

from beancount_import_sparkasse.models import TXN


@dataclass
class BaseImporter(ABC, ImporterProtocol):
    iban: str
    account: str
    fields: Sequence[str]
    date_format: str = "%d.%m.%y"
    currency: str = "EUR"
    file_encoding: str = "ISO-8859-1"
    delimiter: str = ";"
    quotechar: str = '"'

    @abstractmethod
    def parse_amount(self, amount: str) -> Decimal:
        ...

    @abstractmethod
    def csv_to_txn(self, csv_row: dict[str, str]) -> TXN:
        ...

    @property
    def expected_header(self) -> str:
        return self.delimiter.join(
            [f"{self.quotechar}{field}{self.quotechar}" for field in self.fields]
        )

    def extract(self, file, existing_entries=None) -> list[Transaction]:  # type: ignore
        with open(file.name, encoding=self.file_encoding) as f:
            csv_rows = csv.DictReader(
                f, delimiter=self.delimiter, quotechar=self.quotechar
            )
            extracted_transactions = []
            for i, row in enumerate(csv_rows):

                txn = self.csv_to_txn(csv_row=row)

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

    def file_account(self, _):
        return self.account

    def file_date(self, file):
        return max(map(lambda entry: entry.date, self.extract(file)))


def make_posting(
    amount: Decimal | None,
    currency: str | None,
    account: str,
    flag: str | None = None,
):
    if amount:
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
        date=txn.booking_date,
        flag=flag,
        payee=txn.payee_name,
        narration=txn.reference,
        tags=EMPTY_SET,
        links=EMPTY_SET,
        postings=postings,
    )
    return t
