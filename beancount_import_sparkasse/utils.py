#!/usr/bin/env python3

from decimal import Decimal

from beancount.core.data import EMPTY_SET, Amount, Posting, Transaction, new_metadata

from beancount_import_sparkasse.models import TXN


def make_posting(
    amount: Decimal | None,
    currency: str | None,
    account: str,
    flag: str | None = None,
):
    posting = Posting(
        account=account,
        units=Amount(number=amount, currency=currency) if amount else None,
        cost=None,
        price=None,
        flag=flag,
        meta=None,
    )
    return posting


def make_transaction(
    account: str, txn: TXN, fname: str, lineno: int, flag: str
) -> Transaction:
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


def flatten_dict(dd, separator=":", prefix=""):
    return (
        {
            prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
        }
        if isinstance(dd, dict)
        else {prefix: dd}
    )
