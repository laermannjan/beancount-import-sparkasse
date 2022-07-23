#!/usr/bin/env python3

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from copy import deepcopy
from functools import wraps

import yaml
from beancount.core import flags

from beancount_import_sparkasse.models import TXN, Importer, InducedPosting
from beancount_import_sparkasse.utils import flatten_dict

logging.basicConfig(format="%(levelname)s :: %(name)s | %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CSVtoTXNHook(ABC):
    def __init__(self, rule_sets: dict[str, Sequence[dict[str, str]]]) -> None:
        self.rule_sets = rule_sets

    @classmethod
    def from_yaml(cls, fname) -> CSVtoTXNHook:
        with open(fname) as f:
            rule_sets = flatten_dict(yaml.safe_load(f))

        for account, rule_set in rule_sets.items():
            if not isinstance(account, str):
                raise TypeError(f"{account=} was not of type `str`")
            if not isinstance(rule_set, list):
                raise TypeError(f"{rule_set=} for {account=} was not of type `list`")
            for rule in rule_set:
                if not isinstance(rule, dict):
                    raise TypeError(f"{rule=} was not of type `dict`")
                for field_name, regex in rule.items():
                    if not isinstance(field_name, str):
                        raise TypeError(f"{field_name=} was not of type `str`")
                    if not isinstance(regex, str):
                        raise TypeError(f"{regex=} was not of type `str`")

        return cls(rule_sets=flatten_dict(rule_sets))

    def __call__(self, original_txn: TXN) -> TXN:
        txn = deepcopy(original_txn)
        for identifier, rule_set in self.rule_sets.items():
            for rule in rule_set:
                matches = []
                for field_name, pattern in rule.items():
                    match = re.search(
                        pattern=pattern,
                        string=getattr(txn, field_name),
                        flags=re.IGNORECASE,
                    )
                    if match:
                        matches.append(match)
                    else:
                        break
                else:
                    self.augment(identifier=identifier, matches=matches, txn=txn)
        return txn

    @abstractmethod
    def augment(self, identifier: str, matches: list[re.Match], txn: TXN) -> None:
        raise NotImplementedError


def patch_hooks(importer: Importer, hooks: Sequence[CSVtoTXNHook]):
    original_csv_to_txn = importer.csv_to_txn

    @wraps(original_csv_to_txn)
    def patched_csv_to_txn(csv_row: dict[str, str]):
        txn = original_csv_to_txn(csv_row=csv_row)

        for i, hook in enumerate(hooks):
            logger.debug(f"Processing hook {hook.__class__.__name__} {i}/{len(hooks)}")
            txn = hook(txn)
        return txn

    importer.csv_to_txn = patched_csv_to_txn
    return importer


class AccountProcessor(CSVtoTXNHook):
    def augment(self, identifier: str, matches: list[re.Match], txn: TXN) -> None:
        posting = InducedPosting(flag=flags.FLAG_WARNING, account=identifier)
        txn.induced_postings.append(posting)


class MetaProcessor(CSVtoTXNHook):
    def augment(self, identifier: str, matches: list[re.Match], txn: TXN) -> None:

        split_id = identifier.split(":")
        if len(split_id) == 1:
            key = identifier
            meta_value = None
            for match in matches:
                if "meta" in match.groupdict():
                    meta_value = match.group("meta")

            if not meta_value:
                raise ValueError(
                    f"rule set for {identifier=} found on depth 1,"
                    " but no named group 'meta' found"
                )

        elif len(split_id) == 2:
            key = split_id[0]
            meta_value = split_id[1]
        else:
            raise RuntimeError(
                "The provided yaml seems funky. "
                "We expect either level0=meta_tag_name, level1=rule_set_with_meta, "
                "or level1=meta_value, level2=rule_set"
            )

        if not key or not meta_value:
            raise ValueError(
                f"Something broke, sorry."
                f"Either {key=} or {meta_value=} were not set correctly."
            )
        txn.meta[key] = meta_value.lower()


def process_payee_iban(txn: TXN):
    if txn.payee_iban:
        txn.meta["payee_iban"] = txn.payee_iban


def process_payee_bic(txn: TXN):
    if txn.payee_bic:
        txn.meta["payee_bic"] = txn.payee_bic


def process_posting_type(txn: TXN):
    if txn.posting_type:
        txn.meta["posting_type"] = txn.posting_type


def process_amazon(txn: TXN):
    match_regex = r"(\w\d{2}-\d{7}-\d{7}) (\w.+) \w{16}"
    match = re.search(pattern=match_regex, string=txn.reference)
    if match and re.search(r"[amazon|audible]", txn.payee_name, flags=re.IGNORECASE):
        txn.meta["order_number"] = match.group(1).strip()
        if "AMZN Mktp" in match.group(2):
            txn.meta["amazon_platform"] = "marketplace"
        elif "AMZNPrime" in match.group(2):
            txn.meta["amazon_platform"] = "prime"
        txn.reference = ""


def process_paypal(txn: TXN):
    match_regex = r"[PP\.\d+\.PP ]?\. (.*), Ihr Einkauf bei [, ]?(.+)$"
    match = re.search(pattern=match_regex, string=txn.reference)
    if "paypal" in txn.payee_name.lower() and match:
        txn.meta["via"] = "paypal"
        g1 = match.group(1).strip()
        g2 = match.group(2).strip()

        if g1:
            txn.payee_name = g1
        if g2:
            txn.reference = "" if g1 == g2 else g2


def process_debit_payments(txn: TXN):
    debit_regex = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}) Debitk"
    debit_match = re.search(debit_regex, txn.reference)

    sumup_regex = r"SumUp \.(\w.*//.+/.+)/\d+"
    sumup_match = re.search(sumup_regex, txn.payee_name)
    # sparkasse EC
    if txn.posting_type == "KARTENZAHLUNG" and debit_match:
        txn.meta["payment_date"] = debit_match.group(1).strip()
        if sumup_match:
            txn.meta["via"] = "sumup"
            txn.payee_name = sumup_match.group(1).strip()
        txn.reference = ""


def process_withdrawal(txn: TXN):
    match_regex = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}) Debitk"
    match = re.search(match_regex, txn.reference)

    # sparkasse EC
    if txn.posting_type == "BARGELDAUSZAHLUNG" and match:
        txn.meta["withdrawal_date"] = match.group(1).strip()
        txn.reference = ""


def process_db(txn: TXN):
    payee_regex = r"(DB Vertrieb GmbH)"
    payee_match = re.search(payee_regex, txn.payee_name)

    ticket_regex = r"Fahrschein (\w+)"
    ticket_match = re.search(ticket_regex, txn.reference)
    if payee_match:
        txn.payee_name = payee_match.group(1).strip()
        if ticket_match:
            txn.reference = ""
            txn.meta["ticket_number"] = ticket_match.group(1).strip()


def process_telefonica(txn: TXN):
    match_regex = r"Kd-Nr.+ Rg-Nr\.: (\d+/\d+), Ihre (\w+)"
    match = re.search(match_regex, txn.reference)
    if "Telefonica Germany" in txn.payee_name and match:
        txn.reference = match.group(2).strip()
        txn.meta["invoice_number"] = match.group(1).strip()


def process_telecolumbus(txn: TXN):
    reference_regex = r"KD-NR.+ RG-NR.+ (\d+), Faelligkeit"
    reference_match = re.search(reference_regex, txn.reference)

    payee_regex = r"(Tele Columbus .* Co \. KG)"
    payee_match = re.search(payee_regex, txn.payee_name)

    if payee_match and reference_match:
        txn.reference = ""
        txn.meta["invoice_number"] = reference_match.group(1).strip()
        txn.payee_name = payee_match.group(1).strip()
