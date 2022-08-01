#!/usr/bin/env python3

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Sequence
from copy import deepcopy
from functools import wraps

import yaml
from beancount.core import flags

from beancount_import_sparkasse.importers import BaseImporter
from beancount_import_sparkasse.models import TXN, InducedPosting

logger = logging.getLogger(__name__)


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
                    if field_name not in txn.__dict__:
                        raise ValueError(
                            f"You specified a condition identifier {field_name} "
                            "in your yaml, that is not in the list of allowed fields: "
                            f"{txn.__dict__}"
                        )
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
        ...


def patch_hooks(importer: BaseImporter, hooks: Sequence[CSVtoTXNHook]):
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
            meta_values = []
            for match in matches:
                if "meta" in match.groupdict():
                    meta_values.append(match.group("meta"))

        elif len(split_id) == 2:
            key = split_id[0]
            meta_values = [split_id[1]]
        else:
            raise ValueError(
                "The provided yaml seems funky. "
                "We expect either level0=meta_tag_name, level1=rule_set_with_meta, "
                "or level1=meta_value, level2=rule_set"
            )

        fields_dict = defaultdict(list)
        allowed_fields = [f for f, v in txn.__dict__.items() if isinstance(v, str)]
        for match in matches:
            for field, value in match.groupdict().items():
                if field == "meta":
                    continue
                fields_dict[field].append(value.strip())
        for field, value in fields_dict.items():
            if field not in allowed_fields:
                raise ValueError(
                    f"A regex in your yaml contained a named group '{field}',"
                    f"which is not in the list of possible names: {allowed_fields}"
                )
            setattr(txn, field, " ".join(value))
        if key and meta_values:
            txn.meta[key] = " ".join(meta_values).upper()


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
