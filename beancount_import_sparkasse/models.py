#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Literal


@dataclass
class InducedPosting:
    flag: Literal["*"] | Literal["!"]
    account: str


@dataclass
class TXN:
    owner_iban: str
    booking_date: datetime
    posting_type: str
    reference: str
    payee_name: str
    payee_iban: str
    payee_bic: str
    amount: Decimal
    currency: str
    induced_postings: list[InducedPosting] = field(default_factory=list)
    meta: dict[str, str] = field(default_factory=dict)
