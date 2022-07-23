#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


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
    meta: dict[str, str] = field(default_factory=dict)
