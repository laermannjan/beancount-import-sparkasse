#!/usr/bin/env python3

import re

from beancount_import_sparkasse.models import TXN


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
