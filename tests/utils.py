#!/usr/bin/env python3

import random
import string

COMPANY_NAMES = {
    "amazon": "AMAZON EU S.A R.L., NIEDERLASSUNG DEUTSCHLAND",
    "audible": "AUDIBLE GMBH",
}


def random_string(size: int, letters: bool = False, digits: bool = False):
    population = ""
    if letters:
        population += string.ascii_letters
    if digits:
        population += string.digits
    return "".join(random.choices(population=population, k=size))


def fake_iban():
    return "DE" + random_string(size=20, digits=True)


def fake_amazon_order_number():
    a = random_string(size=3, digits=True)
    b = random_string(size=7, digits=True)
    c = random_string(size=7, digits=True)
    return "-".join([a, b, c])


def fake_audible_order_number():
    a = random.choice(string.ascii_uppercase) + random_string(size=2, digits=True)
    b = random_string(size=7, digits=True)
    c = random_string(size=7, digits=True)
    return "-".join([a, b, c])
