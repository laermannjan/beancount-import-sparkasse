# Beancount Importer - Sparkasse
[![tests_badge](https://github.com/laermannjan/beancount-import-sparkasse/actions/workflows/main.yaml/badge.svg)](https://github.com/laermannjan/beancount-import-sparkasse/actions/) [![image](https://img.shields.io/pypi/v/beancount-import-sparkasse.svg)](https://pypi.python.org/pypi/beancount-import-sparkasse) [![image](https://img.shields.io/pypi/pyversions/beancount-import-sparkasse.svg)](https://pypi.python.org/pypi/beancount-import-sparkasse) [![image](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![license_badge](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
## Installation
The importer is available on [PyPI](https://pypi.org/project/beancount-import-sparkasse)
``` sh
pip install --user beancount-import-sparkasse
```

## Configuration
Add the importer to your `beancount` import config

``` python
from beancount_import_sparkase import SparkasseCSVCAMTImporter

CONFIG = [
    SparkasseCSVCAMTImporter(
        iban="DE01 2345 6789 0123 4567 89",
        account="Assets:DE:Sparkasse:Giro"
    )
]

```
