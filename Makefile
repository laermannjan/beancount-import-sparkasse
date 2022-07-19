.PHONY: \
	check-format \
	lint \
	check-type \
	check-code-quality \
    tests

check-format:
	which python
	python --version
	poetry run which python
	poetry run python --version
	poetry run black --check beancount_import_sparkasse
	poetry run isort --check-only beancount_import_sparkasse

lint:
	poetry run flake8 beancount_import_sparkasse

check-type:

check-code-quality: check-format lint check-type

tests:
	which python
	python --version
	poetry run which python
	poetry run python --version
	poetry run pytest tests
