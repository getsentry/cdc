check:
	python -m pyflakes cdc
	python -m mypy -p cdc --disallow-untyped-defs --disallow-incomplete-defs --check-untyped-defs
.PHONY: check

format:
	python -m black cdc/
.PHONY: format
