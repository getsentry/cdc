check:
	python -m mypy -p cdc
.PHONY: check

format:
	python -m black cdc/
.PHONY: format
