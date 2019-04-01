check:
	python -m pyflakes cdc
	python -m mypy -p cdc \
		--check-untyped-defs \
		--disallow-incomplete-defs \
		--disallow-untyped-defs
.PHONY: check

format:
	python -m black cdc/
.PHONY: format
