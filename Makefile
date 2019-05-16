check:
	python -m pyflakes cdc
	python -m mypy -p cdc
.PHONY: check

clean:
	git clean -fdx
.PHONY: clean

format:
	python -m black cdc/
.PHONY: format
