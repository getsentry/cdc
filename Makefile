check:
	test $(shell sort requirements.txt | md5) = $(shell md5 -q requirements.txt)
	test $(shell sort requirements-dev.txt | md5) = $(shell md5 -q requirements-dev.txt)
	python -m black --check cdc/
	python -m pyflakes cdc
	python -m mypy -p cdc
.PHONY: check

clean:
	git clean -fdx
.PHONY: clean

format:
	sort -o requirements.txt requirements.txt
	sort -o requirements-dev.txt requirements-dev.txt
	python -m black cdc/ tests/
.PHONY: format

test:
	python -m pytest
.PHONY: test
