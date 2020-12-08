.PHONY: docs

docs:
	@cd docs && make html

release:
	@VERSION=$$(python -c "from pyrallel.__version__ import __version__;print(__version__)") && git tag $$VERSION

# locate all the files in this directory or below:
FILES=`find . -name '*.py'`

# The command for running mypy:
lint:
	python3 -m mypy $(FILES)

# Run the unit tests.
test:
	python3 -m pytest -s pyrallel/tests/test_map_reduce.py
	python3 -m pytest -s pyrallel/tests/test_parallel_processor.py
	python3 -m pytest -s pyrallel/tests/test_queue.py
