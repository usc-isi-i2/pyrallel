.PHONY: docs

docs:
	@cd docs && make html

release:
	@VERSION=$$(python -c "from pyrallel.__version__ import __version__;print(__version__)") && git tag $$VERSION
