.PHONY: docs

docs:
	@cd docs && make html

release:
	@VERSION=$$(python -c "import pyrallel;print(pyrallel.__version__)") && git tag $$VERSION