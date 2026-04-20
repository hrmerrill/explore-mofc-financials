.PHONY: format install test typecheck doccheck lint check-all run-all serve

format:
	isort src/ tests/
	black -l 100 src/ tests/

install:
	pip install -e ".[dev]"

test:
	pytest

typecheck:
	mypy src/

doccheck:
	interrogate src/

lint: format typecheck doccheck

check-all:
	isort --check-only --diff src/ tests/
	black --check --diff -l 100 src/ tests/
	interrogate src/
	mypy src/
	pytest --tb=short -q

run-all: format install test typecheck doccheck

serve:
	@echo "Dashboard: http://localhost:8000"
	cd docs && python -m http.server 8000 & sleep 1 && open http://localhost:8000
