# Copilot Instructions — explore-mofc-financials

## Project Overview

This repo extracts and analyzes financial data from IRS Form 990 PDFs
published by the Mid-Ohio Food Collective (MOFC). PDFs are image-based
scans processed via OCR (Tesseract + PyMuPDF).

## Package Structure

- **src-layout**: all package code lives under `src/mofc_financials/`
- **tests**: mirror the package structure under `tests/`
- **data**: raw PDFs in `data/raw/`, processed output in `data/processed/`

## Code Style & Conventions

### Virtual Environment

- Activate the virtual environment with `. .venv/bin/activate` before beginning development

### Formatting

- **isort** (profile: black) for import sorting (applied first)
- **black** (line-length 100) for code formatting
- Always run `black -l 100 src/ tests/ && isort src/ tests/` before committing

### Docstrings

- All public functions **must** have **NumPy-style** docstrings
- Include `Parameters`, `Returns`, `Attributes`, `Raises`, and `Examples` sections where applicable
- Private/internal helpers may use single-line docstrings

### Comments

- Comment non-obvious logic, especially OCR heuristics and regex patterns
- Do not comment self-explanatory code

### Type Hints

- **Enforced** via [mypy](https://mypy.readthedocs.io/) in strict mode
- All public functions **must** have complete type annotations
- Use standard Python types (`str`, `int`, `dict`, `list`) — no `typing` module needed for builtins (Python 3.12+)
- Run with: `mypy src/` (via `make check-all`)

## Testing

- Framework: **pytest**
- All public functions must have corresponding tests
- Mock external dependencies (Tesseract, PyMuPDF) — tests must run
  without OCR software or PDF files
- Test file naming: `test_<module>.py`
- Run with: `pytest` (via `make check-all`)

## Checking

- Always run `make check-all` before committing (encompasses formatting, docstrings and unit tests)

## Dependencies

- Runtime: PyMuPDF, pytesseract, Pillow
- Dev: pytest, pytest-cov, black, isort, mypy
- Analysis (optional): pandas, matplotlib, seaborn

## Adding New Modules

When adding statistical analyses or visualizations:

1. Create a new module under `src/mofc_financials/`
2. Export public API from `__init__.py`
3. Add tests in `tests/test_<module>.py`
4. Add any new dependencies to the appropriate group in `pyproject.toml`
5. Update this file if new conventions are introduced
