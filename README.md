# Explore Mid-Ohio Food Collective Financials

MOFC posts their financials [here](https://mofc.org/financials/). I'm interested in finding opportunities for optimization, and the first step is understanding where the money is currently coming from and going. This repo contains code for extracting and analyzing this data.

## Raw Data

MOFC posts image-based PDF scans of their IRS 990 forms. These are stored in `data/raw/` and processed via OCR into structured CSV data in `data/processed/`.

## Getting Started

### Prerequisites

- **Python 3.12+**
- **Tesseract OCR** — required for PDF text extraction
  ```bash
  # macOS
  brew install tesseract

  # Ubuntu/Debian
  sudo apt-get install tesseract-ocr
  ```

### Installation

```bash
# Clone the repo
git clone https://github.com/hrmerrill/explore-mofc-financials.git
cd explore-mofc-financials

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install the package (with dev tools)
pip install -e ".[dev]"
```

### Running the Extraction

```bash
# Via the CLI entry point
mofc-extract

# Or directly
python -m mofc_financials.data_extraction.extract_990
```

This scans all `MOFC-990-*.pdf` files in `data/raw/`, extracts Part I Summary financial fields via OCR, and writes the results to `data/processed/mofc_990_financials.csv`.

## For Developers

### Project Structure

```
explore-mofc-financials/
├── pyproject.toml              # Package metadata, deps, tool config
├── src/
│   └── mofc_financials/        # Main package (src-layout)
│       ├── __init__.py
│       └── data_extraction
│           ├── __init__.py
│           └── extract_990.py  # OCR extraction from 990 PDFs
├── tests/
│   └── test_extract_990.py     # Unit tests
├── data/
│   ├── raw/                    # Source PDFs (not committed)
│   └── processed/              # Extracted CSVs
└── .github/
    └── copilot-instructions.md # AI assistant conventions
```

### Running Tests

```bash
pytest
```

### Code Formatting

This project uses [Black](https://black.readthedocs.io/), [isort](https://pycqa.github.io/isort/), and [mypy](https://mypy.readthedocs.io/) for consistent formatting and type checking:

```bash
# Format all Python files
isort src/ tests/
black -l 100 src/ tests/

# Check without modifying
black --check src/ tests/
isort --check-only src/ tests/

# Type checking
mypy src/
```

### Adding New Analysis

Future modules for statistical analysis or visualization should be added under `src/mofc_financials/` and can use the optional `analysis` dependencies:

```bash
pip install -e ".[analysis]"  # adds pandas, matplotlib, seaborn
```