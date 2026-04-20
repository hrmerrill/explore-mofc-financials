# Explore Mid-Ohio Food Collective Financials

MOFC posts their financials [here](https://mofc.org/financials/). I'm interested in finding opportunities for optimization, and the first step is understanding where the money is currently coming from and going. This repo contains code for extracting and analyzing this data.

## Raw Data

MOFC posts image-based PDF scans of their IRS 990 forms. These are stored in `data/raw/` and processed via OCR into structured CSV data in `data/processed/`.

MOFC also publishes annual audit reports (`MOFC-Audit-*.pdf` in `data/raw/`). These are text-based PDFs (no OCR needed) containing donated and purchased food inventory data used for efficiency metrics.

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

### Running the Extraction Pipeline

The pipeline runs in two stages.

**Stage 1 — Extract and validate (run once per new PDF set)**

```bash
mofc-pipeline
```

This scans all `MOFC-990-*.pdf` files in `data/raw/`, extracts Part I Summary
fields and Parts VIII/IX line-item detail via OCR, writes three CSVs to
`data/processed/`, and produces a validation report flagging values that may
need correction. If `MOFC-Audit-*.pdf` files are present, it also extracts
food volume data and computes efficiency metrics.

**Stage 2 — Manual correction and re-validation (iterate until clean)**

```bash
# Copy the extraction output to editable files (only needed once)
cp data/processed/mofc_990_financials.csv \
   data/processed/mofc_990_financials_manual_edits.csv
cp data/processed/mofc_990_revenue_detail.csv \
   data/processed/mofc_990_revenue_detail_manual_edits.csv
cp data/processed/mofc_990_expense_detail.csv \
   data/processed/mofc_990_expense_detail_manual_edits.csv

# Open data/processed/mofc_990_validation_report.txt, fix values in the
# *_manual_edits.csv files (compare against the source PDFs in data/raw/).

# Re-validate the edited files — no OCR required
mofc-validate
```

`mofc-validate` reads the `*_manual_edits.csv` files when present (falling back
to the original extraction CSVs) and overwrites the validation report. Repeat
until the report shows no errors.

**Audit Data Extraction (standalone)**

```bash
# Extract food volume data from audit PDFs
mofc-extract-audit

# Compute efficiency metrics (requires both 990 + audit CSVs)
mofc-compute-efficiency
```

## Dashboard

An interactive financial dashboard lives at `docs/index.html`. It visualizes
the extracted data across five tabs (Overall, Revenue, Expenses, About,
Efficiency) using [Chart.js](https://www.chartjs.org/). The Efficiency tab
shows cost-per-meal, food volume by source, waste rate, and labor productivity
metrics derived from annual audit reports. The dashboard loads CSV files from
`docs/data/` via relative `fetch()` calls, so
it must be served over HTTP — opening the HTML file directly will fail due to
browser CORS restrictions on `file://` URLs.

### Previewing Locally

Always preview the dashboard locally before pushing changes:

```bash
make serve          # starts a local server at http://localhost:8000
```

Open <http://localhost:8000> and verify:

1. All four tabs render without errors (check the browser console).
2. KPI cards show reasonable values with correct YoY percentages.
3. Charts display data for every year (2019–2023).
4. Grouped/stacked toggles and noncash/food toggles work.

Stop the server with `Ctrl+C` when done.

### Updating Dashboard Data

After Stage 2 validation passes with no errors, copy the final CSVs into
`docs/data/` so the dashboard picks them up:

```bash
cp data/processed/mofc_990_financials_manual_edits.csv    docs/data/
cp data/processed/mofc_990_revenue_detail_manual_edits.csv docs/data/
cp data/processed/mofc_990_expense_detail_manual_edits.csv docs/data/
```

Then preview locally (`make serve`) before committing or deploying.

## For Developers

### Project Structure

```
explore-mofc-financials/
├── pyproject.toml              # Package metadata, deps, tool config
├── Makefile                    # Dev shortcuts (format, test, serve, etc.)
├── src/
│   └── mofc_financials/        # Main package (src-layout)
│       ├── __init__.py
│       └── data_extraction/
│           ├── __init__.py
│           ├── extract_990.py        # Part I Summary OCR extraction
│           ├── extract_990_detail.py # Parts VIII/IX line-item extraction
│           └── validate.py           # Pipeline orchestration + validation
├── tests/
│   └── test_*.py               # Unit tests (no OCR/PDF deps)
├── data/
│   ├── raw/                    # Source PDFs (not committed)
│   └── processed/              # Extracted and manually edited CSVs
├── docs/
│   ├── index.html              # Interactive financial dashboard
│   └── data/                   # CSV data served to the dashboard
└── .github/
    ├── copilot-instructions.md # AI assistant conventions
    └── workflows/ci.yml        # CI: lint, typecheck, test
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `mofc-pipeline` | Full extract + validate pipeline (Stage 1) |
| `mofc-validate` | Re-validate edited CSVs without OCR (Stage 2) |
| `mofc-extract` | Extract Part I Summary only |
| `mofc-extract-detail` | Extract Parts VIII/IX detail only |

### Makefile Targets

```bash
make format       # isort + black
make test         # pytest
make typecheck    # mypy src/
make doccheck     # interrogate src/ (100% docstring coverage)
make lint         # format + typecheck + doccheck
make check-all    # all checks without modifying files
make serve        # local dashboard preview at http://localhost:8000
```

### Running Tests

```bash
pytest                          # unit tests (fast, no OCR needed)
pytest -m smoke --tb=short -v   # smoke tests (slow, requires Tesseract + PDFs)
```

### Code Quality

This project uses [Black](https://black.readthedocs.io/), [isort](https://pycqa.github.io/isort/), [mypy](https://mypy.readthedocs.io/), and [interrogate](https://interrogate.readthedocs.io/) for formatting, type checking, and docstring coverage:

```bash
# Format all Python files
isort src/ tests/
black -l 100 src/ tests/

# Check without modifying
black --check src/ tests/
isort --check-only src/ tests/

# Type checking (strict mode)
mypy src/

# Docstring coverage (100% required)
interrogate src/
```

### CI

GitHub Actions runs lint, typecheck, and test jobs on every push and PR to
`main`. All three must pass before merging. Tests run on Python 3.12, 3.13,
and 3.14.

### Adding New Analysis

Future modules for statistical analysis or visualization should be added under `src/mofc_financials/` and can use the optional `analysis` dependencies:

```bash
pip install -e ".[analysis]"  # adds pandas, matplotlib, seaborn
```

### Dashboard Development Notes

The dashboard (`docs/index.html`) is a single-file HTML/JS app with no build
step. A few things to know when editing it:

- **`file://` URLs won't work.** The dashboard loads CSVs via `fetch()`, which
  browsers block on `file://` due to CORS. Always use `make serve`.

- **Food pass-through inflates revenue and expenses equally.** Noncash food
  donations (revenue line 1g, ~$94–107M/yr) and the corresponding food expense
  (expense line 24a) represent roughly the same food valued on intake vs.
  distribution. The "Include Food" toggle on the Overall tab controls whether
  these amounts are included in the Revenue vs. Expenses and Surplus / Deficit
  charts. It defaults to **on** (food included) and updates both charts
  simultaneously because they share the same underlying totals.

- **Noncash ≠ food expense exactly.** Line 1g (noncash contributions received)
  and line 24a (food distributed) can differ due to timing, spoilage, or
  non-food noncash items. This means the Surplus / Deficit chart may shift
  slightly when toggling food — this is expected, not a bug.

- **Functional chart splits food from program services.** The Functional
  Expense Allocation chart separates "Food (Program)" from "Program Services"
  by subtracting line 24a's `program_service` value from line 25's total
  program services. Food is 100% program service in all years (2019–2023). If
  future 990s allocate food across other functional columns, the subtraction
  logic in the `FUNCTIONAL` data block will need updating.

- **Toggle patterns differ by chart type.** The Revenue Detail and Expense
  Detail charts use an add/remove-dataset pattern (the `wireToggle` helper)
  that appends a new bar group. The Overall food toggle uses a data-swap
  pattern that replaces existing dataset arrays in-place. Both call
  `chart.update()` after mutation — skipping this call is a common Chart.js
  mistake that results in stale visuals.