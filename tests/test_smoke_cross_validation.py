"""Smoke tests — run the extraction pipeline against real MOFC 990 PDFs.

These tests require Tesseract and the actual PDF files, so they are
marked with ``@pytest.mark.smoke`` and skipped by default.

Run explicitly::

    pytest -m smoke --tb=short -v
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from mofc_financials.data_extraction.extract_990 import extract_financials
from mofc_financials.data_extraction.extract_990_detail import (
    extract_expense_detail,
    extract_revenue_detail,
)
from mofc_financials.data_extraction.validate import (
    ValidationIssue,
    run_pipeline,
    validate_year,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
_PDFS = sorted(_DATA_DIR.glob("MOFC-990-*.pdf"))


def _year_from_path(pdf: Path) -> str:
    """Extract the 4-digit year from a PDF filename."""
    m = re.search(r"(\d{4})", pdf.stem)
    return m.group(1) if m else ""


def _to_int(val: str) -> int | None:
    """Convert extracted string to int, returning None for blanks."""
    cleaned = val.strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _detail_lookup(rows: list[dict[str, str]], line_number: str) -> dict[str, str] | None:
    """Find a line item by line_number in the detail results."""
    for r in rows:
        if r.get("line_number") == line_number:
            return r
    return None


pytestmark = pytest.mark.smoke

_skip_no_pdfs = pytest.mark.skipif(
    not _PDFS,
    reason="No MOFC 990 PDFs found in data/raw/ — skipping smoke tests",
)


# ---------------------------------------------------------------------------
# Pipeline integration tests
# ---------------------------------------------------------------------------


@_skip_no_pdfs
class TestPipelineIntegration:
    """Test that the full pipeline runs and produces expected outputs."""

    def test_pipeline_produces_all_files(self, tmp_path: Path) -> None:
        """Pipeline creates summary CSV, detail CSVs, and validation report."""
        run_pipeline(_DATA_DIR, tmp_path)

        assert (tmp_path / "mofc_990_financials.csv").exists()
        assert (tmp_path / "mofc_990_revenue_detail.csv").exists()
        assert (tmp_path / "mofc_990_expense_detail.csv").exists()
        assert (tmp_path / "mofc_990_validation_report.txt").exists()

    def test_report_contains_all_years(self, tmp_path: Path) -> None:
        """Validation report has sections for every year."""
        run_pipeline(_DATA_DIR, tmp_path)
        report = (tmp_path / "mofc_990_validation_report.txt").read_text()

        for pdf in _PDFS:
            year = _year_from_path(pdf)
            assert f"--- {year} ---" in report

    def test_report_contains_summary_table(self, tmp_path: Path) -> None:
        """Validation report includes the SUMMARY section."""
        run_pipeline(_DATA_DIR, tmp_path)
        report = (tmp_path / "mofc_990_validation_report.txt").read_text()
        assert "SUMMARY" in report

    def test_summary_csv_has_all_years(self, tmp_path: Path) -> None:
        """Summary CSV contains one row per PDF."""
        run_pipeline(_DATA_DIR, tmp_path)
        csv_text = (tmp_path / "mofc_990_financials.csv").read_text()
        for pdf in _PDFS:
            year = _year_from_path(pdf)
            assert year in csv_text


# ---------------------------------------------------------------------------
# Per-year extraction quality (minimum viability)
# ---------------------------------------------------------------------------


@_skip_no_pdfs
@pytest.mark.parametrize("pdf", _PDFS, ids=[p.stem for p in _PDFS])
class TestExtractionQuality:
    """Verify minimum extraction quality for each year."""

    def test_summary_has_key_fields(self, pdf: Path) -> None:
        """Summary extractor populates total_revenue and total_expenses."""
        summary = extract_financials(str(pdf))
        year = _year_from_path(pdf)
        assert summary.get("total_revenue"), f"[{year}] summary missing total_revenue"
        assert summary.get("total_expenses"), f"[{year}] summary missing total_expenses"

    def test_expense_detail_min_lines(self, pdf: Path) -> None:
        """Detail extractor finds at least 10 expense line items."""
        year = _year_from_path(pdf)
        expenses = extract_expense_detail(str(pdf))
        assert len(expenses) >= 10, f"[{year}] only {len(expenses)} expense lines"

    def test_revenue_detail_min_lines(self, pdf: Path) -> None:
        """Detail extractor finds at least 5 revenue line items."""
        year = _year_from_path(pdf)
        revenue = extract_revenue_detail(str(pdf))
        assert len(revenue) >= 5, f"[{year}] only {len(revenue)} revenue lines"

    def test_validation_runs_without_crash(self, pdf: Path) -> None:
        """Validation completes without errors for each year."""
        year = _year_from_path(pdf)
        summary = extract_financials(str(pdf))
        revenue = extract_revenue_detail(str(pdf))
        expenses = extract_expense_detail(str(pdf))

        # Should not raise — issues are returned, not raised
        issues = validate_year(year, summary, revenue, expenses)
        assert isinstance(issues, list)
