"""Tests for mofc_financials.data_extraction.validate.

Unit tests for validation logic using mock extraction data.
No OCR or PDF dependencies required.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mofc_financials.data_extraction.extract_990_detail import LineItemRow
from mofc_financials.data_extraction.validate import (
    ValidationIssue,
    _pct_diff,
    _to_int,
    format_report,
    validate_year,
)

# =========================================================================
# _to_int — numeric parsing
# =========================================================================


class TestToInt:
    """Tests for string-to-int parsing."""

    def test_plain_number(self) -> None:
        assert _to_int("123456") == 123456

    def test_negative(self) -> None:
        assert _to_int("-2272381") == -2272381

    def test_blank(self) -> None:
        assert _to_int("") is None

    def test_whitespace(self) -> None:
        assert _to_int("  ") is None

    def test_non_numeric(self) -> None:
        assert _to_int("abc") is None


# =========================================================================
# _pct_diff — relative difference
# =========================================================================


class TestPctDiff:
    """Tests for percentage difference calculation."""

    def test_identical(self) -> None:
        assert _pct_diff(100, 100) == 0.0

    def test_small_diff(self) -> None:
        result = _pct_diff(102, 100)
        assert abs(result - 0.02) < 0.001

    def test_zero_expected_nonzero_actual(self) -> None:
        assert _pct_diff(100, 0) == float("inf")

    def test_both_zero(self) -> None:
        assert _pct_diff(0, 0) == 0.0


# =========================================================================
# validate_year — full validation
# =========================================================================

_GOOD_SUMMARY: dict[str, str] = {
    "form_year": "2023",
    "total_revenue": "133634324",
    "total_expenses": "134475329",
    "contributions_and_grants": "126739348",
    "program_service_revenue": "4362941",
    "investment_income": "2263725",
}

_GOOD_REVENUE: list[LineItemRow] = [
    {"line_number": "1a", "label": "Federated campaigns", "col_a": "87842"},
    {"line_number": "1e", "label": "Government grants", "col_a": "6221821"},
    {"line_number": "1h", "label": "Total contributions", "col_a": "126739348"},
    {"line_number": "2g", "label": "Total program service revenue", "col_a": "4362941"},
    {"line_number": "3", "label": "Investment income", "col_a": "2263725"},
    {"line_number": "12", "label": "Total revenue", "col_a": "133634324"},
]

_GOOD_EXPENSES: list[LineItemRow] = [
    {
        "line_number": "7",
        "label": "Other salaries and wages",
        "col_a": "10438376",
        "col_b": "7526337",
        "col_c": "1774146",
        "col_d": "1137893",
    },
    {
        "line_number": "25",
        "label": "Total functional expenses",
        "col_a": "134475329",
        "col_b": "124038575",
        "col_c": "6846503",
        "col_d": "3590251",
    },
]


class TestValidateYear:
    """Tests for the main validation function."""

    def test_good_data_no_errors(self) -> None:
        """Well-formed data should produce no errors (warnings OK for missing lines)."""
        issues = validate_year("2023", _GOOD_SUMMARY, _GOOD_REVENUE, _GOOD_EXPENSES)
        errors = [i for i in issues if i.severity == "ERROR"]
        assert len(errors) == 0

    def test_missing_total_revenue(self) -> None:
        """Missing line 12 should produce a completeness error."""
        revenue_no_12 = [r for r in _GOOD_REVENUE if r["line_number"] != "12"]
        issues = validate_year("2023", _GOOD_SUMMARY, revenue_no_12, _GOOD_EXPENSES)
        errors = [i for i in issues if i.severity == "ERROR"]
        msg_text = " ".join(e.message for e in errors)
        assert "line 12" in msg_text.lower()

    def test_cross_validation_mismatch(self) -> None:
        """Mismatched detail vs summary should produce cross_validation error."""
        bad_summary = {**_GOOD_SUMMARY, "total_revenue": "999999999"}
        issues = validate_year("2023", bad_summary, _GOOD_REVENUE, _GOOD_EXPENSES)
        cross_errors = [i for i in issues if i.category == "cross_validation"]
        assert len(cross_errors) > 0

    def test_expense_column_sum_mismatch(self) -> None:
        """col_b+c+d != col_a should produce internal_consistency error."""
        bad_expenses: list[LineItemRow] = [
            {
                "line_number": "25",
                "label": "Total functional expenses",
                "col_a": "134475329",
                "col_b": "100000000",
                "col_c": "1000000",
                "col_d": "1000000",
            },
        ]
        issues = validate_year("2023", _GOOD_SUMMARY, _GOOD_REVENUE, bad_expenses)
        consistency = [i for i in issues if i.category == "internal_consistency"]
        assert any("col_b+c+d" in i.message for i in consistency)

    def test_duplicate_line_numbers(self) -> None:
        """Duplicate line numbers should produce a warning."""
        duped_expenses: list[LineItemRow] = [
            *_GOOD_EXPENSES,
            {"line_number": "7", "label": "Other salaries (dup)", "col_a": "123"},
        ]
        issues = validate_year("2023", _GOOD_SUMMARY, _GOOD_REVENUE, duped_expenses)
        dup_warnings = [i for i in issues if i.category == "duplicate"]
        assert len(dup_warnings) > 0

    def test_suspicious_total_line(self) -> None:
        """Total line with tiny value should produce suspicious error."""
        bad_revenue: list[LineItemRow] = [
            *[r for r in _GOOD_REVENUE if r["line_number"] != "12"],
            {"line_number": "12", "label": "Total revenue", "col_a": "988"},
        ]
        issues = validate_year("2023", _GOOD_SUMMARY, bad_revenue, _GOOD_EXPENSES)
        suspicious = [i for i in issues if i.category == "suspicious"]
        assert any("988" in i.message for i in suspicious)

    def test_empty_col_a_on_total(self) -> None:
        """Total line with empty col_a should produce completeness error."""
        bad_revenue: list[LineItemRow] = [
            *[r for r in _GOOD_REVENUE if r["line_number"] != "12"],
            {"line_number": "12", "label": "Total revenue"},
        ]
        issues = validate_year("2023", _GOOD_SUMMARY, bad_revenue, _GOOD_EXPENSES)
        completeness = [i for i in issues if i.category == "completeness"]
        assert any("total column is empty" in i.message for i in completeness)


# =========================================================================
# format_report — report formatting
# =========================================================================


class TestFormatReport:
    """Tests for report formatting."""

    def test_contains_year_sections(self) -> None:
        all_issues = {
            "2022": [
                ValidationIssue("2022", "ERROR", "completeness", "Missing line 12"),
            ],
            "2023": [],
        }
        report = format_report(
            all_issues,
            revenue_counts={"2022": 8, "2023": 9},
            expense_counts={"2022": 21, "2023": 22},
            output_files=["a.csv", "b.csv"],
        )
        assert "--- 2022 ---" in report
        assert "--- 2023 ---" in report
        assert "SUMMARY" in report

    def test_errors_and_warnings_labeled(self) -> None:
        all_issues = {
            "2023": [
                ValidationIssue("2023", "ERROR", "cross_validation", "Mismatch"),
                ValidationIssue("2023", "WARNING", "duplicate", "Dup line"),
            ],
        }
        report = format_report(
            all_issues,
            revenue_counts={"2023": 9},
            expense_counts={"2023": 22},
            output_files=[],
        )
        assert "ERRORS" in report
        assert "WARNINGS" in report
        assert "✗" in report
        assert "⚠" in report

    def test_clean_year_shows_checkmark(self) -> None:
        all_issues = {"2023": []}
        report = format_report(
            all_issues,
            revenue_counts={"2023": 9},
            expense_counts={"2023": 22},
            output_files=[],
        )
        assert "✓" in report

    def test_summary_table_totals(self) -> None:
        all_issues = {
            "2022": [ValidationIssue("2022", "ERROR", "x", "e1")],
            "2023": [ValidationIssue("2023", "WARNING", "x", "w1")],
        }
        report = format_report(
            all_issues,
            revenue_counts={"2022": 8, "2023": 9},
            expense_counts={"2022": 21, "2023": 22},
            output_files=[],
        )
        assert "1 errors, 1 warnings" in report
