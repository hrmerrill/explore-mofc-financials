"""Tests for mofc_financials.data_extraction.validate.

Unit tests for validation logic using mock extraction data.
No OCR or PDF dependencies required.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from mofc_financials.data_extraction.extract_990 import FINANCIAL_FIELDS
from mofc_financials.data_extraction.extract_990_detail import LineItemRow
from mofc_financials.data_extraction.validate import (
    EXPENSE_CSV_FIELDS,
    REVENUE_CSV_FIELDS,
    ValidationIssue,
    _pct_diff,
    _read_detail_csv,
    _read_summary_csv,
    _to_int,
    check_label_consistency,
    check_line_presence_consistency,
    format_report,
    run_validation_only,
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
        """Well-formed data should produce no errors."""
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

    def test_expense_subline_column_sum_mismatch(self) -> None:
        """Sublines col_b/c/d not summing to line 25 col should warn."""
        bad_expenses: list[LineItemRow] = [
            {
                "line_number": "7",
                "label": "Other salaries and wages",
                "col_a": "10438376",
                "col_b": "100",  # way too low vs line 25 col_b
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
        issues = validate_year("2023", _GOOD_SUMMARY, _GOOD_REVENUE, bad_expenses)
        consistency = [i for i in issues if i.category == "internal_consistency"]
        assert any("program_service" in i.message for i in consistency)

    def test_expense_row_column_mismatch(self) -> None:
        """Individual expense row where col_b+c+d != col_a should warn."""
        bad_expenses: list[LineItemRow] = [
            {
                "line_number": "7",
                "label": "Other salaries and wages",
                "col_a": "10438376",
                "col_b": "1000000",  # intentionally wrong
                "col_c": "1774146",
                "col_d": "1137893",
            },
            *[r for r in _GOOD_EXPENSES if r["line_number"] == "25"],
        ]
        issues = validate_year("2023", _GOOD_SUMMARY, _GOOD_REVENUE, bad_expenses)
        consistency = [i for i in issues if i.category == "internal_consistency"]
        assert any("Expense line 7" in i.message for i in consistency)

    def test_revenue_row_column_mismatch(self) -> None:
        """Revenue rows where col_b+c+d != col_a should NOT warn (check removed)."""
        bad_revenue: list[LineItemRow] = [
            *[r for r in _GOOD_REVENUE if r["line_number"] != "3"],
            {
                "line_number": "3",
                "label": "Investment income",
                "col_a": "2263725",
                "col_b": "1000000",  # intentionally wrong
                "col_c": "0",
                "col_d": "0",
            },
        ]
        issues = validate_year("2023", _GOOD_SUMMARY, bad_revenue, _GOOD_EXPENSES)
        consistency = [i for i in issues if i.category == "internal_consistency"]
        assert not any("Revenue line 3" in i.message for i in consistency)

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
        assert "across 2 years" in report

    def test_cross_year_section_rendered(self) -> None:
        """Cross-year issues appear in their own section, not the year summary table."""
        all_issues = {
            "2023": [],
            "cross-year": [
                ValidationIssue("cross-year", "WARNING", "label_consistency", "Line 7 mismatch")
            ],
        }
        report = format_report(
            all_issues,
            revenue_counts={"2023": 9},
            expense_counts={"2023": 22},
            output_files=[],
        )
        assert "--- cross-year ---" in report
        assert "Line 7 mismatch" in report
        # cross-year should not appear as a row in the summary table
        assert "across 1 years" in report
        assert "1 cross-year" in report


# =========================================================================
# check_label_consistency — cross-year label checks
# =========================================================================


class TestCheckLabelConsistency:
    """Tests for cross-year label consistency checking."""

    def test_consistent_labels_no_warnings(self) -> None:
        revenue = {
            "2022": [{"line_number": "12", "label": "Total revenue", "col_a": "100"}],
            "2023": [{"line_number": "12", "label": "Total revenue", "col_a": "200"}],
        }
        issues = check_label_consistency(revenue, {})
        assert len(issues) == 0

    def test_inconsistent_revenue_label_warns(self) -> None:
        revenue = {
            "2022": [{"line_number": "7", "label": "Other salaries", "col_a": "1"}],
            "2023": [{"line_number": "7", "label": "Salaries and wages", "col_a": "2"}],
        }
        issues = check_label_consistency(revenue, {})
        assert len(issues) == 1
        assert issues[0].year == "cross-year"
        assert issues[0].category == "label_consistency"
        assert "Revenue line 7" in issues[0].message
        assert "Other salaries" in issues[0].message
        assert "Salaries and wages" in issues[0].message

    def test_inconsistent_expense_label_warns(self) -> None:
        expenses = {
            "2022": [{"line_number": "25", "label": "Total expenses", "col_a": "1"}],
            "2023": [{"line_number": "25", "label": "Total functional expenses", "col_a": "2"}],
        }
        issues = check_label_consistency({}, expenses)
        assert len(issues) == 1
        assert "Expense line 25" in issues[0].message

    def test_empty_inputs_no_issues(self) -> None:
        assert check_label_consistency({}, {}) == []

    def test_single_year_no_warnings(self) -> None:
        revenue = {"2023": [{"line_number": "12", "label": "Total revenue", "col_a": "100"}]}
        assert check_label_consistency(revenue, {}) == []

    def test_rows_with_blank_labels_skipped(self) -> None:
        revenue = {
            "2022": [{"line_number": "12", "label": "", "col_a": "100"}],
            "2023": [{"line_number": "12", "label": "Total revenue", "col_a": "200"}],
        }
        # Blank label in 2022 should be skipped; only one real label, no warning
        issues = check_label_consistency(revenue, {})
        assert len(issues) == 0


# =========================================================================
# check_line_presence_consistency — cross-year presence checks
# =========================================================================


class TestCheckLinePresenceConsistency:
    """Tests for cross-year line presence checking."""

    def test_consistent_presence_no_warnings(self) -> None:
        revenue = {
            "2022": [{"line_number": "12", "label": "Total revenue", "col_a": "100"}],
            "2023": [{"line_number": "12", "label": "Total revenue", "col_a": "200"}],
        }
        issues = check_line_presence_consistency(revenue, {})
        assert len(issues) == 0

    def test_missing_revenue_line_warns(self) -> None:
        revenue = {
            "2022": [
                {"line_number": "12", "label": "Total revenue", "col_a": "100"},
                {"line_number": "3", "label": "Investment income", "col_a": "50"},
            ],
            "2023": [{"line_number": "12", "label": "Total revenue", "col_a": "200"}],
        }
        issues = check_line_presence_consistency(revenue, {})
        assert len(issues) == 1
        issue = issues[0]
        assert issue.year == "cross-year"
        assert issue.severity == "WARNING"
        assert issue.category == "line_presence"
        assert "Revenue line 3" in issue.message
        assert "2022" in issue.message
        assert "2023" in issue.message

    def test_missing_expense_line_warns(self) -> None:
        expenses = {
            "2022": [{"line_number": "11b", "label": "Legal fees", "col_a": "10"}],
            "2023": [],
        }
        issues = check_line_presence_consistency({}, expenses)
        assert len(issues) == 1
        assert "Expense line 11b" in issues[0].message

    def test_label_included_in_message(self) -> None:
        revenue = {
            "2022": [{"line_number": "5", "label": "Royalties", "col_a": "1"}],
            "2023": [{"line_number": "12", "label": "Total revenue", "col_a": "2"}],
        }
        issues = check_line_presence_consistency(revenue, {})
        assert any("Royalties" in i.message for i in issues)

    def test_single_year_no_warnings(self) -> None:
        revenue = {"2023": [{"line_number": "12", "label": "Total revenue", "col_a": "100"}]}
        assert check_line_presence_consistency(revenue, {}) == []

    def test_empty_inputs_no_issues(self) -> None:
        assert check_line_presence_consistency({}, {}) == []

    def test_multiple_missing_years(self) -> None:
        revenue = {
            "2021": [{"line_number": "9c", "label": "Gaming", "col_a": "5"}],
            "2022": [{"line_number": "12", "label": "Total revenue", "col_a": "1"}],
            "2023": [{"line_number": "12", "label": "Total revenue", "col_a": "2"}],
        }
        issues = check_line_presence_consistency(revenue, {})
        presence_issues = [i for i in issues if "9c" in i.message]
        assert len(presence_issues) == 1
        assert "2022" in presence_issues[0].message
        assert "2023" in presence_issues[0].message


# =========================================================================
# _read_summary_csv — summary CSV parsing
# =========================================================================


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class TestReadSummaryCsv:
    """Tests for reading the summary financials CSV."""

    def test_returns_year_keyed_dict(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "mofc_990_financials.csv"
        rows = [
            {f: "" for f in FINANCIAL_FIELDS} | {"form_year": "2022", "total_revenue": "100"},
            {f: "" for f in FINANCIAL_FIELDS} | {"form_year": "2023", "total_revenue": "200"},
        ]
        _write_csv(csv_path, FINANCIAL_FIELDS, rows)

        result = _read_summary_csv(csv_path)

        assert set(result.keys()) == {"2022", "2023"}
        assert result["2022"]["total_revenue"] == "100"
        assert result["2023"]["total_revenue"] == "200"

    def test_single_year(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "mofc_990_financials.csv"
        rows = [{f: "" for f in FINANCIAL_FIELDS} | {"form_year": "2023"}]
        _write_csv(csv_path, FINANCIAL_FIELDS, rows)

        result = _read_summary_csv(csv_path)

        assert list(result.keys()) == ["2023"]


# =========================================================================
# _read_detail_csv — revenue/expense CSV parsing
# =========================================================================


class TestReadDetailCsv:
    """Tests for reading revenue/expense detail CSVs."""

    def test_revenue_roundtrip(self, tmp_path: Path) -> None:
        """Rows written with human-readable names should round-trip correctly."""
        csv_path = tmp_path / "revenue.csv"
        rows = [
            {
                "form_year": "2023",
                "line_number": "12",
                "label": "Total revenue",
                "total": "133634324",
                "related_or_exempt": "",
                "unrelated_business": "",
                "excluded_from_tax": "",
            }
        ]
        _write_csv(csv_path, REVENUE_CSV_FIELDS, rows)

        col_map = {
            "total": "col_a",
            "related_or_exempt": "col_b",
            "unrelated_business": "col_c",
            "excluded_from_tax": "col_d",
        }
        by_year, counts = _read_detail_csv(csv_path, col_map)

        assert "2023" in by_year
        assert counts["2023"] == 1
        item = by_year["2023"][0]
        assert item["line_number"] == "12"
        assert item["col_a"] == "133634324"
        assert item["col_b"] == ""

    def test_expense_roundtrip(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "expense.csv"
        rows = [
            {
                "form_year": "2023",
                "line_number": "25",
                "label": "Total functional expenses",
                "total": "134475329",
                "program_service": "124038575",
                "management_and_general": "6846503",
                "fundraising": "3590251",
            }
        ]
        _write_csv(csv_path, EXPENSE_CSV_FIELDS, rows)

        col_map = {
            "total": "col_a",
            "program_service": "col_b",
            "management_and_general": "col_c",
            "fundraising": "col_d",
        }
        by_year, counts = _read_detail_csv(csv_path, col_map)

        assert counts["2023"] == 1
        item = by_year["2023"][0]
        assert item["col_a"] == "134475329"
        assert item["col_b"] == "124038575"
        assert item["col_c"] == "6846503"
        assert item["col_d"] == "3590251"

    def test_groups_multiple_years(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "revenue.csv"
        rows = [
            {f: "" for f in REVENUE_CSV_FIELDS} | {"form_year": "2022", "line_number": "12"},
            {f: "" for f in REVENUE_CSV_FIELDS} | {"form_year": "2023", "line_number": "12"},
            {f: "" for f in REVENUE_CSV_FIELDS} | {"form_year": "2023", "line_number": "1h"},
        ]
        _write_csv(csv_path, REVENUE_CSV_FIELDS, rows)

        col_map = {
            "total": "col_a",
            "related_or_exempt": "col_b",
            "unrelated_business": "col_c",
            "excluded_from_tax": "col_d",
        }
        by_year, counts = _read_detail_csv(csv_path, col_map)

        assert counts["2022"] == 1
        assert counts["2023"] == 2


# =========================================================================
# run_validation_only — end-to-end CSV validation
# =========================================================================


def _make_processed_dir(tmp_path: Path) -> Path:
    """Write minimal valid CSVs to a temp directory and return its path."""
    out = tmp_path / "processed"
    out.mkdir()

    # Summary CSV
    summary_rows = [
        {f: "" for f in FINANCIAL_FIELDS}
        | {
            "form_year": "2023",
            "total_revenue": "133634324",
            "total_expenses": "134475329",
            "contributions_and_grants": "126739348",
            "program_service_revenue": "4362941",
            "investment_income": "2263725",
        }
    ]
    _write_csv(out / "mofc_990_financials.csv", FINANCIAL_FIELDS, summary_rows)

    # Revenue manual-edits CSV
    rev_rows = [
        {
            "form_year": "2023",
            "line_number": "1h",
            "label": "Total contributions",
            "total": "126739348",
            "related_or_exempt": "",
            "unrelated_business": "",
            "excluded_from_tax": "",
        },
        {
            "form_year": "2023",
            "line_number": "2g",
            "label": "Total program service revenue",
            "total": "4362941",
            "related_or_exempt": "",
            "unrelated_business": "",
            "excluded_from_tax": "",
        },
        {
            "form_year": "2023",
            "line_number": "3",
            "label": "Investment income",
            "total": "2263725",
            "related_or_exempt": "",
            "unrelated_business": "",
            "excluded_from_tax": "",
        },
        {
            "form_year": "2023",
            "line_number": "12",
            "label": "Total revenue",
            "total": "133634324",
            "related_or_exempt": "",
            "unrelated_business": "",
            "excluded_from_tax": "",
        },
    ]
    _write_csv(out / "mofc_990_revenue_detail_manual_edits.csv", REVENUE_CSV_FIELDS, rev_rows)

    # Expense manual-edits CSV
    exp_rows = [
        {
            "form_year": "2023",
            "line_number": "25",
            "label": "Total functional expenses",
            "total": "134475329",
            "program_service": "124038575",
            "management_and_general": "6846503",
            "fundraising": "3590251",
        }
    ]
    _write_csv(out / "mofc_990_expense_detail_manual_edits.csv", EXPENSE_CSV_FIELDS, exp_rows)

    return out


class TestRunValidationOnly:
    """End-to-end tests for run_validation_only()."""

    def test_prefers_manual_edits_csv(self, tmp_path: Path) -> None:
        """run_validation_only uses *_manual_edits.csv when present."""
        out = _make_processed_dir(tmp_path)
        issues = run_validation_only(out)
        assert "2023" in issues

    def test_falls_back_to_plain_csv(self, tmp_path: Path) -> None:
        """Falls back to plain extraction CSV when no manual-edits file exists."""
        out = _make_processed_dir(tmp_path)
        (out / "mofc_990_revenue_detail_manual_edits.csv").rename(
            out / "mofc_990_revenue_detail.csv"
        )
        (out / "mofc_990_expense_detail_manual_edits.csv").rename(
            out / "mofc_990_expense_detail.csv"
        )
        issues = run_validation_only(out)
        assert "2023" in issues

    def test_writes_validation_report(self, tmp_path: Path) -> None:
        out = _make_processed_dir(tmp_path)
        run_validation_only(out)
        report_path = out / "mofc_990_validation_report.txt"
        assert report_path.exists()
        assert "2023" in report_path.read_text()

    def test_good_data_produces_no_errors(self, tmp_path: Path) -> None:
        out = _make_processed_dir(tmp_path)
        issues = run_validation_only(out)
        errors = [i for i in issues.get("2023", []) if i.severity == "ERROR"]
        assert len(errors) == 0

    def test_missing_summary_csv_exits(self, tmp_path: Path) -> None:
        out = _make_processed_dir(tmp_path)
        (out / "mofc_990_financials.csv").unlink()
        with pytest.raises(SystemExit):
            run_validation_only(out)

    def test_missing_detail_csv_exits(self, tmp_path: Path) -> None:
        out = _make_processed_dir(tmp_path)
        (out / "mofc_990_revenue_detail_manual_edits.csv").unlink()
        with pytest.raises(SystemExit):
            run_validation_only(out)
