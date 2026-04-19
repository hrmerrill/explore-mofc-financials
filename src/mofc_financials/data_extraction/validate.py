"""Cross-validate MOFC 990 extraction results and orchestrate the pipeline.

This module provides a two-stage workflow:

**Stage 1 — Extraction** (run once per new PDF set)::

    mofc-pipeline

1. OCR-extract Part I Summary + Parts VIII/IX detail from each PDF
2. Write ``mofc_990_financials.csv``, ``mofc_990_revenue_detail.csv``,
   and ``mofc_990_expense_detail.csv`` to ``data/processed/``
3. Run cross-validation and write ``mofc_990_validation_report.txt``

**Stage 2 — Manual correction + re-validation** (iterate until clean)::

    # 1. Copy extraction output to editable files (only needed once)
    cp data/processed/mofc_990_revenue_detail.csv \\
       data/processed/mofc_990_revenue_detail_manual_edits.csv
    cp data/processed/mofc_990_expense_detail.csv \\
       data/processed/mofc_990_expense_detail_manual_edits.csv

    # 2. Open the report and fix values in the *_manual_edits.csv files
    #    Compare against source PDFs in data/raw/ as needed.

    # 3. Re-validate the edited files (no OCR required)
    mofc-validate

``mofc-validate`` reads ``*_manual_edits.csv`` when present, falling back
to the original extraction CSVs, and overwrites the validation report.

Programmatic usage
------------------
::

    from mofc_financials.data_extraction.validate import run_pipeline, run_validation_only

    # Full extraction + validation
    issues = run_pipeline(Path("data/raw"), Path("data/processed"))

    # Validate manually edited CSVs only
    issues = run_validation_only(Path("data/processed"))
"""

from __future__ import annotations

import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from mofc_financials.data_extraction.extract_990 import (
    FINANCIAL_FIELDS,
    extract_financials,
)
from mofc_financials.data_extraction.extract_990_detail import (
    LineItemRow,
    extract_expense_detail,
    extract_revenue_detail,
)

# ---------------------------------------------------------------------------
# CSV column definitions (public, shared with tests)
# ---------------------------------------------------------------------------

REVENUE_CSV_FIELDS: list[str] = [
    "form_year",
    "line_number",
    "label",
    "total",
    "related_or_exempt",
    "unrelated_business",
    "excluded_from_tax",
]

EXPENSE_CSV_FIELDS: list[str] = [
    "form_year",
    "line_number",
    "label",
    "total",
    "program_service",
    "management_and_general",
    "fundraising",
]

_COL_TO_REVENUE: dict[str, str] = {
    "col_a": "total",
    "col_b": "related_or_exempt",
    "col_c": "unrelated_business",
    "col_d": "excluded_from_tax",
}

_COL_TO_EXPENSE: dict[str, str] = {
    "col_a": "total",
    "col_b": "program_service",
    "col_c": "management_and_general",
    "col_d": "fundraising",
}

# ---------------------------------------------------------------------------
# Validation data types
# ---------------------------------------------------------------------------


@dataclass
class ValidationIssue:
    """A single finding from cross-validation or consistency checks.

    Attributes
    ----------
    year : str
        Tax year (e.g. ``"2023"``).
    severity : str
        ``"ERROR"`` for items requiring correction, ``"WARNING"`` for items
        to verify.
    category : str
        Issue category: ``cross_validation``, ``internal_consistency``,
        ``completeness``, ``duplicate``, or ``suspicious``.
    message : str
        Human-readable description of the issue.
    """

    year: str
    severity: str
    category: str
    message: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_int(val: str) -> int | None:
    """Parse a numeric string to int, returning None for blanks.

    Parameters
    ----------
    val : str
        Numeric string, possibly with a leading ``-`` for negatives.

    Returns
    -------
    int or None
        Parsed integer, or ``None`` if the string is empty or non-numeric.
    """
    cleaned = val.strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _pct_diff(actual: int, expected: int) -> float:
    """Compute relative difference between two values.

    Parameters
    ----------
    actual : int
        Measured value.
    expected : int
        Reference value.

    Returns
    -------
    float
        Relative difference as a fraction (0.0–1.0+).  Returns ``inf``
        when *expected* is zero and *actual* is not.
    """
    if expected == 0:
        return float("inf") if actual != 0 else 0.0
    return abs(actual - expected) / abs(expected)


def _detail_lookup(rows: list[LineItemRow], line_number: str) -> LineItemRow | None:
    """Find the first line item matching a given line number.

    Parameters
    ----------
    rows : list[LineItemRow]
        Extracted line items.
    line_number : str
        Line number to search for (e.g. ``"25"``).

    Returns
    -------
    LineItemRow or None
        The matching row, or ``None`` if not found.
    """
    for r in rows:
        if r.get("line_number") == line_number:
            return r
    return None


def _rows_to_csv(
    rows: list[LineItemRow],
    form_year: str,
    col_map: dict[str, str],
    fields: list[str],
) -> list[dict[str, str]]:
    """Convert internal ``LineItemRow`` dicts to CSV-ready dicts.

    Parameters
    ----------
    rows : list[LineItemRow]
        Extracted line items with ``col_a``–``col_d`` keys.
    form_year : str
        Tax year to stamp on each row.
    col_map : dict[str, str]
        Maps ``col_a``–``col_d`` to human-readable column names.
    fields : list[str]
        Ordered list of CSV field names.

    Returns
    -------
    list[dict[str, str]]
        CSV-ready dicts with human-readable column names.
    """
    csv_rows: list[dict[str, str]] = []
    for row in rows:
        csv_row = {f: "" for f in fields}
        csv_row["form_year"] = form_year
        csv_row["line_number"] = row.get("line_number", "")
        csv_row["label"] = row.get("label", "")
        for col_key, col_name in col_map.items():
            csv_row[col_name] = str(row.get(col_key, ""))
        csv_rows.append(csv_row)
    return csv_rows


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    """Write rows to a CSV file.

    Parameters
    ----------
    path : Path
        Output file path.
    fieldnames : list[str]
        Ordered column names for the CSV header.
    rows : list[dict[str, str]]
        Row data as dictionaries.
    """
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_summary_csv(path: Path) -> dict[str, dict[str, str]]:
    """Read the summary CSV into a year-keyed dict.

    Parameters
    ----------
    path : Path
        Path to ``mofc_990_financials.csv``.

    Returns
    -------
    dict[str, dict[str, str]]
        Mapping of tax year to summary row dict.
    """
    by_year: dict[str, dict[str, str]] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year = row.get("form_year", "")
            by_year[year] = dict(row)
    return by_year


def _read_detail_csv(
    path: Path,
    col_map: dict[str, str],
) -> tuple[dict[str, list[LineItemRow]], dict[str, int]]:
    """Read a detail CSV and return rows grouped by year.

    Converts human-readable column names (e.g. ``total``, ``program_service``)
    back to the internal ``col_a``/``col_b``/``col_c``/``col_d`` keys used by
    the validation logic.

    Parameters
    ----------
    path : Path
        Path to a revenue or expense detail CSV.
    col_map : dict[str, str]
        Mapping from human-readable column name to internal key
        (e.g. ``{"total": "col_a", "program_service": "col_b", ...}``).

    Returns
    -------
    tuple[dict[str, list[LineItemRow]], dict[str, int]]
        A pair of ``(rows_by_year, count_by_year)`` where *rows_by_year*
        maps each tax year to its list of ``LineItemRow`` dicts, and
        *count_by_year* maps each year to the number of rows.
    """
    by_year: dict[str, list[LineItemRow]] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year = row.get("form_year", "")
            # Build as a plain dict to allow dynamic key assignment, then cast
            # to LineItemRow (TypedDict keys must be literals for mypy).
            item_dict: dict[str, str] = {
                "line_number": row.get("line_number", ""),
                "label": row.get("label", ""),
            }
            for human_col, col_key in col_map.items():
                item_dict[col_key] = row.get(human_col, "")
            by_year.setdefault(year, []).append(cast(LineItemRow, item_dict))
    counts = {year: len(rows) for year, rows in by_year.items()}
    return by_year, counts


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------

# Revenue component lines whose col_a values should sum to line 12
_REVENUE_COMPONENT_LINES = {"1h", "2g", "3", "4", "5", "6d", "7d", "8c", "9c", "10c", "11e"}

# Contribution sub-lines whose col_a values should sum to line 1h
# (1g is a memo line, not additive)
_CONTRIBUTION_ADDITIVE_LINES = {"1a", "1b", "1c", "1d", "1e", "1f"}

# Tolerance thresholds
_CROSS_VALIDATION_TOLERANCE = 0.02  # 2 % for detail-vs-summary checks
_SUBLINE_SUM_TOLERANCE = 0.05  # 5 % for subline sums (missing lines expected)
_COLUMN_SUM_TOLERANCE = 0.02  # 2 % for col_b+c+d vs col_a

# Sentinel key used in all_issues for cross-year findings
_CROSS_YEAR_KEY = "cross-year"


def check_label_consistency(
    revenue_by_year: dict[str, list[LineItemRow]],
    expenses_by_year: dict[str, list[LineItemRow]],
) -> list[ValidationIssue]:
    """Warn when the same line number has different labels across tax years.

    Parameters
    ----------
    revenue_by_year : dict[str, list[LineItemRow]]
        Revenue detail rows grouped by tax year.
    expenses_by_year : dict[str, list[LineItemRow]]
        Expense detail rows grouped by tax year.

    Returns
    -------
    list[ValidationIssue]
        One warning per line number whose label varies across years, for
        each section (revenue and expenses).  All issues use the sentinel
        year ``"cross-year"``.
    """
    issues: list[ValidationIssue] = []

    for section_name, by_year in [
        ("revenue", revenue_by_year),
        ("expense", expenses_by_year),
    ]:
        # Map line_number -> {label -> sorted list of years that use it}
        label_map: dict[str, dict[str, list[str]]] = {}
        for year, rows in sorted(by_year.items()):
            for row in rows:
                ln = row.get("line_number", "")
                label = row.get("label", "").strip()
                if not ln or not label:
                    continue
                label_map.setdefault(ln, {}).setdefault(label, []).append(year)

        for ln, labels_to_years in sorted(label_map.items(), key=lambda x: (len(x[0]), x[0])):
            if len(labels_to_years) > 1:
                detail = "; ".join(
                    f"{', '.join(years)}: {label!r}"
                    for label, years in sorted(labels_to_years.items())
                )
                issues.append(
                    ValidationIssue(
                        _CROSS_YEAR_KEY,
                        "WARNING",
                        "label_consistency",
                        f"{section_name.capitalize()} line {ln} has inconsistent labels "
                        f"across years — {detail}",
                    )
                )

    return issues


def validate_year(
    year: str,
    summary: dict[str, str],
    revenue: list[LineItemRow],
    expenses: list[LineItemRow],
) -> list[ValidationIssue]:
    """Run all validation checks for a single tax year.

    Parameters
    ----------
    year : str
        Tax year (e.g. ``"2023"``).
    summary : dict[str, str]
        Part I Summary extraction result from ``extract_financials``.
    revenue : list[LineItemRow]
        Part VIII revenue detail from ``extract_revenue_detail``.
    expenses : list[LineItemRow]
        Part IX expense detail from ``extract_expense_detail``.

    Returns
    -------
    list[ValidationIssue]
        All issues found, sorted by severity (errors first).
    """
    issues: list[ValidationIssue] = []

    _check_completeness(year, revenue, expenses, issues)
    _check_cross_validation(year, summary, revenue, expenses, issues)
    _check_internal_consistency(year, revenue, expenses, issues)
    _check_duplicates(year, revenue, expenses, issues)
    _check_suspicious_values(year, revenue, expenses, issues)

    # Sort: errors first, then warnings
    issues.sort(key=lambda i: (0 if i.severity == "ERROR" else 1, i.category))
    return issues


def _check_completeness(
    year: str,
    revenue: list[LineItemRow],
    expenses: list[LineItemRow],
    issues: list[ValidationIssue],
) -> None:
    """Check that essential total lines are present and populated.

    Parameters
    ----------
    year : str
        Tax year.
    revenue : list[LineItemRow]
        Revenue detail rows.
    expenses : list[LineItemRow]
        Expense detail rows.
    issues : list[ValidationIssue]
        Accumulator for findings.
    """
    total_line_checks: list[tuple[str, str, list[LineItemRow], str]] = [
        ("12", "Total revenue", revenue, "revenue"),
        ("25", "Total functional expenses", expenses, "expense"),
        ("1h", "Total contributions and grants", revenue, "revenue"),
        ("2g", "Total program service revenue", revenue, "revenue"),
    ]
    for ln, label, rows, section in total_line_checks:
        row = _detail_lookup(rows, ln)
        if row is None:
            issues.append(
                ValidationIssue(
                    year,
                    "ERROR",
                    "completeness",
                    f"{section.capitalize()} line {ln} ({label}) not extracted",
                )
            )
        elif not row.get("col_a"):
            issues.append(
                ValidationIssue(
                    year,
                    "ERROR",
                    "completeness",
                    f"Line {ln} ({label}) found but total column is empty",
                )
            )


def _check_cross_validation(
    year: str,
    summary: dict[str, str],
    revenue: list[LineItemRow],
    expenses: list[LineItemRow],
    issues: list[ValidationIssue],
) -> None:
    """Compare detail total lines against Part I Summary values.

    Parameters
    ----------
    year : str
        Tax year.
    summary : dict[str, str]
        Summary extraction result.
    revenue : list[LineItemRow]
        Revenue detail rows.
    expenses : list[LineItemRow]
        Expense detail rows.
    issues : list[ValidationIssue]
        Accumulator for findings.
    """
    cross_checks: list[tuple[str, str, list[LineItemRow]]] = [
        ("12", "total_revenue", revenue),
        ("25", "total_expenses", expenses),
        ("1h", "contributions_and_grants", revenue),
        ("2g", "program_service_revenue", revenue),
        ("3", "investment_income", revenue),
    ]
    for ln, summary_field, rows in cross_checks:
        row = _detail_lookup(rows, ln)
        s_val = _to_int(summary.get(summary_field, ""))
        d_val = _to_int(row.get("col_a", "") if row else "")

        if s_val is None or d_val is None:
            continue

        diff = _pct_diff(d_val, s_val)
        if diff > _CROSS_VALIDATION_TOLERANCE:
            issues.append(
                ValidationIssue(
                    year,
                    "ERROR",
                    "cross_validation",
                    f"Line {ln} total={d_val:,} vs summary {summary_field}={s_val:,} "
                    f"({diff:.1%} difference)",
                )
            )


def _check_internal_consistency(
    year: str,
    revenue: list[LineItemRow],
    expenses: list[LineItemRow],
    issues: list[ValidationIssue],
) -> None:
    """Check that column sums and subline sums are internally consistent.

    Parameters
    ----------
    year : str
        Tax year.
    revenue : list[LineItemRow]
        Revenue detail rows.
    expenses : list[LineItemRow]
        Expense detail rows.
    issues : list[ValidationIssue]
        Accumulator for findings.
    """
    # Expense line 25: col_b + col_c + col_d ≈ col_a
    exp_total = _detail_lookup(expenses, "25")
    if exp_total:
        col_a = _to_int(exp_total.get("col_a", ""))
        col_b = _to_int(exp_total.get("col_b", "")) or 0
        col_c = _to_int(exp_total.get("col_c", "")) or 0
        col_d = _to_int(exp_total.get("col_d", "")) or 0
        if col_a and col_a > 0:
            col_sum = col_b + col_c + col_d
            diff = _pct_diff(col_sum, col_a)
            if diff > _COLUMN_SUM_TOLERANCE:
                issues.append(
                    ValidationIssue(
                        year,
                        "ERROR",
                        "internal_consistency",
                        f"Expense line 25: col_b+c+d={col_sum:,} vs total={col_a:,} "
                        f"({diff:.1%} difference)",
                    )
                )

    # Expense sublines sum ≈ line 25 for all four columns
    if exp_total:
        col_labels = {
            "col_a": "total",
            "col_b": "program_service",
            "col_c": "management_and_general",
            "col_d": "fundraising",
        }
        for col_key, col_label in col_labels.items():
            expected = _to_int(str(exp_total.get(col_key, "")))
            if not expected:
                continue
            component_sum = sum(
                _to_int(str(r.get(col_key, ""))) or 0
                for r in expenses
                if r.get("line_number") not in ("25", "26")
            )
            diff = _pct_diff(component_sum, expected)
            if diff > _SUBLINE_SUM_TOLERANCE:
                issues.append(
                    ValidationIssue(
                        year,
                        "WARNING",
                        "internal_consistency",
                        f"Expense sublines {col_label} sum={component_sum:,} vs "
                        f"line 25 {col_label}={expected:,} "
                        f"({diff:.1%} — likely missing lines)",
                    )
                )

    # Revenue component lines sum ≈ line 12
    rev_total = _detail_lookup(revenue, "12")
    if rev_total:
        expected = _to_int(rev_total.get("col_a", ""))
        if expected and abs(expected) > 100:
            component_sum = sum(
                _to_int(r.get("col_a", "")) or 0
                for r in revenue
                if r.get("line_number") in _REVENUE_COMPONENT_LINES
            )
            diff = _pct_diff(component_sum, expected)
            if diff > _SUBLINE_SUM_TOLERANCE:
                issues.append(
                    ValidationIssue(
                        year,
                        "WARNING",
                        "internal_consistency",
                        f"Revenue components sum={component_sum:,} vs line 12={expected:,} "
                        f"({diff:.1%} — likely missing lines)",
                    )
                )

    # Contribution sub-lines 1a-1f sum ≈ 1h
    contrib_total = _detail_lookup(revenue, "1h")
    if contrib_total:
        expected = _to_int(contrib_total.get("col_a", ""))
        if expected and abs(expected) > 100:
            additive_sum = sum(
                _to_int(r.get("col_a", "")) or 0
                for r in revenue
                if r.get("line_number") in _CONTRIBUTION_ADDITIVE_LINES
            )
            diff = _pct_diff(additive_sum, expected)
            if diff > _SUBLINE_SUM_TOLERANCE:
                issues.append(
                    ValidationIssue(
                        year,
                        "WARNING",
                        "internal_consistency",
                        f"Contribution sub-lines sum={additive_sum:,} vs 1h={expected:,} "
                        f"({diff:.1%} — likely missing lines)",
                    )
                )

    # Per-row: col_b + col_c + col_d ≈ col_a for every expense row with all columns present
    for row in expenses:
        col_a = _to_int(row.get("col_a", ""))
        col_b = _to_int(row.get("col_b", "")) or 0
        col_c = _to_int(row.get("col_c", "")) or 0
        col_d = _to_int(row.get("col_d", "")) or 0
        if col_a is None or not any([col_b, col_c, col_d]):
            continue
        col_sum = col_b + col_c + col_d
        diff = _pct_diff(col_sum, col_a)
        if diff > _COLUMN_SUM_TOLERANCE:
            ln = row.get("line_number", "?")
            label = row.get("label", "")
            issues.append(
                ValidationIssue(
                    year,
                    "WARNING",
                    "internal_consistency",
                    f"Expense line {ln} ({label}): col_b+c+d={col_sum:,} vs total={col_a:,} "
                    f"({diff:.1%} difference)",
                )
            )

    # Per-row: col_b + col_c + col_d ≈ col_a for every revenue row with all columns present
    for row in revenue:
        col_a = _to_int(row.get("col_a", ""))
        col_b = _to_int(row.get("col_b", "")) or 0
        col_c = _to_int(row.get("col_c", "")) or 0
        col_d = _to_int(row.get("col_d", "")) or 0
        if col_a is None or not any([col_b, col_c, col_d]):
            continue
        col_sum = col_b + col_c + col_d
        diff = _pct_diff(col_sum, col_a)
        if diff > _COLUMN_SUM_TOLERANCE:
            ln = row.get("line_number", "?")
            label = row.get("label", "")
            issues.append(
                ValidationIssue(
                    year,
                    "WARNING",
                    "internal_consistency",
                    f"Revenue line {ln} ({label}): col_b+c+d={col_sum:,} vs total={col_a:,} "
                    f"({diff:.1%} difference)",
                )
            )


def _check_duplicates(
    year: str,
    revenue: list[LineItemRow],
    expenses: list[LineItemRow],
    issues: list[ValidationIssue],
) -> None:
    """Flag duplicate line numbers within a section.

    Parameters
    ----------
    year : str
        Tax year.
    revenue : list[LineItemRow]
        Revenue detail rows.
    expenses : list[LineItemRow]
        Expense detail rows.
    issues : list[ValidationIssue]
        Accumulator for findings.
    """
    for section_name, rows in [("revenue", revenue), ("expense", expenses)]:
        seen: dict[str, int] = {}
        for r in rows:
            ln = r.get("line_number", "")
            seen[ln] = seen.get(ln, 0) + 1
        for ln, count in seen.items():
            if count > 1:
                issues.append(
                    ValidationIssue(
                        year,
                        "WARNING",
                        "duplicate",
                        f"{section_name.capitalize()} line {ln} appears {count} times "
                        f"— verify which value is correct",
                    )
                )


def _check_suspicious_values(
    year: str,
    revenue: list[LineItemRow],
    expenses: list[LineItemRow],
    issues: list[ValidationIssue],
) -> None:
    """Flag values that are suspiciously small for their context.

    Parameters
    ----------
    year : str
        Tax year.
    revenue : list[LineItemRow]
        Revenue detail rows.
    expenses : list[LineItemRow]
        Expense detail rows.
    issues : list[ValidationIssue]
        Accumulator for findings.
    """
    # Total/subtotal lines should not have tiny values
    total_lines = {"12", "25", "1h", "2g"}
    for rows in [revenue, expenses]:
        for r in rows:
            ln = r.get("line_number", "")
            col_a = _to_int(r.get("col_a", ""))
            if col_a is None:
                continue
            label = r.get("label", "")
            if ln in total_lines and 0 < abs(col_a) < 10_000:
                issues.append(
                    ValidationIssue(
                        year,
                        "ERROR",
                        "suspicious",
                        f"Line {ln} ({label}) total={col_a:,} — suspiciously small "
                        f"for a total line (likely OCR error)",
                    )
                )


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------


def format_report(
    all_issues: dict[str, list[ValidationIssue]],
    revenue_counts: dict[str, int],
    expense_counts: dict[str, int],
    output_files: list[str],
) -> str:
    """Format validation findings into a human-readable report.

    Parameters
    ----------
    all_issues : dict[str, list[ValidationIssue]]
        Issues keyed by tax year.
    revenue_counts : dict[str, int]
        Number of revenue lines extracted per year.
    expense_counts : dict[str, int]
        Number of expense lines extracted per year.
    output_files : list[str]
        Paths to generated CSV files.

    Returns
    -------
    str
        Multi-line report text.
    """
    lines: list[str] = []
    lines.append("MOFC 990 Financial Data — Extraction Validation Report")
    lines.append("=" * 56)
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append("Output files:")
    for f in output_files:
        lines.append(f"  • {f}")
    lines.append("")
    lines.append("This report flags values that may need manual correction.")
    lines.append("Compare flagged items against the source PDFs in data/raw/.")
    lines.append("")

    total_errors = 0
    total_warnings = 0

    year_keys = sorted(k for k in all_issues if k != _CROSS_YEAR_KEY)

    for year in year_keys:
        year_issues = all_issues[year]
        errors = [i for i in year_issues if i.severity == "ERROR"]
        warnings = [i for i in year_issues if i.severity == "WARNING"]
        total_errors += len(errors)
        total_warnings += len(warnings)

        lines.append(f"--- {year} ---")
        lines.append(
            f"Extracted: {revenue_counts.get(year, 0)} revenue lines, "
            f"{expense_counts.get(year, 0)} expense lines"
        )

        if errors:
            lines.append("")
            lines.append("ERRORS (require manual correction):")
            for e in errors:
                lines.append(f"  ✗ [{e.category.upper()}] {e.message}")

        if warnings:
            lines.append("")
            lines.append("WARNINGS (verify against source PDF):")
            for w in warnings:
                lines.append(f"  ⚠ [{w.category.upper()}] {w.message}")

        if not errors and not warnings:
            lines.append("  ✓ No issues found")

        lines.append("")

    # Cross-year section
    cross_year = all_issues.get(_CROSS_YEAR_KEY, [])
    if cross_year:
        total_warnings += len(cross_year)
        lines.append("--- cross-year ---")
        lines.append("WARNINGS (label inconsistencies across tax years):")
        for w in cross_year:
            lines.append(f"  ⚠ [{w.category.upper()}] {w.message}")
        lines.append("")

    # Summary table
    lines.append("=" * 56)
    lines.append("SUMMARY")
    lines.append(f"{'Year':<6} {'Errors':>7} {'Warnings':>9} {'Revenue':>9} {'Expense':>9}")
    lines.append("-" * 44)
    for year in year_keys:
        year_issues = all_issues[year]
        n_err = sum(1 for i in year_issues if i.severity == "ERROR")
        n_warn = sum(1 for i in year_issues if i.severity == "WARNING")
        r = revenue_counts.get(year, 0)
        x = expense_counts.get(year, 0)
        lines.append(f"{year:<6} {n_err:>7} {n_warn:>9} {r:>9} {x:>9}")
    lines.append("-" * 44)
    n_cross = len(cross_year)
    lines.append(
        f"Total: {total_errors} errors, {total_warnings} warnings "
        f"across {len(year_keys)} years" + (f" ({n_cross} cross-year)" if n_cross else "")
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


def run_pipeline(
    data_dir: Path,
    output_dir: Path,
) -> dict[str, list[ValidationIssue]]:
    """Run the full extraction-and-validation pipeline.

    For each PDF in *data_dir* matching ``MOFC-990-*.pdf``:

    1. Extract Part I Summary via ``extract_financials``
    2. Extract Part VIII revenue detail via ``extract_revenue_detail``
    3. Extract Part IX expense detail via ``extract_expense_detail``
    4. Cross-validate detail totals against summary values

    Writes three CSV files and a validation report to *output_dir*.

    Parameters
    ----------
    data_dir : Path
        Directory containing ``MOFC-990-*.pdf`` files.
    output_dir : Path
        Directory for CSV and report output.

    Returns
    -------
    dict[str, list[ValidationIssue]]
        Validation issues keyed by tax year.
    """
    pdfs = sorted(data_dir.glob("MOFC-990-*.pdf"))
    if not pdfs:
        print("No 990 PDFs found in", data_dir, file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    all_summary_rows: list[dict[str, str]] = []
    all_revenue_rows: list[dict[str, str]] = []
    all_expense_rows: list[dict[str, str]] = []
    all_issues: dict[str, list[ValidationIssue]] = {}
    revenue_counts: dict[str, int] = {}
    expense_counts: dict[str, int] = {}
    revenue_by_year: dict[str, list[LineItemRow]] = {}
    expenses_by_year: dict[str, list[LineItemRow]] = {}

    for pdf in pdfs:
        year_match = re.search(r"(\d{4})", pdf.stem)
        year = year_match.group(1) if year_match else pdf.stem
        print(f"Processing {pdf.name}...", file=sys.stderr)

        # Step 1: Summary extraction
        summary = extract_financials(str(pdf))
        all_summary_rows.append(summary)

        # Step 2: Detail extraction
        revenue = extract_revenue_detail(str(pdf))
        expenses = extract_expense_detail(str(pdf))

        revenue_by_year[year] = revenue
        expenses_by_year[year] = expenses
        revenue_counts[year] = len(revenue)
        expense_counts[year] = len(expenses)

        # Convert to CSV format
        all_revenue_rows.extend(_rows_to_csv(revenue, year, _COL_TO_REVENUE, REVENUE_CSV_FIELDS))
        all_expense_rows.extend(_rows_to_csv(expenses, year, _COL_TO_EXPENSE, EXPENSE_CSV_FIELDS))

        # Step 3: Validate
        all_issues[year] = validate_year(year, summary, revenue, expenses)

    # Step 4: Cross-year label consistency
    all_issues[_CROSS_YEAR_KEY] = check_label_consistency(revenue_by_year, expenses_by_year)

    # Write CSVs
    summary_path = output_dir / "mofc_990_financials.csv"
    _write_csv(summary_path, FINANCIAL_FIELDS, all_summary_rows)

    rev_path = output_dir / "mofc_990_revenue_detail.csv"
    _write_csv(rev_path, REVENUE_CSV_FIELDS, all_revenue_rows)

    exp_path = output_dir / "mofc_990_expense_detail.csv"
    _write_csv(exp_path, EXPENSE_CSV_FIELDS, all_expense_rows)

    # Step 4: Write report
    output_files = [str(summary_path), str(rev_path), str(exp_path)]
    report = format_report(all_issues, revenue_counts, expense_counts, output_files)
    report_path = output_dir / "mofc_990_validation_report.txt"
    report_path.write_text(report)

    # Print summary to stderr
    total_e = sum(len([i for i in v if i.severity == "ERROR"]) for v in all_issues.values())
    total_w = sum(len([i for i in v if i.severity == "WARNING"]) for v in all_issues.values())
    print(f"\nWrote {len(all_summary_rows)} summary rows to {summary_path}", file=sys.stderr)
    print(f"Wrote {len(all_revenue_rows)} revenue detail rows to {rev_path}", file=sys.stderr)
    print(f"Wrote {len(all_expense_rows)} expense detail rows to {exp_path}", file=sys.stderr)
    print(f"Wrote validation report to {report_path}", file=sys.stderr)
    print(f"  → {total_e} errors, {total_w} warnings", file=sys.stderr)

    # Print report to stdout
    print(report)

    return all_issues


def run_validation_only(output_dir: Path) -> dict[str, list[ValidationIssue]]:
    """Validate manually edited CSV files without re-running OCR extraction.

    Reads ``*_manual_edits.csv`` detail files from *output_dir* when present,
    falling back to the plain extraction CSVs.  Cross-validates against the
    summary CSV (``mofc_990_financials.csv``) and overwrites the validation
    report.

    Parameters
    ----------
    output_dir : Path
        Directory containing the processed CSV files written by
        :func:`run_pipeline`.

    Returns
    -------
    dict[str, list[ValidationIssue]]
        Validation issues keyed by tax year.

    Raises
    ------
    SystemExit
        If the summary CSV or any required detail CSV is not found.
    """
    summary_path = output_dir / "mofc_990_financials.csv"
    if not summary_path.exists():
        print(f"Summary CSV not found: {summary_path}", file=sys.stderr)
        sys.exit(1)

    # Prefer *_manual_edits.csv, fall back to original extraction CSV
    rev_path = output_dir / "mofc_990_revenue_detail_manual_edits.csv"
    if not rev_path.exists():
        rev_path = output_dir / "mofc_990_revenue_detail.csv"

    exp_path = output_dir / "mofc_990_expense_detail_manual_edits.csv"
    if not exp_path.exists():
        exp_path = output_dir / "mofc_990_expense_detail.csv"

    for p in (rev_path, exp_path):
        if not p.exists():
            print(f"Detail CSV not found: {p}", file=sys.stderr)
            sys.exit(1)

    print(f"Validating {rev_path.name} and {exp_path.name}...", file=sys.stderr)

    summary_by_year = _read_summary_csv(summary_path)

    # Reverse the col maps so human-readable names → col_a/col_b/...
    rev_col_map = {v: k for k, v in _COL_TO_REVENUE.items()}
    exp_col_map = {v: k for k, v in _COL_TO_EXPENSE.items()}

    revenue_by_year, revenue_counts = _read_detail_csv(rev_path, rev_col_map)
    expenses_by_year, expense_counts = _read_detail_csv(exp_path, exp_col_map)

    all_years = sorted(set(summary_by_year) | set(revenue_by_year) | set(expenses_by_year))
    all_issues: dict[str, list[ValidationIssue]] = {}

    for year in all_years:
        summary = summary_by_year.get(year, {})
        revenue = revenue_by_year.get(year, [])
        expenses = expenses_by_year.get(year, [])
        all_issues[year] = validate_year(year, summary, revenue, expenses)

    # Cross-year label consistency
    all_issues[_CROSS_YEAR_KEY] = check_label_consistency(revenue_by_year, expenses_by_year)

    output_files = [str(rev_path), str(exp_path)]
    report = format_report(all_issues, revenue_counts, expense_counts, output_files)
    report_path = output_dir / "mofc_990_validation_report.txt"
    report_path.write_text(report)

    total_e = sum(len([i for i in v if i.severity == "ERROR"]) for v in all_issues.values())
    total_w = sum(len([i for i in v if i.severity == "WARNING"]) for v in all_issues.values())
    print(f"Wrote validation report to {report_path}", file=sys.stderr)
    print(f"  \u2192 {total_e} errors, {total_w} warnings", file=sys.stderr)

    print(report)

    return all_issues


def main() -> None:
    """Run the full MOFC 990 extraction pipeline with validation.

    Scans ``data/raw/`` for ``MOFC-990-*.pdf`` files, runs summary and
    detail extraction, writes CSVs to ``data/processed/``, and produces
    a validation report flagging items for manual review.
    """
    data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw"
    output_dir = data_dir.parent / "processed"
    run_pipeline(data_dir, output_dir)


def validate_main() -> None:
    """Re-validate manually edited CSVs without re-running OCR extraction.

    Reads ``*_manual_edits.csv`` files from ``data/processed/``, falling back
    to the original extraction CSVs when edited versions are absent.  Writes
    an updated ``mofc_990_validation_report.txt``.
    """
    output_dir = Path(__file__).resolve().parent.parent.parent.parent / "data" / "processed"
    run_validation_only(output_dir)


if __name__ == "__main__":
    main()
