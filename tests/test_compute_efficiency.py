"""Tests for compute_efficiency module."""

import csv
import io
from pathlib import Path

import pytest

from mofc_financials.data_extraction.compute_efficiency import (
    FIELDNAMES,
    MEALS_PER_POUND,
    _safe_div,
    _to_float,
    _to_int,
    compute_metrics,
    main,
)

# ---------------------------------------------------------------------------
# Helper converters
# ---------------------------------------------------------------------------


class TestToInt:
    """Tests for _to_int."""

    def test_plain(self) -> None:
        assert _to_int("123") == 123

    def test_with_spaces(self) -> None:
        assert _to_int("  456  ") == 456

    def test_float_string(self) -> None:
        assert _to_int("1234.0") == 1234

    def test_empty(self) -> None:
        assert _to_int("") is None

    def test_non_numeric(self) -> None:
        assert _to_int("abc") is None


class TestToFloat:
    """Tests for _to_float."""

    def test_plain(self) -> None:
        assert _to_float("1.5") == 1.5

    def test_empty(self) -> None:
        assert _to_float("") is None

    def test_non_numeric(self) -> None:
        assert _to_float("xyz") is None


class TestSafeDiv:
    """Tests for _safe_div."""

    def test_normal(self) -> None:
        assert _safe_div(10, 4) == 2.5

    def test_zero_denominator(self) -> None:
        assert _safe_div(10, 0) == ""

    def test_none_numerator(self) -> None:
        assert _safe_div(None, 5) == ""

    def test_none_denominator(self) -> None:
        assert _safe_div(5, None) == ""


# ---------------------------------------------------------------------------
# compute_metrics
# ---------------------------------------------------------------------------

# Minimal audit row for testing
_AUDIT_ROW = {
    "form_year": "2021",
    "donated_lbs_beginning_inv": "1000000",
    "donated_lbs_received_total": "5000000",
    "donated_lbs_received_tefap": "2000000",
    "donated_lbs_received_csfp": "500000",
    "donated_lbs_received_oh_food": "1000000",
    "donated_lbs_received_industry": "1500000",
    "donated_lbs_disbursed_total": "-4500000",
    "donated_lbs_disbursed_tefap": "-2000000",
    "donated_lbs_disbursed_csfp": "-400000",
    "donated_lbs_disbursed_oh_food": "-900000",
    "donated_lbs_disbursed_industry": "-1200000",
    "donated_lbs_discarded": "-100000",
    "purchased_lbs_distributed": "-500000",
    "purchased_lbs_purchases": "600000",
    "purchased_val_purchases": "300000",
}

_FIN_ROW = {
    "form_year": "2021",
    "total_expenses": "10000000",
    "employees": "200",
    "salaries_and_compensation": "2000000",
}

_EXP_ROWS = [
    {"form_year": "2021", "line_number": "24a", "total": "7000000", "program_service": ""},
    {"form_year": "2021", "line_number": "5", "total": "1500000", "program_service": ""},
    {"form_year": "2021", "line_number": "7", "total": "200000", "program_service": ""},
    {
        "form_year": "2021",
        "line_number": "25",
        "total": "10000000",
        "program_service": "9200000",
    },
]


class TestComputeMetrics:
    """Tests for compute_metrics."""

    def test_total_lbs(self) -> None:
        """Total lbs = donated disbursed + purchased distributed."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        assert result["total_lbs_distributed"] == 4500000 + 500000

    def test_estimated_meals(self) -> None:
        """Estimated meals = total lbs × 1.2."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        assert result["estimated_meals"] == round(5000000 * MEALS_PER_POUND)

    def test_food_expense_from_24a(self) -> None:
        """Food expense taken from line 24a."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        assert result["food_expense"] == 7000000

    def test_operating_expenses(self) -> None:
        """Operating expenses = total - food."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        assert result["operating_expenses"] == 3000000

    def test_operating_cost_per_lb(self) -> None:
        """Op cost/lb = operating expenses / total lbs."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        expected = 3000000 / 5000000
        assert abs(result["operating_cost_per_lb"] - expected) < 0.001

    def test_waste_rate(self) -> None:
        """Waste rate = discarded / (received + beginning)."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        expected = 100000 / (5000000 + 1000000) * 100
        assert abs(result["waste_rate_pct"] - expected) < 0.01

    def test_purchase_cost_per_lb(self) -> None:
        """Purchase cost/lb = purchase value / purchase lbs."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        expected = 300000 / 600000
        assert abs(result["purchase_cost_per_lb"] - expected) < 0.001

    def test_lbs_per_employee(self) -> None:
        """Lbs per employee = total lbs / employees."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        expected = 5000000 / 200
        assert abs(result["lbs_per_employee"] - expected) < 0.01

    def test_labor_cost_from_detail(self) -> None:
        """Labor cost sums expense detail lines 5, 7, 8, 9, 10."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        assert result["total_labor_cost"] == 1700000  # 1500000 + 200000

    def test_program_service_ratio(self) -> None:
        """Program ratio = program expense / total expense × 100."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        expected = 9200000 / 10000000 * 100
        assert abs(result["program_service_ratio_pct"] - expected) < 0.01

    def test_channel_breakdowns(self) -> None:
        """Channel breakdowns use absolute values."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        assert result["donated_lbs_tefap"] == 2000000
        assert result["donated_lbs_csfp"] == 400000
        assert result["donated_lbs_oh_food"] == 900000
        assert result["donated_lbs_industry"] == 1200000

    def test_purchased_lbs(self) -> None:
        """Purchased lbs = abs(purchased distributed)."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        assert result["purchased_lbs"] == 500000

    def test_all_fieldnames_present(self) -> None:
        """Result has all expected keys."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, _EXP_ROWS)
        for key in FIELDNAMES:
            assert key in result, f"Missing key: {key}"


class TestComputeMetricsNoFinancials:
    """Tests when 990 data is not available."""

    def test_missing_financials(self) -> None:
        """Empty strings for missing 990 fields."""
        result = compute_metrics(_AUDIT_ROW, None, None)
        assert result["total_expenses"] == ""
        assert result["food_expense"] == ""
        assert result["operating_expenses"] == ""
        assert result["employees"] == ""

    def test_food_volumes_still_computed(self) -> None:
        """Food volumes work without 990 data."""
        result = compute_metrics(_AUDIT_ROW, None, None)
        assert result["total_lbs_distributed"] == 5000000
        assert result["waste_rate_pct"] != ""

    def test_labor_fallback_to_salaries(self) -> None:
        """Labor cost falls back to salaries_and_compensation when no detail."""
        result = compute_metrics(_AUDIT_ROW, _FIN_ROW, None)
        assert result["total_labor_cost"] == 2000000  # from financials row


class TestFieldnames:
    """Tests for FIELDNAMES constant."""

    def test_no_duplicates(self) -> None:
        """No duplicate field names."""
        assert len(FIELDNAMES) == len(set(FIELDNAMES))

    def test_form_year_first(self) -> None:
        """form_year is first column."""
        assert FIELDNAMES[0] == "form_year"


class TestMainNoCsv:
    """Tests for main() when CSVs are missing."""

    def test_no_audit_csv_exits(self, tmp_path: Path) -> None:
        """main() raises if audit CSV is missing."""
        import mofc_financials.data_extraction.compute_efficiency as mod

        orig = mod.Path
        # Patch data_dir to empty temp dir
        import unittest.mock

        with unittest.mock.patch.object(
            mod.Path,
            "resolve",
            return_value=tmp_path / "src" / "mofc_financials" / "data_extraction" / "compute.py",
        ):
            with pytest.raises(FileNotFoundError):
                main()
