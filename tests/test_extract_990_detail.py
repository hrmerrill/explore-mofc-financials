"""Tests for mofc_financials.data_extraction.extract_990_detail.

These tests exercise the line-processing and extraction logic without
requiring Tesseract or PDF files.  OCR-dependent functions are mocked.
"""

from unittest.mock import MagicMock, patch

import pytest

from mofc_financials.data_extraction.extract_990_detail import (
    CONTRIBUTION_COL_RANGE,
    CONTRIBUTION_LINE_DEFS,
    EXPENSE_COL_BOUNDS,
    EXPENSE_LINE_DEFS,
    REVENUE_COL_BOUNDS,
    REVENUE_LINE_DEFS,
    WordInfo,
    _extract_dynamic_label,
    _match_patterns,
    _rows_to_csv_dicts,
    _write_csv,
    clean_number,
    cluster_into_lines,
    extract_column_values,
    extract_expense_detail,
    extract_revenue_detail,
    find_section_page,
    is_financial_number,
    main,
    ocr_page_with_positions,
)

# =========================================================================
# clean_number — OCR number cleaning
# =========================================================================


class TestCleanNumber:
    """Tests for OCR number string cleaning."""

    def test_comma_separated(self) -> None:
        assert clean_number("1,864,848") == "1864848"

    def test_pipe_prefix(self) -> None:
        assert clean_number("|120,429,685") == "120429685"

    def test_pipe_suffix(self) -> None:
        assert clean_number("3,466,879|") == "3466879"

    def test_parenthesised_negative(self) -> None:
        assert clean_number("(2,272,381)") == "-2272381"

    def test_period_separator(self) -> None:
        assert clean_number("3.590.251") == "3590251"

    def test_plain_digits(self) -> None:
        assert clean_number("87842") == "87842"

    def test_non_number(self) -> None:
        assert clean_number("abc") == ""

    def test_empty_string(self) -> None:
        assert clean_number("") == ""

    def test_spaces_in_number(self) -> None:
        assert clean_number("1 234 567") == "1234567"


# =========================================================================
# is_financial_number — column-area check
# =========================================================================


class TestIsFinancialNumber:
    """Tests for financial number detection with x-position filtering."""

    def test_number_in_column_area(self) -> None:
        word: WordInfo = {
            "text": "1,864,848",
            "left": 580,
            "top": 400,
            "width": 80,
            "height": 20,
            "conf": 90,
        }
        assert is_financial_number(word, x_min=500) is True

    def test_number_outside_column_area(self) -> None:
        word: WordInfo = {
            "text": "990",
            "left": 100,
            "top": 45,
            "width": 30,
            "height": 15,
            "conf": 95,
        }
        assert is_financial_number(word, x_min=400) is False

    def test_text_not_a_number(self) -> None:
        word: WordInfo = {
            "text": "expenses",
            "left": 600,
            "top": 300,
            "width": 70,
            "height": 18,
            "conf": 88,
        }
        assert is_financial_number(word, x_min=400) is False


# =========================================================================
# cluster_into_lines — y-coordinate grouping
# =========================================================================


class TestClusterIntoLines:
    """Tests for word-to-line clustering."""

    def test_single_line(self) -> None:
        words: list[WordInfo] = [
            {"text": "hello", "left": 10, "top": 100, "width": 40, "height": 15, "conf": 90},
            {"text": "world", "left": 60, "top": 102, "width": 45, "height": 15, "conf": 90},
        ]
        lines = cluster_into_lines(words, y_tolerance=12)
        assert len(lines) == 1
        assert lines[0][0]["text"] == "hello"
        assert lines[0][1]["text"] == "world"

    def test_two_lines(self) -> None:
        words: list[WordInfo] = [
            {"text": "line1", "left": 10, "top": 100, "width": 40, "height": 15, "conf": 90},
            {"text": "line2", "left": 10, "top": 130, "width": 40, "height": 15, "conf": 90},
        ]
        lines = cluster_into_lines(words, y_tolerance=12)
        assert len(lines) == 2

    def test_empty_input(self) -> None:
        assert cluster_into_lines([]) == []

    def test_words_sorted_left_to_right(self) -> None:
        words: list[WordInfo] = [
            {"text": "B", "left": 200, "top": 100, "width": 10, "height": 15, "conf": 90},
            {"text": "A", "left": 50, "top": 102, "width": 10, "height": 15, "conf": 90},
        ]
        lines = cluster_into_lines(words, y_tolerance=12)
        assert lines[0][0]["text"] == "A"
        assert lines[0][1]["text"] == "B"


# =========================================================================
# extract_column_values — x-position to column mapping
# =========================================================================


class TestExtractColumnValues:
    """Tests for mapping numbers to named columns by x-position."""

    def test_all_four_expense_columns(self) -> None:
        words: list[WordInfo] = [
            {"text": "1,864,848", "left": 580, "top": 400, "width": 80, "height": 20, "conf": 90},
            {"text": "228,833", "left": 760, "top": 400, "width": 60, "height": 20, "conf": 90},
            {"text": "1,521,598", "left": 900, "top": 400, "width": 80, "height": 20, "conf": 90},
            {"text": "114,417", "left": 1080, "top": 400, "width": 60, "height": 20, "conf": 90},
        ]
        result = extract_column_values(words, EXPENSE_COL_BOUNDS)
        assert result == {
            "col_a": "1864848",
            "col_b": "228833",
            "col_c": "1521598",
            "col_d": "114417",
        }

    def test_two_columns_a_and_d(self) -> None:
        """Professional fundraising: total + fundraising only."""
        words: list[WordInfo] = [
            {"text": "1,325,809", "left": 580, "top": 750, "width": 80, "height": 20, "conf": 90},
            {"text": "1,325,809", "left": 1055, "top": 750, "width": 80, "height": 20, "conf": 90},
        ]
        result = extract_column_values(words, EXPENSE_COL_BOUNDS)
        assert result == {"col_a": "1325809", "col_d": "1325809"}

    def test_no_numbers(self) -> None:
        words: list[WordInfo] = [
            {"text": "Travel", "left": 50, "top": 300, "width": 40, "height": 15, "conf": 90},
        ]
        result = extract_column_values(words, EXPENSE_COL_BOUNDS)
        assert result == {}

    def test_number_outside_all_bounds(self) -> None:
        words: list[WordInfo] = [
            {"text": "990", "left": 100, "top": 45, "width": 30, "height": 15, "conf": 95},
        ]
        result = extract_column_values(words, EXPENSE_COL_BOUNDS)
        assert result == {}

    def test_revenue_columns(self) -> None:
        words: list[WordInfo] = [
            {
                "text": "133,634,324",
                "left": 644,
                "top": 1496,
                "width": 100,
                "height": 20,
                "conf": 90,
            },
            {
                "text": "6,894,976",
                "left": 796,
                "top": 1495,
                "width": 80,
                "height": 20,
                "conf": 90,
            },
        ]
        result = extract_column_values(words, REVENUE_COL_BOUNDS)
        assert result == {"col_a": "133634324", "col_b": "6894976"}


# =========================================================================
# _match_patterns — pattern matching
# =========================================================================


class TestMatchPatterns:
    """Tests for line-item pattern matching."""

    def test_expense_match(self) -> None:
        result = _match_patterns("Other salaries and wages", EXPENSE_LINE_DEFS)
        assert result is not None
        assert result[0] == "7"

    def test_expense_match_ocr_variant(self) -> None:
        result = _match_patterns("Othersalaries and wages", EXPENSE_LINE_DEFS)
        assert result is not None
        assert result[0] == "7"

    def test_no_match(self) -> None:
        result = _match_patterns("Something random", EXPENSE_LINE_DEFS)
        assert result is None

    def test_contribution_match(self) -> None:
        result = _match_patterns("Government grants (contributions)", CONTRIBUTION_LINE_DEFS)
        assert result is not None
        assert result[0] == "1e"

    def test_revenue_match(self) -> None:
        result = _match_patterns("Investment income (including dividends)", REVENUE_LINE_DEFS)
        assert result is not None
        assert result[0] == "3"

    def test_multiline_label_match(self) -> None:
        """Labels spanning multiple OCR lines are concatenated for matching."""
        label = "5 Compensation of current officers, directors, " "trustees, and key employees"
        result = _match_patterns(label, EXPENSE_LINE_DEFS)
        assert result is not None
        assert result[0] == "5"


# =========================================================================
# _extract_dynamic_label — clean OCR labels
# =========================================================================


class TestExtractDynamicLabel:
    """Tests for dynamic label extraction from OCR text."""

    def test_simple_label(self) -> None:
        assert _extract_dynamic_label("a Food") == "Food"

    def test_label_with_artifacts(self) -> None:
        result = _extract_dynamic_label("b Service Delivery")
        assert "Service Delivery" in result

    def test_label_with_business_code(self) -> None:
        result = _extract_dynamic_label("2a Shared Maintenance Fees boooss")
        assert "Shared Maintenance Fees" in result

    def test_empty_label(self) -> None:
        assert _extract_dynamic_label("   ") == "Unknown"


# =========================================================================
# _rows_to_csv_dicts — output formatting
# =========================================================================


class TestRowsToCsvDicts:
    """Tests for internal-to-CSV format conversion."""

    def test_basic_conversion(self) -> None:
        col_map = {
            "col_a": "total",
            "col_b": "program_service",
            "col_c": "management_and_general",
            "col_d": "fundraising",
        }
        fields = [
            "form_year",
            "line_number",
            "label",
            "total",
            "program_service",
            "management_and_general",
            "fundraising",
        ]
        rows = [
            {
                "line_number": "7",
                "label": "Other salaries and wages",
                "col_a": "10438376",
                "col_b": "7526337",
                "col_c": "1774146",
                "col_d": "1137893",
            }
        ]
        result = _rows_to_csv_dicts(rows, "2023", col_map, fields)
        assert len(result) == 1
        assert result[0]["form_year"] == "2023"
        assert result[0]["total"] == "10438376"
        assert result[0]["program_service"] == "7526337"
        assert result[0]["management_and_general"] == "1774146"
        assert result[0]["fundraising"] == "1137893"

    def test_missing_columns_are_empty(self) -> None:
        col_map = {"col_a": "total", "col_b": "col_b", "col_c": "col_c", "col_d": "col_d"}
        fields = ["form_year", "line_number", "label", "total", "col_b", "col_c", "col_d"]
        rows = [{"line_number": "1c", "label": "Fundraising events", "col_a": "87842"}]
        result = _rows_to_csv_dicts(rows, "2023", col_map, fields)
        assert result[0]["total"] == "87842"
        assert result[0]["col_b"] == ""
        assert result[0]["col_c"] == ""


# =========================================================================
# find_section_page — page locator (mocked)
# =========================================================================


class TestFindSectionPage:
    """Tests for section page finding with mocked OCR."""

    @patch("mofc_financials.data_extraction.extract_990_detail.pytesseract")
    @patch("mofc_financials.data_extraction.extract_990_detail.Image")
    @patch("mofc_financials.data_extraction.extract_990_detail.io")
    @patch("mofc_financials.data_extraction.extract_990_detail.fitz")
    def test_finds_revenue_page(
        self, mock_fitz: MagicMock, mock_io: MagicMock, mock_image: MagicMock, mock_tess: MagicMock
    ) -> None:
        mock_doc = MagicMock()
        mock_doc.__len__ = lambda self: 15
        mock_fitz.open.return_value = mock_doc
        mock_fitz.Matrix.return_value = "matrix"
        mock_io.BytesIO.return_value = "bytes_buf"
        mock_image.open.return_value = "img"
        mock_tess.image_to_string.side_effect = [
            "Page 0 content",
            "Statement of Revenue here",
        ]

        result = find_section_page("fake.pdf", "statement of revenue")
        assert result == 1

    @patch("mofc_financials.data_extraction.extract_990_detail.pytesseract")
    @patch("mofc_financials.data_extraction.extract_990_detail.Image")
    @patch("mofc_financials.data_extraction.extract_990_detail.io")
    @patch("mofc_financials.data_extraction.extract_990_detail.fitz")
    def test_returns_none_when_not_found(
        self, mock_fitz: MagicMock, mock_io: MagicMock, mock_image: MagicMock, mock_tess: MagicMock
    ) -> None:
        mock_doc = MagicMock()
        mock_doc.__len__ = lambda self: 3
        mock_fitz.open.return_value = mock_doc
        mock_fitz.Matrix.return_value = "matrix"
        mock_io.BytesIO.return_value = "bytes_buf"
        mock_image.open.return_value = "img"
        mock_tess.image_to_string.return_value = "No match here"

        result = find_section_page("fake.pdf", "statement of revenue")
        assert result is None


# =========================================================================
# extract_expense_detail — full expense extraction (mocked OCR)
# =========================================================================

# Simulated word positions mimicking a Part IX page
_MOCK_EXPENSE_WORDS: list[WordInfo] = [
    # Line 7: Other salaries and wages — all 4 columns
    {"text": "7", "left": 30, "top": 507, "width": 10, "height": 15, "conf": 95},
    {"text": "Other", "left": 50, "top": 507, "width": 40, "height": 15, "conf": 92},
    {"text": "salaries", "left": 100, "top": 507, "width": 55, "height": 15, "conf": 90},
    {"text": "and", "left": 165, "top": 507, "width": 25, "height": 15, "conf": 93},
    {"text": "wages", "left": 200, "top": 507, "width": 40, "height": 15, "conf": 91},
    {"text": "10,438,376", "left": 567, "top": 507, "width": 85, "height": 18, "conf": 88},
    {"text": "7,526,337", "left": 738, "top": 507, "width": 75, "height": 18, "conf": 89},
    {"text": "1,774,146", "left": 896, "top": 507, "width": 75, "height": 18, "conf": 90},
    {"text": "1,137,893", "left": 1054, "top": 507, "width": 75, "height": 18, "conf": 87},
    # Line 11c: Accounting — 2 columns (A and C)
    {"text": "Accounting", "left": 80, "top": 704, "width": 80, "height": 15, "conf": 91},
    {"text": "90,416", "left": 612, "top": 704, "width": 50, "height": 18, "conf": 92},
    {"text": "90,416", "left": 930, "top": 704, "width": 50, "height": 18, "conf": 91},
    # Line 11e: Professional fundraising — 2 columns (A and D)
    {"text": "Professional", "left": 50, "top": 752, "width": 85, "height": 15, "conf": 90},
    {"text": "fundraising", "left": 140, "top": 752, "width": 80, "height": 15, "conf": 88},
    {"text": "services.", "left": 225, "top": 752, "width": 60, "height": 15, "conf": 89},
    {"text": "1,325,809", "left": 579, "top": 752, "width": 75, "height": 18, "conf": 90},
    {"text": "1,325,809", "left": 1054, "top": 752, "width": 75, "height": 18, "conf": 89},
    # Line 16: Occupancy — all 4 columns
    {"text": "16", "left": 30, "top": 944, "width": 15, "height": 15, "conf": 95},
    {"text": "Occupancy", "left": 55, "top": 944, "width": 70, "height": 15, "conf": 92},
    {"text": "2,060,149", "left": 578, "top": 944, "width": 75, "height": 18, "conf": 90},
    {"text": "1,950,454", "left": 738, "top": 944, "width": 80, "height": 18, "conf": 89},
    {"text": "98,726", "left": 931, "top": 944, "width": 50, "height": 18, "conf": 91},
    {"text": "10,969", "left": 1088, "top": 944, "width": 50, "height": 18, "conf": 90},
    # Line 22: Depreciation — all 4 columns
    {"text": "22", "left": 30, "top": 1112, "width": 15, "height": 15, "conf": 95},
    {"text": "Depreciation,", "left": 55, "top": 1112, "width": 90, "height": 15, "conf": 88},
    {"text": "depletion,", "left": 155, "top": 1112, "width": 70, "height": 15, "conf": 87},
    {"text": "and", "left": 235, "top": 1112, "width": 25, "height": 15, "conf": 93},
    {"text": "amortization", "left": 270, "top": 1112, "width": 90, "height": 15, "conf": 89},
    {"text": "1,447,173", "left": 579, "top": 1112, "width": 75, "height": 18, "conf": 90},
    {"text": "1,383,103", "left": 738, "top": 1112, "width": 80, "height": 18, "conf": 88},
    {"text": "42,903", "left": 930, "top": 1112, "width": 50, "height": 18, "conf": 91},
    {"text": "21,167", "left": 1088, "top": 1112, "width": 50, "height": 18, "conf": 90},
    # Line 23: Insurance — 2 columns (A and B)
    {"text": "23", "left": 30, "top": 1137, "width": 15, "height": 15, "conf": 95},
    {"text": "Insurance", "left": 55, "top": 1137, "width": 65, "height": 15, "conf": 92},
    {"text": "71,236", "left": 612, "top": 1137, "width": 50, "height": 18, "conf": 91},
    {"text": "71,236", "left": 772, "top": 1137, "width": 50, "height": 18, "conf": 90},
    # 24 Other expenses header (no numbers)
    {"text": "24", "left": 30, "top": 1180, "width": 15, "height": 15, "conf": 95},
    {"text": "Other", "left": 55, "top": 1180, "width": 40, "height": 15, "conf": 90},
    {"text": "expenses.", "left": 105, "top": 1180, "width": 60, "height": 15, "conf": 88},
    {"text": "Itemize", "left": 175, "top": 1180, "width": 50, "height": 15, "conf": 87},
    {"text": "expenses", "left": 235, "top": 1180, "width": 55, "height": 15, "conf": 89},
    {"text": "not", "left": 300, "top": 1180, "width": 20, "height": 15, "conf": 93},
    {"text": "covered", "left": 330, "top": 1180, "width": 50, "height": 15, "conf": 90},
    # 24a: Food — 2 columns (A and B)
    {"text": "a", "left": 30, "top": 1257, "width": 8, "height": 15, "conf": 88},
    {"text": "Food", "left": 50, "top": 1257, "width": 30, "height": 15, "conf": 95},
    {"text": "107,214,002", "left": 557, "top": 1257, "width": 95, "height": 18, "conf": 90},
    {"text": "107,214,002", "left": 716, "top": 1257, "width": 95, "height": 18, "conf": 89},
    # 24b: Service Delivery — 2 columns (A and B)
    {"text": "b", "left": 30, "top": 1281, "width": 8, "height": 15, "conf": 90},
    {"text": "Service", "left": 50, "top": 1281, "width": 50, "height": 15, "conf": 91},
    {"text": "Delivery", "left": 110, "top": 1281, "width": 55, "height": 15, "conf": 90},
    {"text": "1,709,416", "left": 579, "top": 1281, "width": 75, "height": 18, "conf": 89},
    {"text": "1,709,416", "left": 738, "top": 1281, "width": 75, "height": 18, "conf": 88},
    # 25: Total functional expenses — all 4 columns
    {"text": "25", "left": 30, "top": 1377, "width": 15, "height": 15, "conf": 95},
    {"text": "Total", "left": 55, "top": 1377, "width": 35, "height": 15, "conf": 93},
    {"text": "functional", "left": 100, "top": 1377, "width": 70, "height": 15, "conf": 91},
    {"text": "expenses.", "left": 180, "top": 1377, "width": 60, "height": 15, "conf": 90},
    {"text": "134,475,329", "left": 558, "top": 1377, "width": 95, "height": 18, "conf": 90},
    {"text": "124,038,575", "left": 716, "top": 1377, "width": 95, "height": 18, "conf": 89},
    {"text": "6,846,503", "left": 897, "top": 1377, "width": 75, "height": 18, "conf": 91},
    {"text": "3,590,251", "left": 1055, "top": 1377, "width": 75, "height": 18, "conf": 88},
]


class TestExtractExpenseDetail:
    """Tests for full expense extraction with mocked OCR."""

    @patch("mofc_financials.data_extraction.extract_990_detail.ocr_page_with_positions")
    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_extracts_expense_line_items(self, mock_find: MagicMock, mock_ocr: MagicMock) -> None:
        mock_find.return_value = 10
        mock_ocr.return_value = _MOCK_EXPENSE_WORDS

        result = extract_expense_detail("fake.pdf")

        # Should find multiple line items
        assert len(result) >= 5
        line_nums = [r["line_number"] for r in result]
        assert "7" in line_nums
        assert "11c" in line_nums
        assert "11e" in line_nums
        assert "16" in line_nums
        assert "25" in line_nums

    @patch("mofc_financials.data_extraction.extract_990_detail.ocr_page_with_positions")
    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_other_salaries_all_columns(self, mock_find: MagicMock, mock_ocr: MagicMock) -> None:
        mock_find.return_value = 10
        mock_ocr.return_value = _MOCK_EXPENSE_WORDS

        result = extract_expense_detail("fake.pdf")
        salaries = next(r for r in result if r["line_number"] == "7")
        assert salaries["col_a"] == "10438376"
        assert salaries["col_b"] == "7526337"
        assert salaries["col_c"] == "1774146"
        assert salaries["col_d"] == "1137893"

    @patch("mofc_financials.data_extraction.extract_990_detail.ocr_page_with_positions")
    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_accounting_two_columns(self, mock_find: MagicMock, mock_ocr: MagicMock) -> None:
        mock_find.return_value = 10
        mock_ocr.return_value = _MOCK_EXPENSE_WORDS

        result = extract_expense_detail("fake.pdf")
        accounting = next(r for r in result if r["line_number"] == "11c")
        assert accounting["col_a"] == "90416"
        assert accounting.get("col_c") == "90416"

    @patch("mofc_financials.data_extraction.extract_990_detail.ocr_page_with_positions")
    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_professional_fundraising_a_and_d(
        self, mock_find: MagicMock, mock_ocr: MagicMock
    ) -> None:
        mock_find.return_value = 10
        mock_ocr.return_value = _MOCK_EXPENSE_WORDS

        result = extract_expense_detail("fake.pdf")
        fund = next(r for r in result if r["line_number"] == "11e")
        assert fund["col_a"] == "1325809"
        assert fund.get("col_d") == "1325809"

    @patch("mofc_financials.data_extraction.extract_990_detail.ocr_page_with_positions")
    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_dynamic_other_expenses(self, mock_find: MagicMock, mock_ocr: MagicMock) -> None:
        """24a-24e items should be detected as dynamic line items."""
        mock_find.return_value = 10
        mock_ocr.return_value = _MOCK_EXPENSE_WORDS

        result = extract_expense_detail("fake.pdf")
        line_nums = [r["line_number"] for r in result]
        assert "24a" in line_nums
        assert "24b" in line_nums

        food = next(r for r in result if r["line_number"] == "24a")
        assert food["col_a"] == "107214002"
        assert food["col_b"] == "107214002"

    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_returns_empty_when_page_not_found(self, mock_find: MagicMock) -> None:
        mock_find.return_value = None
        result = extract_expense_detail("fake.pdf")
        assert result == []


# =========================================================================
# extract_revenue_detail — full revenue extraction (mocked OCR)
# =========================================================================

_MOCK_REVENUE_WORDS: list[WordInfo] = [
    # 1c: Fundraising events — contribution column only
    {"text": "Fundraising", "left": 80, "top": 244, "width": 85, "height": 15, "conf": 88},
    {"text": "events", "left": 175, "top": 244, "width": 45, "height": 15, "conf": 90},
    {"text": "87,842", "left": 569, "top": 244, "width": 50, "height": 18, "conf": 91},
    # 1e: Government grants — contribution column
    {"text": "Government", "left": 80, "top": 287, "width": 80, "height": 15, "conf": 89},
    {"text": "grants", "left": 170, "top": 287, "width": 45, "height": 15, "conf": 90},
    {"text": "6,221,821", "left": 536, "top": 287, "width": 70, "height": 18, "conf": 88},
    # 1h: Total contributions — column A
    {"text": "Total.", "left": 80, "top": 404, "width": 35, "height": 15, "conf": 92},
    {"text": "Add", "left": 125, "top": 404, "width": 25, "height": 15, "conf": 93},
    {"text": "lines", "left": 160, "top": 404, "width": 35, "height": 15, "conf": 91},
    {"text": "1a", "left": 205, "top": 404, "width": 15, "height": 15, "conf": 88},
    {"text": "126,739,348", "left": 643, "top": 404, "width": 95, "height": 18, "conf": 90},
    # 2a: Shared Maintenance Fees — columns A and B
    {"text": "2a", "left": 30, "top": 457, "width": 15, "height": 15, "conf": 95},
    {"text": "Shared", "left": 55, "top": 457, "width": 45, "height": 15, "conf": 90},
    {"text": "Maintenance", "left": 110, "top": 457, "width": 80, "height": 15, "conf": 88},
    {"text": "Fees", "left": 200, "top": 457, "width": 30, "height": 15, "conf": 92},
    {"text": "3,466,879", "left": 665, "top": 457, "width": 75, "height": 18, "conf": 89},
    {"text": "3,466,879", "left": 795, "top": 457, "width": 75, "height": 18, "conf": 88},
    # 2b: Program Earned Income — columns A and B
    {"text": "b", "left": 30, "top": 478, "width": 8, "height": 15, "conf": 90},
    {"text": "Program", "left": 55, "top": 478, "width": 55, "height": 15, "conf": 91},
    {"text": "Earned", "left": 120, "top": 478, "width": 45, "height": 15, "conf": 89},
    {"text": "Income", "left": 175, "top": 478, "width": 45, "height": 15, "conf": 90},
    {"text": "896,062", "left": 688, "top": 478, "width": 55, "height": 18, "conf": 90},
    {"text": "896,062", "left": 817, "top": 478, "width": 55, "height": 18, "conf": 89},
    # 2g: Total program service revenue — column A
    {"text": "Total.", "left": 50, "top": 606, "width": 35, "height": 15, "conf": 93},
    {"text": "Add", "left": 95, "top": 606, "width": 25, "height": 15, "conf": 92},
    {"text": "lines", "left": 130, "top": 606, "width": 35, "height": 15, "conf": 91},
    {"text": "2a-2f", "left": 175, "top": 606, "width": 35, "height": 15, "conf": 89},
    {"text": "program", "left": 220, "top": 606, "width": 50, "height": 15, "conf": 88},
    {"text": "service", "left": 280, "top": 606, "width": 45, "height": 15, "conf": 90},
    {"text": "4,362,941", "left": 665, "top": 606, "width": 75, "height": 18, "conf": 90},
    # 3: Investment income — columns A and B
    {"text": "Investment", "left": 50, "top": 650, "width": 70, "height": 15, "conf": 90},
    {"text": "income", "left": 130, "top": 650, "width": 45, "height": 15, "conf": 91},
    {"text": "2,263,725", "left": 665, "top": 654, "width": 75, "height": 18, "conf": 88},
    {"text": "2,263,725", "left": 794, "top": 654, "width": 75, "height": 18, "conf": 89},
    # 12: Total revenue — columns A and B
    {"text": "12", "left": 30, "top": 1491, "width": 15, "height": 15, "conf": 95},
    {"text": "Total", "left": 55, "top": 1491, "width": 35, "height": 15, "conf": 93},
    {"text": "revenue.", "left": 100, "top": 1491, "width": 55, "height": 15, "conf": 91},
    {"text": "133,634,324", "left": 644, "top": 1491, "width": 95, "height": 18, "conf": 90},
    {"text": "6,894,976", "left": 796, "top": 1491, "width": 75, "height": 18, "conf": 89},
]


class TestExtractRevenueDetail:
    """Tests for full revenue extraction with mocked OCR."""

    @patch("mofc_financials.data_extraction.extract_990_detail.ocr_page_with_positions")
    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_extracts_revenue_line_items(self, mock_find: MagicMock, mock_ocr: MagicMock) -> None:
        mock_find.return_value = 9
        mock_ocr.return_value = _MOCK_REVENUE_WORDS

        result = extract_revenue_detail("fake.pdf")

        line_nums = [r["line_number"] for r in result]
        assert "1c" in line_nums
        assert "1e" in line_nums
        assert "1h" in line_nums
        assert "3" in line_nums
        assert "12" in line_nums

    @patch("mofc_financials.data_extraction.extract_990_detail.ocr_page_with_positions")
    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_contribution_line_values(self, mock_find: MagicMock, mock_ocr: MagicMock) -> None:
        mock_find.return_value = 9
        mock_ocr.return_value = _MOCK_REVENUE_WORDS

        result = extract_revenue_detail("fake.pdf")
        govt_grants = next(r for r in result if r["line_number"] == "1e")
        assert govt_grants["col_a"] == "6221821"

    @patch("mofc_financials.data_extraction.extract_990_detail.ocr_page_with_positions")
    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_total_revenue_columns(self, mock_find: MagicMock, mock_ocr: MagicMock) -> None:
        mock_find.return_value = 9
        mock_ocr.return_value = _MOCK_REVENUE_WORDS

        result = extract_revenue_detail("fake.pdf")
        total = next(r for r in result if r["line_number"] == "12")
        assert total["col_a"] == "133634324"
        assert total.get("col_b") == "6894976"

    @patch("mofc_financials.data_extraction.extract_990_detail.ocr_page_with_positions")
    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_dynamic_program_service_revenue(
        self, mock_find: MagicMock, mock_ocr: MagicMock
    ) -> None:
        """Org-specific program service lines (2a, 2b) should be extracted."""
        mock_find.return_value = 9
        mock_ocr.return_value = _MOCK_REVENUE_WORDS

        result = extract_revenue_detail("fake.pdf")
        line_nums = [r["line_number"] for r in result]
        assert "2a" in line_nums
        assert "2b" in line_nums

    @patch("mofc_financials.data_extraction.extract_990_detail.find_section_page")
    def test_returns_empty_when_page_not_found(self, mock_find: MagicMock) -> None:
        mock_find.return_value = None
        result = extract_revenue_detail("fake.pdf")
        assert result == []


# =========================================================================
# Constant integrity checks
# =========================================================================


class TestConstants:
    """Verify pattern definitions are well-formed."""

    def test_expense_patterns_no_duplicates(self) -> None:
        line_nums = [ln for ln, _, _ in EXPENSE_LINE_DEFS]
        assert len(line_nums) == len(set(line_nums))

    def test_contribution_patterns_no_duplicates(self) -> None:
        line_nums = [ln for ln, _, _ in CONTRIBUTION_LINE_DEFS]
        assert len(line_nums) == len(set(line_nums))

    def test_revenue_patterns_no_duplicates(self) -> None:
        line_nums = [ln for ln, _, _ in REVENUE_LINE_DEFS]
        assert len(line_nums) == len(set(line_nums))

    def test_column_bounds_non_overlapping(self) -> None:
        for bounds in [EXPENSE_COL_BOUNDS, REVENUE_COL_BOUNDS]:
            for i in range(len(bounds) - 1):
                assert bounds[i][2] <= bounds[i + 1][1], f"Overlap: {bounds[i]} and {bounds[i + 1]}"


# =========================================================================
# _write_csv — CSV output
# =========================================================================


class TestWriteCsv:
    """Tests for CSV writing helper."""

    def test_writes_csv_file(self, tmp_path: MagicMock) -> None:
        out = tmp_path / "test.csv"
        _write_csv(out, ["a", "b"], [{"a": "1", "b": "2"}])
        content = out.read_text()
        assert "a,b" in content
        assert "1,2" in content


# =========================================================================
# main — CLI entry point (mocked)
# =========================================================================


class TestMain:
    """Tests for the detail extraction CLI entry point."""

    @patch("mofc_financials.data_extraction.extract_990_detail.Path")
    def test_main_exits_on_no_pdfs(self, mock_path_cls: MagicMock) -> None:
        mock_raw_dir = MagicMock()
        mock_raw_dir.glob.return_value = []
        mock_path_cls.return_value.resolve.return_value.parent.parent.parent.parent.__truediv__.return_value.__truediv__.return_value = (
            mock_raw_dir
        )

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
