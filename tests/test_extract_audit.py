"""Tests for mofc_financials.data_extraction.extract_audit.

These tests exercise audit PDF parsing logic using representative text
snippets — no real PDFs or OCR software required.
"""

from unittest.mock import MagicMock, patch

import pytest

from mofc_financials.data_extraction.extract_audit import (
    FIELDNAMES,
    _channel_key,
    _collect_numbers,
    _extract_channel_block,
    _find_inventory_section,
    _fiscal_year_from_filename,
    _restrict_to_primary_year,
    extract_audit_data,
    extract_fee_waived,
    extract_valuation_rate,
    main,
    parse_donated_food_table,
    parse_number,
    parse_purchased_food_table,
)

# =========================================================================
# parse_number — core number parser
# =========================================================================


class TestParseNumber:
    """Tests for number parsing with $, commas, and parenthetical negatives."""

    def test_plain_integer(self):
        assert parse_number("1785488") == 1785488

    def test_with_commas(self):
        assert parse_number("1,785,488") == 1785488

    def test_with_dollar_sign(self):
        assert parse_number("$ 1,934,300") == 1934300

    def test_parenthetical_negative(self):
        assert parse_number("(9,648,916)") == -9648916

    def test_parenthetical_with_spaces(self):
        assert parse_number(" (1,586,029) ") == -1586029

    def test_empty_string(self):
        assert parse_number("") is None

    def test_dash(self):
        assert parse_number("-") is None

    def test_whitespace_only(self):
        assert parse_number("   ") is None

    def test_dollar_sign_with_spaces(self):
        assert parse_number("$    3,574,557") == 3574557

    def test_zero(self):
        assert parse_number("0") == 0


# =========================================================================
# _collect_numbers — multi-line number collection
# =========================================================================


class TestCollectNumbers:
    """Tests for scanning forward to collect numbers across lines."""

    def test_numbers_on_separate_lines(self):
        lines = ["TEFAP", "  11,201,210", "  9,569,494"]
        assert _collect_numbers(lines, 0, max_scan=10) == [11201210, 9569494]

    def test_numbers_on_same_line(self):
        lines = ["     17,949,081       17,042,886"]
        assert _collect_numbers(lines, 0) == [17949081, 17042886]

    def test_skips_blank_lines(self):
        lines = ["Beginning Inventory", "", "", "1,785,488", "", "", "$ 1,934,300"]
        assert _collect_numbers(lines, 0, max_scan=10) == [1785488, 1934300]

    def test_stops_at_label_after_numbers(self):
        lines = ["  11,201,210", "  9,569,494", "CSFP", "1,888,522"]
        assert _collect_numbers(lines, 0) == [11201210, 9569494]

    def test_does_not_stop_at_label_before_numbers(self):
        lines = ["TEFAP", "  11,201,210", "  9,569,494"]
        assert _collect_numbers(lines, 0, max_scan=10) == [11201210, 9569494]

    def test_stop_patterns(self):
        lines = ["  11,201,210", "CSFP", "1,888,522"]
        nums = _collect_numbers(lines, 0, max_scan=10, stop_patterns=["csfp"])
        assert nums == [11201210]

    def test_max_scan_limits_search(self):
        lines = ["Label", "", "", "", "", "", "", "999"]
        assert _collect_numbers(lines, 0, max_scan=3) == []

    def test_parenthetical_negatives(self):
        lines = ["(9,648,916)", "(8,165,533)"]
        assert _collect_numbers(lines, 0) == [-9648916, -8165533]


# =========================================================================
# _channel_key — channel name normalization
# =========================================================================


class TestChannelKey:
    """Tests for channel name to CSV key normalization."""

    def test_tefap(self):
        assert _channel_key("TEFAP") == "tefap"

    def test_cfap(self):
        assert _channel_key("CFAP") == "cfap"

    def test_csfp(self):
        assert _channel_key("CSFP") == "csfp"

    def test_oh_food(self):
        assert _channel_key("OH Food Purchase Program") == "oh_food"

    def test_ohio_food(self):
        assert _channel_key("Ohio Food Purchase and Agricultural Clearance Program") == "oh_food"

    def test_industry(self):
        assert _channel_key("Industry Surplus") == "industry"


# =========================================================================
# parse_donated_food_table — FY2019 single-year format
# =========================================================================

# Simplified FY2019-style text with the large whitespace gaps
_FY2019_DONATED_TEXT = """
6/30/2019


Pounds

Dollar Value


Beginning Inventory


1,785,488


$ 1,934,300


Pounds received for the year



   TEFAP

11,201,210

9,569,494

   CSFP

1,888,522

1,574,352

   OH Food Purchase Program

15,098,153

3,565,212

   Industry Surplus

37,863,743

61,339,264

Pounds disbursed for the year



   TEFAP

(9,648,916)

(8,165,533)

   CSFP

(2,197,313)

(1,811,300)

   OH Food Purchase Program

 (14,615,551)

 (3,448,241)

   Industry Surplus

(36,536,202)

(59,222,692)


Pounds discarded – unusable food
(1,586,029)

(2,059,088)


Ending Inventory


3,253,105


$ 3,275,768
"""


class TestParseDonatedFoodFY2019:
    """Tests for parsing FY2019 single-year donated food table."""

    def test_beginning_inventory(self):
        result = parse_donated_food_table(_FY2019_DONATED_TEXT)
        assert result["donated_lbs_beginning_inv"] == 1785488
        assert result["donated_val_beginning_inv"] == 1934300

    def test_received_channels(self):
        result = parse_donated_food_table(_FY2019_DONATED_TEXT)
        assert result["donated_lbs_received_tefap"] == 11201210
        assert result["donated_val_received_tefap"] == 9569494
        assert result["donated_lbs_received_csfp"] == 1888522
        assert result["donated_val_received_csfp"] == 1574352
        assert result["donated_lbs_received_oh_food"] == 15098153
        assert result["donated_val_received_oh_food"] == 3565212
        assert result["donated_lbs_received_industry"] == 37863743
        assert result["donated_val_received_industry"] == 61339264

    def test_received_total_summed(self):
        """FY2019 has no Total row — totals must be computed."""
        result = parse_donated_food_table(_FY2019_DONATED_TEXT)
        expected_lbs = 11201210 + 1888522 + 15098153 + 37863743
        expected_val = 9569494 + 1574352 + 3565212 + 61339264
        assert result["donated_lbs_received_total"] == expected_lbs
        assert result["donated_val_received_total"] == expected_val

    def test_disbursed_channels(self):
        result = parse_donated_food_table(_FY2019_DONATED_TEXT)
        assert result["donated_lbs_disbursed_tefap"] == -9648916
        assert result["donated_val_disbursed_tefap"] == -8165533

    def test_disbursed_total_summed(self):
        result = parse_donated_food_table(_FY2019_DONATED_TEXT)
        expected = -9648916 + -2197313 + -14615551 + -36536202
        assert result["donated_lbs_disbursed_total"] == expected

    def test_discarded(self):
        result = parse_donated_food_table(_FY2019_DONATED_TEXT)
        assert result["donated_lbs_discarded"] == -1586029
        assert result["donated_val_discarded"] == -2059088

    def test_ending_inventory(self):
        result = parse_donated_food_table(_FY2019_DONATED_TEXT)
        assert result["donated_lbs_ending_inv"] == 3253105
        assert result["donated_val_ending_inv"] == 3275768

    def test_inventory_balance(self):
        """begin + received - disbursed - discarded = ending (pounds)."""
        r = parse_donated_food_table(_FY2019_DONATED_TEXT)
        computed = (
            r["donated_lbs_beginning_inv"]
            + r["donated_lbs_received_total"]
            + r["donated_lbs_disbursed_total"]  # already negative
            + r["donated_lbs_discarded"]  # already negative
        )
        assert computed == r["donated_lbs_ending_inv"]


# =========================================================================
# parse_donated_food_table — FY2021+ format with CFAP and Total rows
# =========================================================================

_FY2021_DONATED_TEXT = """6/30/2021
Pounds
Dollar Value
Beginning Inventory
       3,138,433
$    3,574,557
Pounds received for the year
TEFAP
     17,949,081
    17,042,886
CFAP
       6,105,566
    10,379,464
CSFP
       1,688,887
      1,849,037
OH Food Purchase Program
     10,165,025
      4,494,670
Industry Surplus
     29,781,752
    53,309,336
Total - Pounds received for the year
      65,690,311
     87,075,393
Pounds disbursed for the year
TEFAP
   (16,620,973)
  (16,269,874)
CFAP
     (6,153,307)
  (10,424,883)
CSFP
     (2,042,446)
    (1,939,994)
OH Food Purchase Program
    (10,330,516)
     (4,490,972)
Industry Surplus
   (28,394,321)
  (50,769,667)
Total - Pounds disbursed for the year
   (63,541,563)
  (83,895,390)
Pounds discarded - unusable food
     (1,824,729)
    (3,266,265)
Ending Inventory
       3,462,452
$    3,488,295
"""


class TestParseDonatedFoodFY2021:
    """Tests for parsing FY2021+ format with CFAP and Total rows."""

    def test_cfap_present(self):
        result = parse_donated_food_table(_FY2021_DONATED_TEXT)
        assert result["donated_lbs_received_cfap"] == 6105566
        assert result["donated_val_received_cfap"] == 10379464

    def test_explicit_total_used(self):
        result = parse_donated_food_table(_FY2021_DONATED_TEXT)
        assert result["donated_lbs_received_total"] == 65690311
        assert result["donated_val_received_total"] == 87075393

    def test_disbursed_total(self):
        result = parse_donated_food_table(_FY2021_DONATED_TEXT)
        assert result["donated_lbs_disbursed_total"] == -63541563
        assert result["donated_val_disbursed_total"] == -83895390

    def test_cfap_disbursed(self):
        result = parse_donated_food_table(_FY2021_DONATED_TEXT)
        assert result["donated_lbs_disbursed_cfap"] == -6153307
        assert result["donated_val_disbursed_cfap"] == -10424883

    def test_inventory_balance(self):
        r = parse_donated_food_table(_FY2021_DONATED_TEXT)
        computed = (
            r["donated_lbs_beginning_inv"]
            + r["donated_lbs_received_total"]
            + r["donated_lbs_disbursed_total"]
            + r["donated_lbs_discarded"]
        )
        assert computed == r["donated_lbs_ending_inv"]


# =========================================================================
# parse_purchased_food_table
# =========================================================================

_PURCHASED_FOOD_TEXT = """Purchased Food
In addition to donated food, the Organization also maintains an inventory of purchased food as
follows:
6/30/2019
Pounds
Dollar Value
Beginning Inventory
592,001
$ 318,217
Purchases
3,163,321
3,512,433
Food Distributed
(3,154,333)
(3,479,880)
Ending Inventory
 600,989
$ 350,770
TOTAL INVENTORY
$3,626,538
"""


class TestParsePurchasedFood:
    """Tests for the purchased food inventory table."""

    def test_beginning_inventory(self):
        result = parse_purchased_food_table(_PURCHASED_FOOD_TEXT)
        assert result["purchased_lbs_beginning_inv"] == 592001
        assert result["purchased_val_beginning_inv"] == 318217

    def test_purchases(self):
        result = parse_purchased_food_table(_PURCHASED_FOOD_TEXT)
        assert result["purchased_lbs_purchases"] == 3163321
        assert result["purchased_val_purchases"] == 3512433

    def test_distributed(self):
        result = parse_purchased_food_table(_PURCHASED_FOOD_TEXT)
        assert result["purchased_lbs_distributed"] == -3154333
        assert result["purchased_val_distributed"] == -3479880

    def test_ending_inventory(self):
        result = parse_purchased_food_table(_PURCHASED_FOOD_TEXT)
        assert result["purchased_lbs_ending_inv"] == 600989
        assert result["purchased_val_ending_inv"] == 350770

    def test_inventory_balance(self):
        r = parse_purchased_food_table(_PURCHASED_FOOD_TEXT)
        computed = (
            r["purchased_lbs_beginning_inv"]
            + r["purchased_lbs_purchases"]
            + r["purchased_lbs_distributed"]
        )
        assert computed == r["purchased_lbs_ending_inv"]


# Two-year purchased food (should only take first year)
_PURCHASED_FOOD_TWO_YEAR = """Purchased Food
follows:
June 30, 2023
Pounds
Dollar Value
Beginning Inventory
      606,048
$   572,944
Purchases
      25,426,661
13,314,299
Food Distributed
  (25,252,327)
(13,120,674)
Ending Inventory - Purchased
        780,382
$    766,569

June 30, 2022
Pounds
Dollar Value
Beginning Inventory
    1,203,113
$      1,184,434
Purchases
   18,655,320
        8,552,979
Food Distributed
 (19,252,385)
       (9,164,469)
Ending Inventory - Purchased
       606,048
$         572,944
"""


class TestPurchasedFoodTwoYear:
    """Tests that two-year purchased food takes only the first year."""

    def test_beginning_is_primary_year(self):
        result = parse_purchased_food_table(_PURCHASED_FOOD_TWO_YEAR)
        assert result["purchased_lbs_beginning_inv"] == 606048

    def test_ending_is_primary_year(self):
        result = parse_purchased_food_table(_PURCHASED_FOOD_TWO_YEAR)
        assert result["purchased_lbs_ending_inv"] == 780382

    def test_does_not_overwrite_with_prior_year(self):
        result = parse_purchased_food_table(_PURCHASED_FOOD_TWO_YEAR)
        # Prior year beginning = 1,203,113 — should NOT appear
        assert result["purchased_lbs_beginning_inv"] != 1203113


# =========================================================================
# extract_valuation_rate
# =========================================================================


class TestExtractValuationRate:
    """Tests for valuation rate extraction."""

    def test_standard_format(self):
        text = "The estimated value of donated food is $1.62 per pound for 2019"
        assert extract_valuation_rate(text) == 1.62

    def test_with_spaces(self):
        text = "estimated value of donated food is $ 1.97 per pound for 2024"
        assert extract_valuation_rate(text) == 1.97

    def test_not_found(self):
        assert extract_valuation_rate("no rate here") is None


# =========================================================================
# extract_fee_waived
# =========================================================================


class TestExtractFeeWaived:
    """Tests for Shared Maintenance Fee waiver detection."""

    def test_waived(self):
        text = "Shared Maintenance Fees were waived the second half of Fiscal Year 2021."
        assert extract_fee_waived(text) is True

    def test_not_waived(self):
        text = "the Foodbank assesses member agencies a $0.08 per pound charge"
        assert extract_fee_waived(text) is False

    def test_full_year_waiver(self):
        text = "Shared Maintenance Fees were waived for the fiscal years 2023 and 2022"
        assert extract_fee_waived(text) is True


# =========================================================================
# _fiscal_year_from_filename
# =========================================================================


class TestFiscalYearFromFilename:
    """Tests for year extraction from filename."""

    def test_standard(self):
        assert _fiscal_year_from_filename("MOFC-Audit-2024.pdf") == 2024

    def test_path_component(self):
        assert _fiscal_year_from_filename("MOFC-Audit-2019.pdf") == 2019

    def test_no_year(self):
        with pytest.raises(ValueError):
            _fiscal_year_from_filename("audit.pdf")


# =========================================================================
# _restrict_to_primary_year
# =========================================================================


class TestRestrictToPrimaryYear:
    """Tests for restricting two-year text to primary year only."""

    def test_cuts_at_prior_year(self):
        text = "Primary year data\n" * 50 + "Activities of donated food for 6/30/2020\nPrior year"
        result = _restrict_to_primary_year(text, 2021)
        assert "Prior year" not in result
        assert "Primary year data" in result

    def test_single_year_unchanged(self):
        text = "Only one year of data here for 2019"
        result = _restrict_to_primary_year(text, 2019)
        assert result == text

    def test_june_30_format(self):
        text = "x " * 200 + "June 30, 2022\nprior year data"
        result = _restrict_to_primary_year(text, 2023)
        assert "prior year data" not in result


# =========================================================================
# _find_inventory_section
# =========================================================================


class TestFindInventorySection:
    """Tests for locating the inventory note."""

    def test_standard_heading(self):
        text = "Other stuff\nNote 4 – Inventory\nDonated Food"
        result = _find_inventory_section(text)
        assert result.startswith("Note 4")

    def test_em_dash(self):
        text = "Other stuff\nNote 7 — Inventory\nDonated Food"
        result = _find_inventory_section(text)
        assert result.startswith("Note 7")

    def test_no_heading_raises(self):
        with pytest.raises(ValueError, match="Cannot locate"):
            _find_inventory_section("No inventory note here")

    def test_fallback_to_donated_food(self):
        text = "Some text\nInventory\n\nDonated Food\nData here"
        result = _find_inventory_section(text)
        assert "Donated Food" in result


# =========================================================================
# extract_audit_data — integration with mocked PDF
# =========================================================================


class TestExtractAuditData:
    """Integration test with mocked PDF reading."""

    def _mock_full_text(self):
        """Build a realistic mock PDF text."""
        return (
            "MID-OHIO FOODBANK\nFOR THE YEAR ENDED JUNE 30, 2019\n"
            "Note 4 - Inventory\n"
            "Donated Food\n"
            "The estimated value of donated food is $1.62 per pound for 2019, "
            "which was based on the 2018 Feeding America Product Valuation Survey.\n"
            + _FY2019_DONATED_TEXT
            + "\n"
            + _PURCHASED_FOOD_TEXT
            + "\n"
            "the Foodbank assesses member agencies a $0.08 per pound charge"
        )

    @patch("mofc_financials.data_extraction.extract_audit._get_full_text")
    def test_returns_all_fieldnames(self, mock_get_text):
        mock_get_text.return_value = self._mock_full_text()
        result = extract_audit_data("data/raw/MOFC-Audit-2019.pdf")
        for field in FIELDNAMES:
            assert field in result, f"Missing field: {field}"

    @patch("mofc_financials.data_extraction.extract_audit._get_full_text")
    def test_fiscal_year(self, mock_get_text):
        mock_get_text.return_value = self._mock_full_text()
        result = extract_audit_data("data/raw/MOFC-Audit-2019.pdf")
        assert result["form_year"] == 2019

    @patch("mofc_financials.data_extraction.extract_audit._get_full_text")
    def test_valuation_rate(self, mock_get_text):
        mock_get_text.return_value = self._mock_full_text()
        result = extract_audit_data("data/raw/MOFC-Audit-2019.pdf")
        assert result["valuation_rate_per_lb"] == 1.62

    @patch("mofc_financials.data_extraction.extract_audit._get_full_text")
    def test_fee_not_waived(self, mock_get_text):
        mock_get_text.return_value = self._mock_full_text()
        result = extract_audit_data("data/raw/MOFC-Audit-2019.pdf")
        assert result["fee_waived"] is False

    @patch("mofc_financials.data_extraction.extract_audit._get_full_text")
    def test_cfap_defaults_to_zero(self, mock_get_text):
        mock_get_text.return_value = self._mock_full_text()
        result = extract_audit_data("data/raw/MOFC-Audit-2019.pdf")
        assert result["donated_lbs_received_cfap"] == 0
        assert result["donated_val_received_cfap"] == 0


# =========================================================================
# main — CLI entry point
# =========================================================================


class TestMain:
    """Tests for the CLI entry point."""

    @patch("mofc_financials.data_extraction.extract_audit.Path")
    def test_no_pdfs_exits(self, mock_path_cls):
        mock_data_dir = MagicMock()
        mock_data_dir.glob.return_value = []
        mock_path_cls.return_value.resolve.return_value.parent.parent.parent.parent.__truediv__ = (
            MagicMock(return_value=mock_data_dir)
        )
        with pytest.raises(SystemExit):
            main()
