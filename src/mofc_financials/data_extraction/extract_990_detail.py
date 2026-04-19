"""Extract granular line-item financials from IRS Form 990 Parts VIII and IX.

This module provides position-aware OCR extraction of detailed revenue
(Part VIII — Statement of Revenue) and expense (Part IX — Statement of
Functional Expenses) data from 990 PDFs.  Unlike the summary extractor in
``extract_990.py``, this module captures individual line items with their
multi-column breakdowns.

Typical usage
-------------
CLI (after ``pip install -e .``)::

    mofc-extract-detail

Programmatic::

    from mofc_financials.data_extraction.extract_990_detail import (
        extract_revenue_detail,
        extract_expense_detail,
    )
    revenue = extract_revenue_detail("data/raw/MOFC-990-2023.pdf")
    expenses = extract_expense_detail("data/raw/MOFC-990-2023.pdf")
"""

import csv
import io
import re
import sys
from pathlib import Path
from typing import TypedDict

import fitz  # PyMuPDF
import pytesseract
from PIL import Image

# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------


class WordInfo(TypedDict):
    """A single word from position-aware OCR output."""

    text: str
    left: int
    top: int
    width: int
    height: int
    conf: int


class LineItemRow(TypedDict, total=False):
    """A single extracted line item with column values."""

    form_year: str
    section: str
    line_number: str
    label: str
    col_a: str
    col_b: str
    col_c: str
    col_d: str


# ---------------------------------------------------------------------------
# Constants — column boundaries at 2× zoom (1224×1584 px)
# ---------------------------------------------------------------------------

# Part IX (Statement of Functional Expenses) column x-ranges.
# (A) Total, (B) Program service, (C) Management & general, (D) Fundraising
EXPENSE_COL_BOUNDS: list[tuple[str, int, int]] = [
    ("col_a", 520, 700),
    ("col_b", 700, 850),
    ("col_c", 850, 1020),
    ("col_d", 1020, 1224),
]

# Part VIII (Statement of Revenue) main column x-ranges.
# (A) Total, (B) Related/exempt, (C) Unrelated business, (D) Excluded
REVENUE_COL_BOUNDS: list[tuple[str, int, int]] = [
    ("col_a", 620, 780),
    ("col_b", 780, 900),
    ("col_c", 900, 1020),
    ("col_d", 1020, 1224),
]

# Contribution detail (lines 1a–1g) amounts sit left of the main columns.
CONTRIBUTION_COL_RANGE: tuple[int, int] = (400, 620)

# ---------------------------------------------------------------------------
# Line-item pattern definitions
# ---------------------------------------------------------------------------

# Each tuple: (line_number, canonical_label, compiled_regex)
# Patterns are matched case-insensitively against accumulated label text.

EXPENSE_LINE_DEFS: list[tuple[str, str, re.Pattern[str]]] = [
    (
        "1",
        "Grants and other assistance to domestic organizations",
        re.compile(r"grants.*(?:assistance|assis).*domestic\s+org", re.I),
    ),
    (
        "2",
        "Grants and other assistance to domestic individuals",
        re.compile(r"grants.*(?:assistance|assis).*domestic\s+ind", re.I),
    ),
    (
        "3",
        "Grants and other assistance to foreign organizations",
        re.compile(r"grants.*(?:assistance|assis).*foreign", re.I),
    ),
    (
        "4",
        "Benefits paid to or for members",
        re.compile(r"benefits\s+paid.*members", re.I),
    ),
    (
        "5",
        "Compensation of current officers, directors, trustees, key employees",
        re.compile(r"compensation\s+of\s+current\s+officers", re.I),
    ),
    (
        "6",
        "Compensation not included above to disqualified persons",
        re.compile(r"compensation\s+not\s+included", re.I),
    ),
    (
        "7",
        "Other salaries and wages",
        re.compile(r"other\s*salaries\s+and\s+wages", re.I),
    ),
    (
        "8",
        "Pension plan accruals and contributions",
        re.compile(r"pension\s+plan", re.I),
    ),
    (
        "9",
        "Other employee benefits",
        re.compile(r"other\s+employee\s+benefits", re.I),
    ),
    ("10", "Payroll taxes", re.compile(r"payroll\s*tax", re.I)),
    (
        "11a",
        "Management fees",
        re.compile(r"(?:fees.*nonemployees|11\s*\w*\s*management)", re.I),
    ),
    ("11b", "Legal fees", re.compile(r"\blegal\b", re.I)),
    ("11c", "Accounting fees", re.compile(r"\baccounting\b", re.I)),
    ("11d", "Lobbying fees", re.compile(r"\blobbying\b", re.I)),
    (
        "11e",
        "Professional fundraising services",
        re.compile(r"professional\s+fundraising\s+services", re.I),
    ),
    (
        "11f",
        "Investment management fees",
        re.compile(r"investment\s+management\s+fees", re.I),
    ),
    (
        "11g",
        "Other fees for services",
        re.compile(r"(?:line\s+11g|other.*if\s*line\s*11g)", re.I),
    ),
    (
        "12",
        "Advertising and promotion",
        re.compile(r"advertising\s+and\s+promotion", re.I),
    ),
    ("13", "Office expenses", re.compile(r"office\s+expenses", re.I)),
    (
        "14",
        "Information technology",
        re.compile(r"information\s*technology", re.I),
    ),
    ("16", "Occupancy", re.compile(r"\boccupancy\b", re.I)),
    ("17", "Travel", re.compile(r"(?:^|\b)(?:17\s+)?travel\b", re.I)),
    (
        "18",
        "Payments of travel or entertainment for public officials",
        re.compile(r"payments\s+of\s+travel.*(?:entertainment|officials)", re.I),
    ),
    (
        "19",
        "Conferences, conventions, and meetings",
        re.compile(r"conferences.*conventions.*meetings", re.I),
    ),
    (
        "22",
        "Depreciation, depletion, and amortization",
        re.compile(r"depreciation.*(?:depletion|amortization)", re.I),
    ),
    ("23", "Insurance", re.compile(r"(?:^|\b)(?:23\s+)?insurance\b", re.I)),
    (
        "25",
        "Total functional expenses",
        re.compile(r"total\s+functional\s+expenses", re.I),
    ),
]

# Revenue contribution detail lines (1a–1g) — single-amount column
CONTRIBUTION_LINE_DEFS: list[tuple[str, str, re.Pattern[str]]] = [
    ("1a", "Federated campaigns", re.compile(r"federated\s+campaigns", re.I)),
    ("1b", "Membership dues", re.compile(r"membership\s+dues", re.I)),
    (
        "1c",
        "Fundraising events",
        re.compile(r"fundraising\s*events", re.I),
    ),
    (
        "1d",
        "Related organizations",
        re.compile(r"related\s+organizations", re.I),
    ),
    (
        "1e",
        "Government grants (contributions)",
        re.compile(r"government\s+grants", re.I),
    ),
    (
        "1f",
        "All other contributions, gifts, grants, and similar amounts",
        re.compile(r"all\s+other\s+contributions", re.I),
    ),
    (
        "1g",
        "Noncash contributions included in lines 1a-1f",
        re.compile(r"noncash\s+contributions", re.I),
    ),
]

# Revenue main lines (1h, 3–12) — multi-column
REVENUE_LINE_DEFS: list[tuple[str, str, re.Pattern[str]]] = [
    (
        "1h",
        "Total contributions and grants",
        re.compile(r"(?:total.*add\s+lines?\s*1a|1h\b)", re.I),
    ),
    (
        "2g",
        "Total program service revenue",
        re.compile(r"total.*(?:add\s+lines?\s*2a|program\s+service)", re.I),
    ),
    (
        "3",
        "Investment income",
        re.compile(r"investment\s+income", re.I),
    ),
    (
        "4",
        "Income from investment of tax-exempt bond proceeds",
        re.compile(r"tax.?exempt\s+bond", re.I),
    ),
    ("5", "Royalties", re.compile(r"^\s*(?:5\s+)?royalties", re.I)),
    (
        "6d",
        "Net rental income or (loss)",
        re.compile(r"net\s+rental\s+income", re.I),
    ),
    (
        "7d",
        "Net gain or (loss) from sales of assets",
        re.compile(r"net\s*gain\s+or\s+\(?loss", re.I),
    ),
    (
        "8c",
        "Net income or (loss) from fundraising events",
        re.compile(r"net\s+income.*fundraising\s+events", re.I),
    ),
    (
        "9c",
        "Net income or (loss) from gaming activities",
        re.compile(r"net\s+income.*gaming", re.I),
    ),
    (
        "10c",
        "Net income or (loss) from sales of inventory",
        re.compile(r"net\s+income.*sales\s+of\s+inventory", re.I),
    ),
    (
        "11e",
        "Total other revenue",
        re.compile(r"(?:total.*add\s*lines?\s*(?:11)?a|11e\s+total)", re.I),
    ),
    ("12", "Total revenue", re.compile(r"total\s+revenue", re.I)),
]

# Revenue column names (for CSV headers)
REVENUE_COLUMNS: list[str] = [
    "total",
    "related_or_exempt",
    "unrelated_business",
    "excluded_from_tax",
]

# Expense column names (for CSV headers)
EXPENSE_COLUMNS: list[str] = [
    "total",
    "program_service",
    "management_and_general",
    "fundraising",
]


# ---------------------------------------------------------------------------
# OCR helpers
# ---------------------------------------------------------------------------


def ocr_page_with_positions(pdf_path: str, page_num: int) -> list[WordInfo]:
    """Render a PDF page at 2× resolution and return word-level OCR data.

    Parameters
    ----------
    pdf_path : str
        Filesystem path to the PDF file.
    page_num : int
        Zero-based page index to render.

    Returns
    -------
    list[WordInfo]
        Words with text and bounding-box position data, filtered to
        confidence > 0 and non-empty text.
    """
    doc = fitz.open(pdf_path)
    page = doc[page_num]
    mat = fitz.Matrix(2, 2)
    pix = page.get_pixmap(matrix=mat)
    img = Image.open(io.BytesIO(pix.tobytes("png")))
    data: dict[str, list[str | int]] = pytesseract.image_to_data(
        img, output_type=pytesseract.Output.DICT
    )

    words: list[WordInfo] = []
    texts = data["text"]
    for i in range(len(texts)):
        text = str(texts[i]).strip()
        conf = int(data["conf"][i])
        if text and conf > 0:
            words.append(
                WordInfo(
                    text=text,
                    left=int(data["left"][i]),
                    top=int(data["top"][i]),
                    width=int(data["width"][i]),
                    height=int(data["height"][i]),
                    conf=conf,
                )
            )
    return words


def find_section_page(pdf_path: str, marker: str) -> int | None:
    """Find the page number containing a section marker via plain-text OCR.

    Searches up to the first 15 pages for a page whose OCR text contains
    ``marker`` (case-insensitive).

    Parameters
    ----------
    pdf_path : str
        Filesystem path to the PDF file.
    marker : str
        Case-insensitive text to search for (e.g. ``"statement of revenue"``).

    Returns
    -------
    int or None
        Zero-based page index, or ``None`` if the marker is not found.
    """
    doc = fitz.open(pdf_path)
    for i in range(min(15, len(doc))):
        page = doc[i]
        mat = fitz.Matrix(2, 2)
        pix = page.get_pixmap(matrix=mat)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        text = str(pytesseract.image_to_string(img))
        if marker.lower() in text.lower():
            return i
    return None


# ---------------------------------------------------------------------------
# Line-processing helpers
# ---------------------------------------------------------------------------

# Y-pixel tolerance for grouping words into the same logical line
_Y_TOLERANCE = 12


def cluster_into_lines(
    words: list[WordInfo], y_tolerance: int = _Y_TOLERANCE
) -> list[list[WordInfo]]:
    """Group words into horizontal lines by y-coordinate proximity.

    Parameters
    ----------
    words : list[WordInfo]
        Word-level OCR data with position information.
    y_tolerance : int, optional
        Maximum vertical pixel distance between words on the same line.

    Returns
    -------
    list[list[WordInfo]]
        Lines of words, sorted top-to-bottom. Words within each line are
        sorted left-to-right.
    """
    if not words:
        return []

    sorted_words = sorted(words, key=lambda w: (w["top"], w["left"]))
    lines: list[list[WordInfo]] = [[sorted_words[0]]]

    for word in sorted_words[1:]:
        if abs(word["top"] - lines[-1][0]["top"]) <= y_tolerance:
            lines[-1].append(word)
        else:
            lines.append([word])

    # Sort words within each line left-to-right
    for line in lines:
        line.sort(key=lambda w: w["left"])

    return lines


def clean_number(text: str) -> str:
    """Strip OCR artifacts from a number string and return digits only.

    Handles commas, periods used as thousands separators, leading/trailing
    pipes (common OCR artifact on 990 forms), and parenthesised negatives.

    Parameters
    ----------
    text : str
        Raw OCR text for a single number token.

    Returns
    -------
    str
        Digit-only string, prefixed with ``"-"`` for negative values,
        or ``""`` if the text is not a valid number.

    Examples
    --------
    >>> clean_number("1,864,848")
    '1864848'
    >>> clean_number("|120,429,685")
    '120429685'
    >>> clean_number("(2,272,381)")
    '-2272381'
    """
    stripped = text.strip().strip("|").strip()

    # Parenthesised negatives: (2,272,381)
    paren_match = re.match(r"^\(([\d,.\s]+)\)$", stripped)
    if paren_match:
        inner = paren_match.group(1).replace(",", "").replace(".", "").replace(" ", "")
        return "-" + inner if inner.isdigit() else ""

    cleaned = stripped.replace(",", "").replace(".", "").replace(" ", "")
    return cleaned if cleaned.isdigit() else ""


def is_financial_number(word: WordInfo, x_min: int = 400) -> bool:
    """Check whether a word is a financial number in a column area.

    Parameters
    ----------
    word : WordInfo
        Word with text and position data.
    x_min : int
        Minimum x-coordinate to be considered in a column area.

    Returns
    -------
    bool
        True if the word is a number at or right of ``x_min``.
    """
    if word["left"] < x_min:
        return False
    return bool(clean_number(word["text"]))


def extract_column_values(
    words: list[WordInfo],
    col_bounds: list[tuple[str, int, int]],
) -> dict[str, str]:
    """Map number words to named columns by x-position.

    Parameters
    ----------
    words : list[WordInfo]
        Words from a single line (may contain both text and numbers).
    col_bounds : list[tuple[str, int, int]]
        Column definitions as ``(name, x_min, x_max)`` tuples.

    Returns
    -------
    dict[str, str]
        Column name → cleaned number string. Only columns with values
        are included.
    """
    result: dict[str, str] = {}
    for word in words:
        val = clean_number(word["text"])
        if not val:
            continue
        x = word["left"]
        for col_name, x_min, x_max in col_bounds:
            if x_min <= x < x_max:
                result[col_name] = val
                break
    return result


def _line_text(words: list[WordInfo]) -> str:
    """Reconstruct the text of a line from its words."""
    return " ".join(w["text"] for w in words)


def _match_patterns(
    label: str,
    patterns: list[tuple[str, str, re.Pattern[str]]],
) -> tuple[str, str] | None:
    """Match label text against a list of (line_num, label, pattern) defs.

    Parameters
    ----------
    label : str
        Combined label text (may span multiple OCR lines).
    patterns : list[tuple[str, str, re.Pattern[str]]]
        Pattern definitions to try in order.

    Returns
    -------
    tuple[str, str] or None
        ``(line_number, canonical_label)`` on match, else ``None``.
    """
    for line_num, canonical_label, pattern in patterns:
        if pattern.search(label):
            return line_num, canonical_label
    return None


# ---------------------------------------------------------------------------
# Revenue extraction (Part VIII)
# ---------------------------------------------------------------------------


def extract_revenue_detail(pdf_path: str) -> list[LineItemRow]:
    """Extract Part VIII (Statement of Revenue) line items from a 990 PDF.

    Finds the revenue page, OCRs it with position data, and extracts both
    contribution detail lines (1a–1g, single-amount) and main revenue lines
    (1h–12, multi-column).

    Parameters
    ----------
    pdf_path : str
        Filesystem path to an IRS 990 PDF.

    Returns
    -------
    list[LineItemRow]
        Extracted line items with column breakdowns.  Each dict has keys:
        ``line_number``, ``label``, and ``col_a`` through ``col_d``
        (where present).  For contribution detail lines (1a–1g), only
        ``col_a`` (the contribution amount) is populated.

    Notes
    -----
    Revenue column meanings:

    - **col_a** — Total revenue
    - **col_b** — Related or exempt function revenue
    - **col_c** — Unrelated business revenue
    - **col_d** — Revenue excluded from tax under sections 512–514
    """
    page_num = find_section_page(pdf_path, "statement of revenue")
    if page_num is None:
        return []

    words = ocr_page_with_positions(pdf_path, page_num)
    lines = cluster_into_lines(words)

    results: list[LineItemRow] = []
    label_buffer: list[str] = []
    seen_contributions_total = False
    seen_program_total = False
    program_letter_idx = 0  # For numbering 2a, 2b, ...
    other_rev_letter_idx = 0  # For numbering 11a, 11b, ...
    in_other_revenue = False

    for line_words in lines:
        # Separate text words (left of columns) from potential number words
        text_words: list[WordInfo] = []
        for w in line_words:
            if not is_financial_number(w, x_min=380):
                text_words.append(w)

        line_label = _line_text(text_words)

        # --- Check for contribution detail numbers (x < 620) ---
        contrib_vals: dict[str, str] = {}
        for w in line_words:
            val = clean_number(w["text"])
            if val and CONTRIBUTION_COL_RANGE[0] <= w["left"] < CONTRIBUTION_COL_RANGE[1]:
                contrib_vals["col_a"] = val

        # --- Check for main column numbers (x >= 620) ---
        main_vals = extract_column_values(line_words, REVENUE_COL_BOUNDS)

        has_numbers = bool(contrib_vals) or bool(main_vals)

        if not has_numbers:
            label_buffer.append(line_label)
            # Detect "other revenue" section entry
            if re.search(r"business\s+code", line_label, re.I):
                in_other_revenue = seen_program_total
            continue

        # Build full label from buffer + current line
        full_label = " ".join(label_buffer + [line_label]).strip()
        label_buffer = []

        # --- Try contribution detail patterns (1a–1g) ---
        if contrib_vals and not main_vals:
            match = _match_patterns(full_label, CONTRIBUTION_LINE_DEFS)
            if match:
                line_num, canonical_label = match
                row: LineItemRow = {
                    "line_number": line_num,
                    "label": canonical_label,
                }
                row.update(contrib_vals)  # type: ignore[typeddict-item]
                results.append(row)
                continue

        # --- Try main revenue patterns (1h, 2g, 3–12) ---
        match = _match_patterns(full_label, REVENUE_LINE_DEFS)
        if match:
            line_num, canonical_label = match
            vals = main_vals if main_vals else contrib_vals
            row = {"line_number": line_num, "label": canonical_label}
            row.update(vals)  # type: ignore[typeddict-item]
            results.append(row)

            if line_num == "1h":
                seen_contributions_total = True
            elif line_num == "2g":
                seen_program_total = True
            continue

        # --- Dynamic program service revenue lines (2a–2f) ---
        if seen_contributions_total and not seen_program_total and main_vals:
            program_letter_idx += 1
            letter = chr(ord("a") + program_letter_idx - 1)
            # Extract org-specific label from OCR text
            dynamic_label = _extract_dynamic_label(full_label)
            row = {
                "line_number": f"2{letter}",
                "label": dynamic_label,
            }
            row.update(main_vals)  # type: ignore[typeddict-item]
            results.append(row)
            continue

        # --- Dynamic other revenue lines (11a–11d) ---
        if in_other_revenue and main_vals:
            other_rev_letter_idx += 1
            letter = chr(ord("a") + other_rev_letter_idx - 1)
            dynamic_label = _extract_dynamic_label(full_label)
            row = {
                "line_number": f"11{letter}",
                "label": dynamic_label,
            }
            row.update(main_vals)  # type: ignore[typeddict-item]
            results.append(row)

    return results


# ---------------------------------------------------------------------------
# Expense extraction (Part IX)
# ---------------------------------------------------------------------------


def extract_expense_detail(pdf_path: str) -> list[LineItemRow]:
    """Extract Part IX (Statement of Functional Expenses) line items.

    Finds the expense page, OCRs it with position data, and extracts each
    expense line item with its four-column breakdown.

    Parameters
    ----------
    pdf_path : str
        Filesystem path to an IRS 990 PDF.

    Returns
    -------
    list[LineItemRow]
        Extracted line items with column breakdowns.  Each dict has keys:
        ``line_number``, ``label``, and ``col_a`` through ``col_d``
        (where present).

    Notes
    -----
    Expense column meanings:

    - **col_a** — Total expenses
    - **col_b** — Program service expenses
    - **col_c** — Management and general expenses
    - **col_d** — Fundraising expenses
    """
    page_num = find_section_page(pdf_path, "statement of functional")
    if page_num is None:
        return []

    words = ocr_page_with_positions(pdf_path, page_num)
    lines = cluster_into_lines(words)

    results: list[LineItemRow] = []
    label_buffer: list[str] = []
    in_other_expenses = False
    other_expense_letter_idx = 0

    for line_words in lines:
        text_words = [w for w in line_words if not is_financial_number(w, x_min=500)]
        line_label = _line_text(text_words)

        col_vals = extract_column_values(line_words, EXPENSE_COL_BOUNDS)

        if not col_vals:
            label_buffer.append(line_label)
            # Detect "24 Other expenses" section header
            if re.search(r"other\s+expenses.*(?:itemize|list|covered)", line_label, re.I):
                in_other_expenses = True
            continue

        full_label = " ".join(label_buffer + [line_label]).strip()
        label_buffer = []

        # --- Try known expense patterns ---
        match = _match_patterns(full_label, EXPENSE_LINE_DEFS)
        if match:
            line_num, canonical_label = match
            row: LineItemRow = {"line_number": line_num, "label": canonical_label}
            row.update(col_vals)  # type: ignore[typeddict-item]
            results.append(row)
            # After insurance (23), next items with numbers are 24a–24e
            if line_num == "23":
                in_other_expenses = True
            continue

        # --- Dynamic other-expense lines (24a–24e) ---
        if in_other_expenses:
            other_expense_letter_idx += 1
            letter = chr(ord("a") + other_expense_letter_idx - 1)
            if other_expense_letter_idx <= 5:  # 24a through 24e
                dynamic_label = _extract_dynamic_label(full_label)
                row = {"line_number": f"24{letter}", "label": dynamic_label}
                row.update(col_vals)  # type: ignore[typeddict-item]
                results.append(row)
            continue

    return results


def _extract_dynamic_label(full_label: str) -> str:
    """Extract a clean label from OCR text for dynamic line items.

    Strips leading line-number prefixes, OCR artifacts, and trailing
    dots/dashes.

    Parameters
    ----------
    full_label : str
        Raw label text from OCR (may include line numbers and artifacts).

    Returns
    -------
    str
        Cleaned label text.
    """
    # Strip leading line-number-like prefixes (e.g., "2a", "11a", "a", "b")
    cleaned = re.sub(r"^[\s|]*(?:\d+\s*)?[a-e]?\s+", "", full_label, count=1).strip()
    # Remove trailing dots, dashes, OCR noise
    cleaned = re.sub(r"[\s.·\-—_|]+$", "", cleaned)
    # Remove leading OCR artifacts
    cleaned = re.sub(r"^[|©§*]+\s*", "", cleaned)
    # Remove business-code-like prefixes (e.g., "900000", "boooss")
    cleaned = re.sub(r"\b[0bo]{3,}\d*\b", "", cleaned, flags=re.I).strip()
    return cleaned if cleaned else "Unknown"


# ---------------------------------------------------------------------------
# Combined extraction + CSV output
# ---------------------------------------------------------------------------

# CSV column orders
_REVENUE_CSV_FIELDS = [
    "form_year",
    "line_number",
    "label",
    "total",
    "related_or_exempt",
    "unrelated_business",
    "excluded_from_tax",
]

_EXPENSE_CSV_FIELDS = [
    "form_year",
    "line_number",
    "label",
    "total",
    "program_service",
    "management_and_general",
    "fundraising",
]

# Map from internal column keys to human-readable CSV column names
_COL_KEY_TO_REVENUE: dict[str, str] = {
    "col_a": "total",
    "col_b": "related_or_exempt",
    "col_c": "unrelated_business",
    "col_d": "excluded_from_tax",
}

_COL_KEY_TO_EXPENSE: dict[str, str] = {
    "col_a": "total",
    "col_b": "program_service",
    "col_c": "management_and_general",
    "col_d": "fundraising",
}


def _rows_to_csv_dicts(
    rows: list[LineItemRow],
    form_year: str,
    col_map: dict[str, str],
    fields: list[str],
) -> list[dict[str, str]]:
    """Convert internal LineItemRow dicts to CSV-ready dicts.

    Parameters
    ----------
    rows : list[LineItemRow]
        Extracted line items with ``col_a``–``col_d`` keys.
    form_year : str
        Tax year to include in each row.
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


def main() -> None:
    """Run granular extraction on all MOFC 990 PDFs and write detail CSVs.

    Scans ``data/raw/`` for files matching ``MOFC-990-*.pdf``, extracts
    Part VIII (revenue) and Part IX (expense) line items from each, and
    writes:

    - ``data/processed/mofc_990_revenue_detail.csv``
    - ``data/processed/mofc_990_expense_detail.csv``

    Results are also printed to stdout.
    """
    data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw"
    pdfs = sorted(data_dir.glob("MOFC-990-*.pdf"))
    if not pdfs:
        print("No 990 PDFs found in data/raw/", file=sys.stderr)
        sys.exit(1)

    all_revenue: list[dict[str, str]] = []
    all_expenses: list[dict[str, str]] = []

    for pdf in pdfs:
        print(f"Processing {pdf.name} (detail)...", file=sys.stderr)
        year_match = re.search(r"(\d{4})", pdf.stem)
        form_year = year_match.group(1) if year_match else ""

        revenue_rows = extract_revenue_detail(str(pdf))
        expense_rows = extract_expense_detail(str(pdf))

        all_revenue.extend(
            _rows_to_csv_dicts(revenue_rows, form_year, _COL_KEY_TO_REVENUE, _REVENUE_CSV_FIELDS)
        )
        all_expenses.extend(
            _rows_to_csv_dicts(expense_rows, form_year, _COL_KEY_TO_EXPENSE, _EXPENSE_CSV_FIELDS)
        )

    out_dir = data_dir.parent / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Write revenue detail CSV
    rev_path = out_dir / "mofc_990_revenue_detail.csv"
    _write_csv(rev_path, _REVENUE_CSV_FIELDS, all_revenue)
    print(f"Wrote {len(all_revenue)} revenue rows to {rev_path}", file=sys.stderr)

    # Write expense detail CSV
    exp_path = out_dir / "mofc_990_expense_detail.csv"
    _write_csv(exp_path, _EXPENSE_CSV_FIELDS, all_expenses)
    print(f"Wrote {len(all_expenses)} expense rows to {exp_path}", file=sys.stderr)

    # Print combined output to stdout
    print("\n=== Revenue Detail ===")
    with open(rev_path) as f:
        print(f.read())
    print("=== Expense Detail ===")
    with open(exp_path) as f:
        print(f.read())


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


if __name__ == "__main__":
    main()
