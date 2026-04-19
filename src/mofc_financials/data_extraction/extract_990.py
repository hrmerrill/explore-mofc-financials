"""Extract key financials from MOFC IRS Form 990 PDFs via OCR.

This module reads image-based PDF scans of IRS 990 forms published by
the Mid-Ohio Food Collective, applies OCR to locate the Part I Summary
page, and parses structured financial fields (revenue, expenses, assets,
etc.) into a flat dictionary suitable for CSV export or downstream analysis.

Typical usage
-------------
CLI (after ``pip install -e .``)::

    mofc-extract

Programmatic::

    from mofc_financials.data_extraction.extract_990 import extract_financials
    data = extract_financials("data/raw/MOFC-990-2023.pdf")
"""

import csv
import io
import re
import sys
from pathlib import Path

import fitz  # PyMuPDF
import pytesseract
from PIL import Image

# ---------------------------------------------------------------------------
# OCR helpers
# ---------------------------------------------------------------------------


def ocr_page(pdf_path: str, page_num: int) -> str:
    """Render a single PDF page at 2× resolution and OCR it.

    Parameters
    ----------
    pdf_path : str
        Filesystem path to the PDF file.
    page_num : int
        Zero-based page index to render.

    Returns
    -------
    str
        Raw OCR text extracted from the rendered page image.
    """
    doc = fitz.open(pdf_path)
    page = doc[page_num]
    # 2× zoom gives tesseract enough resolution for reliable recognition
    mat = fitz.Matrix(2, 2)
    pix = page.get_pixmap(matrix=mat)
    img = Image.open(io.BytesIO(pix.tobytes("png")))
    return str(pytesseract.image_to_string(img))


def find_summary_page(pdf_path: str) -> str:
    """Locate and OCR the Part I Summary page containing top-level financials.

    Scans the first five pages of the PDF looking for the page that contains
    both "Total revenue" and "Total expenses" — the hallmark of the Part I
    Summary section on IRS Form 990.

    Parameters
    ----------
    pdf_path : str
        Filesystem path to the PDF file.

    Returns
    -------
    str
        OCR text of the summary page, or an empty string if not found.
    """
    doc = fitz.open(pdf_path)
    for i in range(min(5, len(doc))):
        text = ocr_page(pdf_path, i)
        if "Total revenue" in text and "Total expenses" in text:
            return text
    return ""


# ---------------------------------------------------------------------------
# Number-parsing utilities
# ---------------------------------------------------------------------------


def extract_last_number(line: str, negative_ok: bool = False) -> str:
    """Extract the rightmost number from an OCR text line.

    IRS 990 summary lines typically show prior-year and current-year values
    side-by-side.  The *rightmost* number is the current / end-of-year value.

    The parser works right-to-left so it can tolerate common OCR artifacts
    like spaces after commas (e.g. ``"115,703, 645"``).

    Parameters
    ----------
    line : str
        A single line of OCR text potentially containing one or more numbers.
    negative_ok : bool, optional
        When ``True``, detect parenthesised negative values such as
        ``"(2,272,381)"``.  Default is ``False``.

    Returns
    -------
    str
        The extracted number as a digit-only string (e.g. ``"120458264"``),
        prefixed with ``"-"`` for negative values, or ``""`` if no number
        is found.

    Examples
    --------
    >>> extract_last_number("91,201,516 120,458,264")
    '120458264'
    >>> extract_last_number("Net loss ... (2,272,381)", negative_ok=True)
    '-2272381'
    """
    line = line.rstrip()

    # --- Handle parenthesised negatives (e.g. "(2,272,381)") ---
    if negative_ok and line.endswith(")"):
        paren_content = re.search(r"\(([\d,.\s]+)\)\s*$", line)
        if paren_content:
            inner = paren_content.group(1).replace(",", "").replace(".", "").replace(" ", "")
            return "-" + inner if inner.isdigit() else ""

    # --- Right-to-left digit collection ---
    i = len(line) - 1

    # Skip any trailing non-digit characters
    while i >= 0 and not line[i].isdigit():
        i -= 1
    if i < 0:
        return ""

    parts = []
    while i >= 0:
        # Grab a contiguous run of digits
        end = i + 1
        while i >= 0 and line[i].isdigit():
            i -= 1
        parts.append(line[i + 1 : end])

        # A comma/period immediately before the digits means the number
        # continues (e.g. "120,458,264")
        if i >= 0 and line[i] in (",", "."):
            i -= 1
            continue
        # OCR sometimes inserts a space after a comma: "170, 580"
        elif i >= 1 and line[i] == " " and line[i - 1] in (",", "."):
            i -= 2  # skip both the space and the comma
            continue
        else:
            break

    result = "".join(reversed(parts))
    return ("-" + result) if (negative_ok and line.endswith(")")) else result


# ---------------------------------------------------------------------------
# Financial field extraction
# ---------------------------------------------------------------------------

# Ordered list of field names that will appear in the output dictionary
FINANCIAL_FIELDS = [
    "form_year",
    "gross_receipts",
    "employees",
    "volunteers",
    "contributions_and_grants",
    "program_service_revenue",
    "investment_income",
    "other_revenue",
    "total_revenue",
    "total_expenses",
    "salaries_and_compensation",
    "professional_fundraising_fees",
    "other_expenses",
    "revenue_less_expenses",
    "total_assets_eoy",
    "total_liabilities_eoy",
    "net_assets_eoy",
]


def extract_financials(pdf_path: str) -> dict[str, str]:
    """Extract all Part I Summary financial fields from a single 990 PDF.

    Parameters
    ----------
    pdf_path : str
        Filesystem path to an IRS 990 PDF (image-based scan).

    Returns
    -------
    dict
        Dictionary keyed by ``FINANCIAL_FIELDS`` with string values.
        Missing or unrecognised fields are set to ``""``.
    """
    filename = Path(pdf_path).stem
    year_match = re.search(r"(\d{4})", filename)
    form_year = year_match.group(1) if year_match else ""

    text = find_summary_page(pdf_path)
    if not text:
        print(f"  WARNING: Could not find summary page in {filename}", file=sys.stderr)
        return {"form_year": form_year}

    lines = text.split("\n")

    result = {field: "" for field in FINANCIAL_FIELDS}
    result["form_year"] = form_year

    for idx, line in enumerate(lines):
        ll = line.lower()

        # --- Revenue & workforce fields ---
        if "gross receipts" in ll:
            val = extract_last_number(line)
            if val and len(val) > 4:
                result["gross_receipts"] = val

        elif re.search(r"individuals?\s*empl", ll):
            nums = re.findall(r"\d+", line)
            result["employees"] = nums[-1] if nums else ""

        elif re.search(r"vo\w*nte\w*rs", ll) and re.search(r"es\w*mat", ll):
            # Matches "volunteers" (with OCR variants) + "estimated"
            val = extract_last_number(line)
            if val and len(val) > 2:
                result["volunteers"] = val

        elif "contributions and grants" in ll:
            result["contributions_and_grants"] = extract_last_number(line)

        elif "program service revenue" in ll:
            result["program_service_revenue"] = extract_last_number(line)

        elif re.search(r"investment\s*income", ll):
            result["investment_income"] = extract_last_number(line, negative_ok=True)

        elif "other revenue" in ll and ("lines 5" in ll or "11e" in ll):
            result["other_revenue"] = extract_last_number(line)

        elif "total revenue" in ll and "add lines" in ll:
            result["total_revenue"] = extract_last_number(line)

        # --- Expense fields ---
        elif "salaries" in ll and "compensation" in ll:
            result["salaries_and_compensation"] = extract_last_number(line)

        elif "fundraising fees" in ll:
            result["professional_fundraising_fees"] = extract_last_number(line)

        elif re.search(r"11f.?24e", ll) or ("other expenses" in ll and "column" in ll):
            result["other_expenses"] = extract_last_number(line)

        elif "total expenses" in ll and "add lines" in ll:
            result["total_expenses"] = extract_last_number(line)

        elif "revenue less expenses" in ll:
            result["revenue_less_expenses"] = extract_last_number(line, negative_ok=True)

        # --- Balance-sheet fields ---
        elif "total assets" in ll and re.search(r"line\s*16", ll):
            result["total_assets_eoy"] = extract_last_number(line)

        elif "total liabilities" in ll and re.search(r"line\s*26", ll):
            result["total_liabilities_eoy"] = extract_last_number(line)

        elif re.search(r"net\s*assets or fund", ll):
            val = extract_last_number(line)
            if val and len(val) > 3:
                result["net_assets_eoy"] = val
            else:
                # OCR sometimes splits the label from the value across lines;
                # look back up to 3 lines for a plausible number
                for lookback in range(1, 4):
                    if idx >= lookback:
                        prev = lines[idx - lookback].strip()
                        if prev:
                            val = extract_last_number(prev)
                            if val and len(val) > 3:
                                result["net_assets_eoy"] = val
                                break

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run extraction on all MOFC 990 PDFs and write results to CSV.

    Scans ``data/raw/`` for files matching ``MOFC-990-*.pdf``, extracts
    financial data from each, and writes a combined CSV to
    ``data/processed/mofc_990_financials.csv``.  The CSV is also printed
    to stdout for easy piping.
    """
    data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw"
    pdfs = sorted(data_dir.glob("MOFC-990-*.pdf"))
    if not pdfs:
        print("No 990 PDFs found in data/raw/", file=sys.stderr)
        sys.exit(1)

    rows = []
    for pdf in pdfs:
        print(f"Processing {pdf.name}...", file=sys.stderr)
        row = extract_financials(str(pdf))
        rows.append(row)

    out_path = data_dir.parent / "processed" / "mofc_990_financials.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} rows to {out_path}", file=sys.stderr)

    with open(out_path) as f:
        print(f.read())


if __name__ == "__main__":
    main()
