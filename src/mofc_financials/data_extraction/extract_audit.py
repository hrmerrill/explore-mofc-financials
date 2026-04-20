"""Extract food volume data from MOFC audited financial statement PDFs.

This module reads text-based audit PDFs (no OCR needed) and extracts
donated food inventory tables, purchased food tables, Feeding America
valuation rates, and Shared Maintenance Fee waiver status.  Output is
a single CSV suitable for downstream efficiency-metric computation.

Typical usage
-------------
CLI (after ``pip install -e .``)::

    mofc-extract-audit

Programmatic::

    from mofc_financials.data_extraction.extract_audit import extract_audit_data
    data = extract_audit_data("data/raw/MOFC-Audit-2024.pdf")
"""

import csv
import re
import sys
from pathlib import Path

import fitz  # PyMuPDF

# ---------------------------------------------------------------------------
# Number parsing
# ---------------------------------------------------------------------------

_NUM_RE = re.compile(r"[\$,\s]")
_PAREN_RE = re.compile(r"^\((.+)\)$")


def parse_number(raw: str) -> int | None:
    """Parse a number string, handling ``$``, commas, and parenthetical negatives.

    Parameters
    ----------
    raw : str
        Raw text that may contain ``$``, commas, spaces, or be wrapped in
        parentheses to indicate a negative value.

    Returns
    -------
    int or None
        Parsed integer, or ``None`` if *raw* is empty or unparseable.

    Examples
    --------
    >>> parse_number("$ 1,934,300")
    1934300
    >>> parse_number("(9,648,916)")
    -9648916
    >>> parse_number("")
    """
    raw = raw.strip()
    if not raw or raw == "-":
        return None
    negative = False
    m = _PAREN_RE.match(raw)
    if m:
        raw = m.group(1)
        negative = True
    cleaned = _NUM_RE.sub("", raw)
    if not cleaned:
        return None
    try:
        value = int(cleaned)
    except ValueError:
        try:
            value = int(float(cleaned))
        except ValueError:
            return None
    return -value if negative else value


# ---------------------------------------------------------------------------
# Text extraction helpers
# ---------------------------------------------------------------------------

# Channel labels in order they appear in the inventory note.
# CFAP only present in FY2021-2022.
_DONATED_CHANNELS = ["TEFAP", "CFAP", "CSFP", "OH Food Purchase", "Industry Surplus"]

# Regex to find the inventory note heading
_INVENTORY_NOTE_RE = re.compile(r"Note\s+\d+\s*[-–—]\s*Inventory", re.IGNORECASE)

# Regex for valuation rate
_VALUATION_RE = re.compile(
    r"\$\s*(\d+\.\d{2})\s*per\s+pound\s+for\s+(\d{4})",
    re.IGNORECASE,
)

# Regex for fee waiver
_FEE_WAIVER_RE = re.compile(
    r"(?:Shared\s+Maintenance\s+Fee|maintenance\s+fee)s?\s+were\s+waived",
    re.IGNORECASE,
)


def _get_full_text(pdf_path: str) -> str:
    """Extract all text from a PDF file.

    Parameters
    ----------
    pdf_path : str
        Path to the PDF file.

    Returns
    -------
    str
        Concatenated text from all pages, separated by form-feed characters.
    """
    doc = fitz.open(pdf_path)
    pages = []
    for page in doc:
        pages.append(page.get_text())
    doc.close()
    return "\f".join(pages)


def _find_inventory_section(full_text: str) -> str:
    """Locate the inventory note section within the full PDF text.

    Parameters
    ----------
    full_text : str
        Full text of the PDF.

    Returns
    -------
    str
        Text from the inventory note heading through the end of the
        purchased food table.

    Raises
    ------
    ValueError
        If no inventory note heading is found.
    """
    m = _INVENTORY_NOTE_RE.search(full_text)
    if not m:
        # Some years embed the heading differently; try broader match
        alt = re.search(r"Inventory\s*\n+\s*Donated\s+Food", full_text, re.IGNORECASE)
        if alt:
            return full_text[alt.start() :]
        raise ValueError("Cannot locate Inventory note in PDF text")
    return full_text[m.start() :]


def _extract_number_pair(line: str) -> tuple[int | None, int | None]:
    """Extract a (pounds, dollar_value) pair from a text line.

    Parameters
    ----------
    line : str
        A single line of text that may contain two numbers.

    Returns
    -------
    tuple[int | None, int | None]
        Parsed (pounds, dollar_value) pair.
    """
    # Find all number-like tokens (possibly parenthetical)
    tokens = re.findall(r"\([\d,]+\)|[\$]?\s*[\d,]+", line)
    nums = [parse_number(t) for t in tokens]
    nums = [n for n in nums if n is not None]
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], None
    return None, None


def _collect_numbers(
    lines: list[str],
    start: int,
    max_scan: int = 10,
    stop_patterns: list[str] | None = None,
) -> list[int]:
    """Scan forward from *start*, collecting numbers across lines.

    Skips blank lines and stops at lines that look like section labels
    (contain letters but no digits) *after* at least one number has been
    collected, or at lines matching *stop_patterns*.

    Parameters
    ----------
    lines : list[str]
        Lines to scan.
    start : int
        Index to begin scanning.
    max_scan : int
        Maximum number of lines to scan.
    stop_patterns : list[str] or None
        Lowercase patterns that should terminate scanning immediately
        (e.g., channel names that follow the current one).

    Returns
    -------
    list[int]
        Collected numbers in order of appearance.
    """
    nums: list[int] = []
    stops = stop_patterns or []
    for i in range(start, min(start + max_scan, len(lines))):
        line = lines[i].strip()
        if not line:
            continue
        line_lower = line.lower()

        # Stop at explicit stop patterns (but not on first line)
        if i > start and stops:
            if any(s in line_lower for s in stops):
                break

        # Stop if line looks like a new label (letters, no digits)
        # but only after we've already collected at least one number
        has_digit = any(c.isdigit() for c in line)
        has_alpha = re.search(r"[a-zA-Z]{2,}", line)
        if has_alpha and not has_digit and nums:
            break
        tokens = re.findall(r"\([\d,]+\)|[\$]?\s*[\d,]+", line)
        for t in tokens:
            n = parse_number(t)
            if n is not None:
                nums.append(n)
        if len(nums) >= 2:
            break
    return nums


def _find_line_with(lines: list[str], pattern: str, start: int = 0) -> int:
    """Find the first line index containing a pattern (case-insensitive).

    Parameters
    ----------
    lines : list[str]
        Lines to search.
    pattern : str
        Substring to search for (case-insensitive).
    start : int
        Index to start searching from.

    Returns
    -------
    int
        Index of matching line, or -1 if not found.
    """
    pat_lower = pattern.lower()
    for i in range(start, len(lines)):
        if pat_lower in lines[i].lower():
            return i
    return -1


def _extract_channel_block(
    lines: list[str],
    start_idx: int,
    channels: list[str],
) -> tuple[dict[str, tuple[int, int]], int, int | None, int | None]:
    """Extract channel-level pounds/value pairs from a block of lines.

    Scans forward from *start_idx* looking for channel labels and their
    associated numbers on subsequent lines.  Numbers may appear on the
    same line as the label or on the next 1–3 non-blank lines.

    Parameters
    ----------
    lines : list[str]
        All lines of the inventory section.
    start_idx : int
        Line index to begin scanning (after "Pounds received/disbursed").
    channels : list[str]
        Channel names to look for.

    Returns
    -------
    tuple
        (channel_dict, end_idx, total_lbs, total_val) where channel_dict
        maps channel keys to (lbs, val) tuples, end_idx is where scanning
        stopped, and total_lbs/total_val are from an explicit Total row
        if present.
    """
    result: dict[str, tuple[int, int]] = {}
    total_lbs: int | None = None
    total_val: int | None = None
    idx = start_idx
    end_idx = start_idx

    while idx < len(lines):
        line = lines[idx].strip()
        line_lower = line.lower()

        if not line:
            idx += 1
            continue

        # Check for Total row
        if "total" in line_lower and ("received" in line_lower or "disbursed" in line_lower):
            nums = _collect_numbers(lines, idx, max_scan=4)
            total_lbs = nums[0] if len(nums) >= 1 else None
            total_val = nums[1] if len(nums) >= 2 else None
            end_idx = idx + 1
            break

        # Stop if we hit a different section
        if any(
            kw in line_lower
            for kw in [
                "pounds disbursed",
                "pounds discarded",
                "ending inventory",
                "purchased food",
                "total inventory",
            ]
        ):
            end_idx = idx
            break

        # Check for channel labels
        for ch in channels:
            if ch.lower() in line_lower:
                # Build stop patterns: other channel names + section keywords
                other_channels = [c.lower() for c in channels if c != ch]
                stops = other_channels + [
                    "total",
                    "pounds disbursed",
                    "pounds discarded",
                    "ending inventory",
                    "purchased food",
                ]
                nums = _collect_numbers(lines, idx, max_scan=10, stop_patterns=stops)
                key = _channel_key(ch)
                if nums:
                    result[key] = (nums[0], nums[1] if len(nums) >= 2 else 0)
                break

        idx += 1
        end_idx = idx

    return result, end_idx, total_lbs, total_val


def _channel_key(channel_name: str) -> str:
    """Normalize a channel name to a CSV-friendly key.

    Parameters
    ----------
    channel_name : str
        Human-readable channel name from the PDF.

    Returns
    -------
    str
        Normalized key: ``tefap``, ``cfap``, ``csfp``, ``oh_food``,
        or ``industry``.
    """
    name = channel_name.lower().strip()
    if "tefap" in name:
        return "tefap"
    if "cfap" in name:
        return "cfap"
    if "csfp" in name:
        return "csfp"
    if "oh food" in name or "ohio food" in name:
        return "oh_food"
    if "industry" in name:
        return "industry"
    return name.replace(" ", "_")


# ---------------------------------------------------------------------------
# Main parsing functions
# ---------------------------------------------------------------------------


def parse_donated_food_table(inventory_text: str) -> dict[str, int]:
    """Parse the donated food inventory table from inventory note text.

    Parameters
    ----------
    inventory_text : str
        Text of the inventory note section (from ``_find_inventory_section``).

    Returns
    -------
    dict[str, int]
        Dictionary with keys like ``donated_lbs_received_tefap``,
        ``donated_val_received_tefap``, etc.
    """
    result: dict[str, int] = {}
    lines = inventory_text.split("\n")

    # --- Beginning Inventory ---
    bi_idx = _find_line_with(lines, "Beginning Inventory")
    if bi_idx >= 0:
        nums = _collect_numbers(lines, bi_idx, max_scan=10)
        result["donated_lbs_beginning_inv"] = nums[0] if len(nums) >= 1 else 0
        result["donated_val_beginning_inv"] = nums[1] if len(nums) >= 2 else 0

    # --- Pounds received ---
    recv_idx = _find_line_with(lines, "Pounds received for the year")
    if recv_idx >= 0:
        channels, end_idx, total_lbs, total_val = _extract_channel_block(
            lines, recv_idx + 1, _DONATED_CHANNELS
        )
        for ch_key, (lbs, val) in channels.items():
            result[f"donated_lbs_received_{ch_key}"] = lbs
            result[f"donated_val_received_{ch_key}"] = val

        # Compute totals
        if total_lbs is not None:
            result["donated_lbs_received_total"] = total_lbs
            result["donated_val_received_total"] = total_val or 0
        else:
            # FY2019: sum channels
            result["donated_lbs_received_total"] = sum(v[0] for v in channels.values())
            result["donated_val_received_total"] = sum(v[1] for v in channels.values())

        # --- Pounds disbursed ---
        disb_idx = _find_line_with(lines, "Pounds disbursed for the year", end_idx)
        if disb_idx < 0:
            disb_idx = _find_line_with(lines, "Pounds disbursed", end_idx)
        if disb_idx >= 0:
            d_channels, d_end, d_total_lbs, d_total_val = _extract_channel_block(
                lines, disb_idx + 1, _DONATED_CHANNELS
            )
            for ch_key, (lbs, val) in d_channels.items():
                result[f"donated_lbs_disbursed_{ch_key}"] = lbs
                result[f"donated_val_disbursed_{ch_key}"] = val

            if d_total_lbs is not None:
                result["donated_lbs_disbursed_total"] = d_total_lbs
                result["donated_val_disbursed_total"] = d_total_val or 0
            else:
                result["donated_lbs_disbursed_total"] = sum(v[0] for v in d_channels.values())
                result["donated_val_disbursed_total"] = sum(v[1] for v in d_channels.values())

            search_from = d_end
        else:
            search_from = end_idx

    else:
        search_from = 0

    # --- Pounds discarded ---
    disc_idx = _find_line_with(lines, "Pounds discarded", search_from)
    if disc_idx < 0:
        disc_idx = _find_line_with(lines, "discarded", search_from)
    if disc_idx >= 0:
        nums = _collect_numbers(lines, disc_idx, max_scan=10)
        result["donated_lbs_discarded"] = nums[0] if len(nums) >= 1 else 0
        result["donated_val_discarded"] = nums[1] if len(nums) >= 2 else 0

    # --- Ending Inventory ---
    end_inv_patterns = ["Ending Inventory", "ending inventory"]
    ei_idx = -1
    for pat in end_inv_patterns:
        ei_idx = _find_line_with(lines, pat, search_from)
        if ei_idx >= 0:
            break
    if ei_idx >= 0:
        nums = _collect_numbers(lines, ei_idx, max_scan=10)
        result["donated_lbs_ending_inv"] = nums[0] if len(nums) >= 1 else 0
        result["donated_val_ending_inv"] = nums[1] if len(nums) >= 2 else 0

    return result


def parse_purchased_food_table(inventory_text: str) -> dict[str, int]:
    """Parse the purchased food inventory table from inventory note text.

    Parameters
    ----------
    inventory_text : str
        Text of the inventory note section.

    Returns
    -------
    dict[str, int]
        Dictionary with keys like ``purchased_lbs_beginning_inv``,
        ``purchased_val_purchases``, etc.
    """
    result: dict[str, int] = {}

    # Find "Purchased Food" section
    pf_idx = inventory_text.lower().find("purchased food")
    if pf_idx < 0:
        return result

    pf_text = inventory_text[pf_idx:]
    lines = pf_text.split("\n")

    field_map = {
        "beginning inventory": ("purchased_lbs_beginning_inv", "purchased_val_beginning_inv"),
        "purchases": ("purchased_lbs_purchases", "purchased_val_purchases"),
        "food distributed": ("purchased_lbs_distributed", "purchased_val_distributed"),
        "ending inventory": ("purchased_lbs_ending_inv", "purchased_val_ending_inv"),
    }

    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        for pattern, (lbs_key, val_key) in field_map.items():
            if pattern in line_lower:
                # Skip "Purchased Food" header line itself for "purchases"
                if pattern == "purchases" and "purchased food" in line_lower:
                    continue
                # Only take first occurrence of each field (skip prior-year data)
                if lbs_key in result:
                    continue
                nums = _collect_numbers(lines, i, max_scan=10)
                result[lbs_key] = nums[0] if len(nums) >= 1 else 0
                result[val_key] = nums[1] if len(nums) >= 2 else 0
                break

        # Stop at TOTAL INVENTORY or next note
        if "total inventory" in line_lower or re.match(r"note\s+\d+", line_lower):
            break
        # Stop after all fields found
        if len(result) >= 8:
            break

    return result


def extract_valuation_rate(full_text: str) -> float | None:
    """Extract the Feeding America per-pound valuation rate.

    Parameters
    ----------
    full_text : str
        Full text of the audit PDF.

    Returns
    -------
    float or None
        Valuation rate in dollars per pound, or ``None`` if not found.
    """
    m = _VALUATION_RE.search(full_text)
    if m:
        return float(m.group(1))
    return None


def extract_fee_waived(full_text: str) -> bool:
    """Detect whether Shared Maintenance Fees were waived.

    Parameters
    ----------
    full_text : str
        Full text of the audit PDF.

    Returns
    -------
    bool
        ``True`` if fees were waived (fully or partially), ``False`` otherwise.
    """
    return bool(_FEE_WAIVER_RE.search(full_text))


def _fiscal_year_from_filename(filename: str) -> int:
    """Extract the fiscal year from an audit PDF filename.

    Parameters
    ----------
    filename : str
        Filename like ``MOFC-Audit-2024.pdf``.

    Returns
    -------
    int
        Four-digit fiscal year.

    Raises
    ------
    ValueError
        If the year cannot be extracted.
    """
    m = re.search(r"(\d{4})", filename)
    if not m:
        raise ValueError(f"Cannot extract year from filename: {filename}")
    return int(m.group(1))


def _restrict_to_primary_year(inventory_text: str, fiscal_year: int) -> str:
    """Restrict inventory text to the primary fiscal year's table only.

    For two-year comparative reports (FY2020+), the text contains two
    sets of inventory tables.  This function returns only the text for
    the primary (first) year.

    Parameters
    ----------
    inventory_text : str
        Full inventory note text.
    fiscal_year : int
        The primary fiscal year of the report.

    Returns
    -------
    str
        Text restricted to the primary year's tables.
    """
    prior_year = fiscal_year - 1

    # Look for the prior year's table start.  It appears as a date line
    # like "6/30/2020" or "06/30/2020" or "June 30, 2020" after the
    # primary year's table.
    patterns = [
        rf"(?:6/30|06/30|June\s+30)[,/]\s*{prior_year}",
        rf"Activities.*{prior_year}",
        rf"for\s+{prior_year}\s+are\s+summarized",
    ]
    earliest_match = len(inventory_text)
    for pat in patterns:
        m = re.search(pat, inventory_text, re.IGNORECASE)
        if m and m.start() < earliest_match:
            # Only use if it's after some reasonable amount of primary-year data
            if m.start() > 200:
                earliest_match = m.start()

    return inventory_text[:earliest_match]


# ---------------------------------------------------------------------------
# Top-level extraction
# ---------------------------------------------------------------------------

# CSV column order
FIELDNAMES = [
    "form_year",
    "donated_lbs_received_tefap",
    "donated_val_received_tefap",
    "donated_lbs_received_cfap",
    "donated_val_received_cfap",
    "donated_lbs_received_csfp",
    "donated_val_received_csfp",
    "donated_lbs_received_oh_food",
    "donated_val_received_oh_food",
    "donated_lbs_received_industry",
    "donated_val_received_industry",
    "donated_lbs_received_total",
    "donated_val_received_total",
    "donated_lbs_disbursed_tefap",
    "donated_val_disbursed_tefap",
    "donated_lbs_disbursed_cfap",
    "donated_val_disbursed_cfap",
    "donated_lbs_disbursed_csfp",
    "donated_val_disbursed_csfp",
    "donated_lbs_disbursed_oh_food",
    "donated_val_disbursed_oh_food",
    "donated_lbs_disbursed_industry",
    "donated_val_disbursed_industry",
    "donated_lbs_disbursed_total",
    "donated_val_disbursed_total",
    "donated_lbs_discarded",
    "donated_val_discarded",
    "donated_lbs_beginning_inv",
    "donated_val_beginning_inv",
    "donated_lbs_ending_inv",
    "donated_val_ending_inv",
    "purchased_lbs_beginning_inv",
    "purchased_val_beginning_inv",
    "purchased_lbs_purchases",
    "purchased_val_purchases",
    "purchased_lbs_distributed",
    "purchased_val_distributed",
    "purchased_lbs_ending_inv",
    "purchased_val_ending_inv",
    "valuation_rate_per_lb",
    "fee_waived",
]


def extract_audit_data(pdf_path: str) -> dict[str, int | float | bool | str]:
    """Extract all food-volume data from a single audit PDF.

    Parameters
    ----------
    pdf_path : str
        Path to an MOFC audit PDF file.

    Returns
    -------
    dict[str, int | float | bool | str]
        Dictionary with keys matching :data:`FIELDNAMES`.
    """
    filename = Path(pdf_path).name
    fiscal_year = _fiscal_year_from_filename(filename)

    full_text = _get_full_text(pdf_path)
    inventory_text = _find_inventory_section(full_text)
    primary_text = _restrict_to_primary_year(inventory_text, fiscal_year)

    donated = parse_donated_food_table(primary_text)
    # Parse purchased food from full inventory text (not restricted),
    # because _restrict_to_primary_year may cut before the purchased
    # food section in years where the prior-year intro text appears
    # between the donated and purchased tables.
    purchased = parse_purchased_food_table(inventory_text)
    valuation = extract_valuation_rate(full_text)
    fee_waived = extract_fee_waived(full_text)

    # Build output row with defaults of 0 for missing channels
    row: dict[str, int | float | bool | str] = {"form_year": fiscal_year}
    for field in FIELDNAMES:
        if field == "form_year":
            continue
        if field == "valuation_rate_per_lb":
            row[field] = valuation if valuation is not None else ""
        elif field == "fee_waived":
            row[field] = fee_waived
        elif field in donated:
            row[field] = donated[field]
        elif field in purchased:
            row[field] = purchased[field]
        else:
            row[field] = 0

    return row


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run extraction on all MOFC audit PDFs and write results to CSV.

    Scans ``data/raw/`` for files matching ``MOFC-Audit-*.pdf``, extracts
    food-volume data from each, and writes a combined CSV to
    ``data/processed/mofc_audit_food_volume.csv``.
    """
    data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw"
    pdfs = sorted(data_dir.glob("MOFC-Audit-*.pdf"))
    if not pdfs:
        print("No audit PDFs found in data/raw/", file=sys.stderr)
        sys.exit(1)

    rows = []
    for pdf in pdfs:
        print(f"Processing {pdf.name}...", file=sys.stderr)
        row = extract_audit_data(str(pdf))
        rows.append(row)

    out_path = data_dir.parent / "processed" / "mofc_audit_food_volume.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} rows to {out_path}", file=sys.stderr)

    with open(out_path) as f:
        print(f.read())


if __name__ == "__main__":
    main()
