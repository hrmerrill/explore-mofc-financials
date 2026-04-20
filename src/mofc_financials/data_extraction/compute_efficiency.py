"""Compute efficiency metrics from audit food-volume and 990 financial data.

Joins audit-extracted food volumes with IRS Form 990 financials to produce
KPIs like cost-per-meal, waste rate, and labor productivity.

Typical usage
-------------
CLI (after ``pip install -e .``)::

    mofc-compute-efficiency

Programmatic::

    from mofc_financials.data_extraction.compute_efficiency import compute_metrics
    metrics = compute_metrics(audit_row, financials_row, expense_rows)
"""

import csv
import sys
from pathlib import Path

# Feeding America standard: 1.2 meals per pound of food
MEALS_PER_POUND = 1.2

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_int(value: str) -> int | None:
    """Convert a CSV string value to int, returning ``None`` for blanks.

    Parameters
    ----------
    value : str
        Raw CSV cell value.

    Returns
    -------
    int or None
        Parsed integer, or ``None`` if *value* is empty or non-numeric.
    """
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return int(float(value))
        except ValueError:
            return None


def _to_float(value: str) -> float | None:
    """Convert a CSV string value to float, returning ``None`` for blanks.

    Parameters
    ----------
    value : str
        Raw CSV cell value.

    Returns
    -------
    float or None
        Parsed float, or ``None`` if *value* is empty or non-numeric.
    """
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _safe_div(numerator: int | float | None, denominator: int | float | None) -> float | str:
    """Divide, returning empty string if either operand is missing or zero.

    Parameters
    ----------
    numerator : int, float, or None
        Top of the fraction.
    denominator : int, float, or None
        Bottom of the fraction.

    Returns
    -------
    float or str
        Result of division, or ``""`` if division is impossible.
    """
    if numerator is None or denominator is None or denominator == 0:
        return ""
    return numerator / denominator


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

# Output CSV column order
FIELDNAMES = [
    "form_year",
    "total_lbs_distributed",
    "estimated_meals",
    "total_expenses",
    "food_expense",
    "operating_expenses",
    "operating_cost_per_lb",
    "operating_cost_per_meal",
    "all_in_cost_per_meal",
    "waste_rate_pct",
    "purchase_cost_per_lb",
    "employees",
    "lbs_per_employee",
    "total_labor_cost",
    "lbs_per_labor_dollar",
    "program_service_ratio_pct",
    "net_ppe",
    "capital_per_lb",
    "donated_lbs_tefap",
    "donated_lbs_csfp",
    "donated_lbs_oh_food",
    "donated_lbs_industry",
    "purchased_lbs",
    "meals_per_dollar",
]


def compute_metrics(
    audit_row: dict[str, str],
    financials_row: dict[str, str] | None,
    expense_rows: list[dict[str, str]] | None,
) -> dict[str, int | float | str]:
    """Compute efficiency KPIs for a single fiscal year.

    Parameters
    ----------
    audit_row : dict[str, str]
        Row from ``mofc_audit_food_volume.csv`` (or manual-edits variant).
    financials_row : dict[str, str] or None
        Matching row from ``mofc_990_financials_manual_edits.csv``, or
        ``None`` if 990 data is unavailable for this year.
    expense_rows : list[dict[str, str]] or None
        All expense-detail rows for this year from
        ``mofc_990_expense_detail_manual_edits.csv``, or ``None``.

    Returns
    -------
    dict[str, int | float | str]
        Dictionary with keys matching :data:`FIELDNAMES`.
    """
    year = audit_row["form_year"]

    # --- Food volumes from audit ---
    donated_disbursed_lbs = abs(_to_int(audit_row.get("donated_lbs_disbursed_total", "")) or 0)
    purchased_distributed_lbs = abs(_to_int(audit_row.get("purchased_lbs_distributed", "")) or 0)
    total_lbs = donated_disbursed_lbs + purchased_distributed_lbs

    donated_received_lbs = abs(_to_int(audit_row.get("donated_lbs_received_total", "")) or 0)
    donated_beginning_lbs = abs(_to_int(audit_row.get("donated_lbs_beginning_inv", "")) or 0)
    donated_discarded_lbs = abs(_to_int(audit_row.get("donated_lbs_discarded", "")) or 0)

    purchased_lbs = abs(_to_int(audit_row.get("purchased_lbs_purchases", "")) or 0)
    purchased_val = abs(_to_int(audit_row.get("purchased_val_purchases", "")) or 0)

    estimated_meals = round(total_lbs * MEALS_PER_POUND) if total_lbs else ""

    # Channel breakdowns (absolute values for display)
    donated_lbs_tefap = abs(_to_int(audit_row.get("donated_lbs_disbursed_tefap", "")) or 0)
    donated_lbs_csfp = abs(_to_int(audit_row.get("donated_lbs_disbursed_csfp", "")) or 0)
    donated_lbs_oh_food = abs(_to_int(audit_row.get("donated_lbs_disbursed_oh_food", "")) or 0)
    donated_lbs_industry = abs(_to_int(audit_row.get("donated_lbs_disbursed_industry", "")) or 0)

    # --- 990 financials ---
    total_expenses: int | None = None
    employees: int | None = None
    salaries: int | None = None
    net_ppe: int | str = ""
    program_expenses: int | None = None

    if financials_row:
        total_expenses = _to_int(financials_row.get("total_expenses", ""))
        employees = _to_int(financials_row.get("employees", ""))
        salaries = _to_int(financials_row.get("salaries_and_compensation", ""))

    # Food expense from 990 line 24a
    food_expense: int | None = None
    total_labor: int | None = None
    if expense_rows:
        for er in expense_rows:
            if er.get("line_number", "").strip() == "24a":
                food_expense = _to_int(er.get("total", ""))
            # Salaries line items for labor cost
            ln = er.get("line_number", "").strip()
            if ln in ("5", "7", "8", "9", "10"):
                val = _to_int(er.get("total", ""))
                if val:
                    total_labor = (total_labor or 0) + val
            # Program service expenses (line 25, column B)
            if ln == "25":
                ps = _to_int(er.get("program_service", ""))
                if ps:
                    program_expenses = ps

    # Fall back to salaries from financials if no labor from detail
    if total_labor is None and salaries is not None:
        total_labor = salaries

    # --- Computed KPIs ---
    operating_expenses: int | str = ""
    if total_expenses is not None and food_expense is not None:
        operating_expenses = total_expenses - food_expense

    operating_cost_per_lb = _safe_div(
        operating_expenses if isinstance(operating_expenses, int) else None, total_lbs
    )
    operating_cost_per_meal = _safe_div(
        operating_cost_per_lb if isinstance(operating_cost_per_lb, float) else None,
        MEALS_PER_POUND,
    )
    all_in_cost_per_meal = _safe_div(
        total_expenses, estimated_meals if isinstance(estimated_meals, int) else None
    )

    # Waste rate: discarded / (received + beginning inventory)
    waste_denom = donated_received_lbs + donated_beginning_lbs
    waste_rate = _safe_div(donated_discarded_lbs, waste_denom)
    waste_rate_pct: float | str = ""
    if isinstance(waste_rate, float):
        waste_rate_pct = round(waste_rate * 100, 2)

    purchase_cost_per_lb = _safe_div(purchased_val, purchased_lbs)

    # Meals per dollar of food purchased (cash donation purchasing power).
    # Matches MOFC's public "2.5 meals per $1 donated" methodology.
    purchase_cost_per_meal = _safe_div(
        purchase_cost_per_lb if isinstance(purchase_cost_per_lb, float) else None,
        MEALS_PER_POUND,
    )
    meals_per_dollar = _safe_div(
        1.0, purchase_cost_per_meal if isinstance(purchase_cost_per_meal, float) else None
    )

    lbs_per_employee = _safe_div(total_lbs, employees)
    lbs_per_labor_dollar = _safe_div(total_lbs, total_labor)

    program_ratio: float | str = ""
    if program_expenses is not None and total_expenses:
        program_ratio = round(program_expenses / total_expenses * 100, 2)

    capital_per_lb = _safe_div(
        _to_int(str(net_ppe)) if isinstance(net_ppe, int) else None, total_lbs
    )

    # --- Build row ---
    def _fmt(v: int | float | str) -> int | float | str:
        """Round floats to 4 decimal places for CSV output."""
        if isinstance(v, float):
            return round(v, 4)
        return v

    return {
        "form_year": year,
        "total_lbs_distributed": total_lbs,
        "estimated_meals": estimated_meals,
        "total_expenses": total_expenses if total_expenses is not None else "",
        "food_expense": food_expense if food_expense is not None else "",
        "operating_expenses": operating_expenses,
        "operating_cost_per_lb": _fmt(operating_cost_per_lb),
        "operating_cost_per_meal": _fmt(operating_cost_per_meal),
        "all_in_cost_per_meal": _fmt(all_in_cost_per_meal),
        "waste_rate_pct": waste_rate_pct,
        "purchase_cost_per_lb": _fmt(purchase_cost_per_lb),
        "employees": employees if employees is not None else "",
        "lbs_per_employee": _fmt(lbs_per_employee),
        "total_labor_cost": total_labor if total_labor is not None else "",
        "lbs_per_labor_dollar": _fmt(lbs_per_labor_dollar),
        "program_service_ratio_pct": program_ratio,
        "net_ppe": net_ppe,
        "capital_per_lb": _fmt(capital_per_lb),
        "donated_lbs_tefap": donated_lbs_tefap,
        "donated_lbs_csfp": donated_lbs_csfp,
        "donated_lbs_oh_food": donated_lbs_oh_food,
        "donated_lbs_industry": donated_lbs_industry,
        "purchased_lbs": purchased_distributed_lbs,
        "meals_per_dollar": _fmt(meals_per_dollar),
    }


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_csv(path: Path) -> list[dict[str, str]]:
    """Load a CSV file into a list of dicts.

    Parameters
    ----------
    path : Path
        Path to the CSV file.

    Returns
    -------
    list[dict[str, str]]
        Rows as dictionaries.
    """
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _find_csv(data_dir: Path, name: str, manual_edits_name: str) -> Path:
    """Find a CSV, preferring manual-edits version.

    Parameters
    ----------
    data_dir : Path
        Directory containing processed CSVs.
    name : str
        Base CSV filename.
    manual_edits_name : str
        Manual-edits variant filename.

    Returns
    -------
    Path
        Path to the found CSV file.

    Raises
    ------
    FileNotFoundError
        If neither version exists.
    """
    manual = data_dir / manual_edits_name
    if manual.exists():
        return manual
    base = data_dir / name
    if base.exists():
        return base
    raise FileNotFoundError(f"Neither {manual} nor {base} found")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Compute efficiency metrics and write to CSV.

    Reads audit food-volume data and 990 financials, computes KPIs,
    and writes ``data/processed/mofc_efficiency_metrics.csv``.
    """
    data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data" / "processed"

    # Load audit data
    audit_path = _find_csv(
        data_dir,
        "mofc_audit_food_volume.csv",
        "mofc_audit_food_volume_manual_edits.csv",
    )
    audit_rows = _load_csv(audit_path)
    print(f"Loaded {len(audit_rows)} audit rows from {audit_path.name}", file=sys.stderr)

    # Load 990 financials
    fin_rows: list[dict[str, str]] = []
    try:
        fin_path = _find_csv(
            data_dir,
            "mofc_990_financials.csv",
            "mofc_990_financials_manual_edits.csv",
        )
        fin_rows = _load_csv(fin_path)
        print(f"Loaded {len(fin_rows)} 990 rows from {fin_path.name}", file=sys.stderr)
    except FileNotFoundError:
        print("Warning: No 990 financials CSV found", file=sys.stderr)

    # Load expense detail
    exp_rows: list[dict[str, str]] = []
    try:
        exp_path = _find_csv(
            data_dir,
            "mofc_990_expense_detail.csv",
            "mofc_990_expense_detail_manual_edits.csv",
        )
        exp_rows = _load_csv(exp_path)
        print(f"Loaded {len(exp_rows)} expense rows from {exp_path.name}", file=sys.stderr)
    except FileNotFoundError:
        print("Warning: No expense detail CSV found", file=sys.stderr)

    # Index by year
    fin_by_year = {r["form_year"]: r for r in fin_rows}
    exp_by_year: dict[str, list[dict[str, str]]] = {}
    for r in exp_rows:
        exp_by_year.setdefault(r["form_year"], []).append(r)

    # Compute metrics
    results = []
    for audit_row in audit_rows:
        year = audit_row["form_year"]
        fin_row = fin_by_year.get(year)
        year_expenses = exp_by_year.get(year)
        metrics = compute_metrics(audit_row, fin_row, year_expenses)
        results.append(metrics)

    # Write output
    out_path = data_dir / "mofc_efficiency_metrics.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nWrote {len(results)} rows to {out_path}", file=sys.stderr)

    with open(out_path) as f:
        print(f.read())


if __name__ == "__main__":
    main()
