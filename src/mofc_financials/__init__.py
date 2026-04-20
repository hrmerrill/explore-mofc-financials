"""
mofc_financials — Extract and analyze Mid-Ohio Food Collective financial data.

This package provides tools for OCR-based extraction of financial data from
IRS Form 990 PDFs published by the Mid-Ohio Food Collective (MOFC), as well
as text-based extraction from annual audit PDFs and efficiency metric computation.
"""

from mofc_financials.data_extraction.compute_efficiency import compute_metrics
from mofc_financials.data_extraction.extract_990 import (
    extract_financials,
    extract_last_number,
)
from mofc_financials.data_extraction.extract_990_detail import (
    extract_expense_detail,
    extract_revenue_detail,
)
from mofc_financials.data_extraction.extract_audit import extract_audit_data
from mofc_financials.data_extraction.validate import run_pipeline, validate_year

__all__ = [
    "extract_financials",
    "extract_last_number",
    "extract_expense_detail",
    "extract_revenue_detail",
    "extract_audit_data",
    "compute_metrics",
    "run_pipeline",
    "validate_year",
]
