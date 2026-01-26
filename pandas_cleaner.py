import re
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# =====================================================
# CONFIG
# =====================================================

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

NON_NUMERIC_KEYS = {
    "id",
    "company_id",
    "year",
    "fy",
    "fiscal_year",
    "period"
}


# =====================================================
# BASIC HELPERS
# =====================================================

def to_float(val: Any) -> Optional[float]:
    """
    Convert messy API values into float.

    Handles:
      1234
      "1,234"
      "12.5"
      "12%"
      "10 Years: 11%"
      None
      "NULL", "NA"
    Rule:
      - Extract all numbers
      - Return LAST number (finance data convention)
    """
    if val is None:
        return None

    if isinstance(val, (int, float)):
        return float(val)

    if not isinstance(val, str):
        return None

    val = val.strip().replace(",", "")

    if val.upper() in {"NULL", "NA", "N/A", ""}:
        return None

    nums = re.findall(r"-?\d+\.?\d*", val)
    if not nums:
        return None

    return float(nums[-1])


def extract_year(val: Any) -> Optional[int]:
    """
    Handles:
      'Dec 2012'
      'Mar 2015'
      'FY 2020'
      2018
    """
    if val is None:
        return None

    if isinstance(val, int):
        return val

    if isinstance(val, str):
        m = re.search(r"\b(19|20)\d{2}\b", val)
        if m:
            return int(m.group())

    return None


def extract_period(text: Any) -> Optional[str]:
    """
    '10 Years: 21%' -> '10 Years'
    """
    if not isinstance(text, str):
        return None

    return text.split(":")[0].strip()


# =====================================================
# FINANCIAL TABLE CLEANER
# (P&L, Balance Sheet, Cashflow)
# =====================================================

def clean_financial_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Converts numeric-looking fields to float.
    Keeps identifiers untouched.
    """
    cleaned: List[Dict[str, Any]] = []

    for r in rows:
        row: Dict[str, Any] = {}

        for k, v in r.items():
            key = k.lower()

            if key == "year":
                row[k] = extract_year(v)
            elif key in NON_NUMERIC_KEYS:
                row[k] = v
            else:
                row[k] = to_float(v)

        cleaned.append(row)

    return cleaned


# =====================================================
# ANALYSIS TABLE CLEANER (CRITICAL)
# =====================================================

def clean_analysis(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Cleans analysis block and GUARANTEES non-null primary key.

    Primary key rule:
      - Use API 'id' if present
      - Else generate deterministic id:
            {company_id}_{period}
    """

    output: List[Dict[str, Any]] = []

    for r in rows:
        company_id = r.get("company_id")
        period = extract_period(r.get("compounded_sales_growth"))

        # 🔴 PRIMARY KEY FIX
        row_id = r.get("id")
        if not row_id and company_id and period:
            safe_period = period.replace(" ", "_")
            row_id = f"{company_id}_{safe_period}"

        output.append({
            "id": row_id,
            "company_id": company_id,
            "period": period,
            "sales_growth": to_float(r.get("compounded_sales_growth")),
            "profit_growth": to_float(r.get("compounded_profit_growth")),
            "roe": to_float(r.get("roe")),
            "stock_cagr": to_float(r.get("stock_price_cagr")),
        })

    return output


# =====================================================
# API RESPONSE VALIDATION
# =====================================================

def validate_api_data(api_json: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Ensures required sections exist and are valid.
    """

    if not isinstance(api_json, dict):
        return False, "API response is not a dict"

    if "data" not in api_json:
        return False, "Missing 'data' key"

    data = api_json["data"]

    required_sections = [
        "profitandloss",
        "balancesheet",
        "cashflow"
    ]

    for section in required_sections:
        if section not in data:
            return False, f"Missing '{section}' section"

        if not isinstance(data[section], list):
            return False, f"'{section}' is not a list"

    return True, None


# =====================================================
# OPTIONAL DEBUG LOGGER
# =====================================================

def log_cleaned(company_id: Any, data: Dict[str, Any]) -> None:
    """
    Writes cleaned data to timestamped JSON for debugging.
    """
    safe_id = "".join(c for c in str(company_id) if c.isalnum() or c in "_-")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    path = LOG_DIR / f"{safe_id}_{ts}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
