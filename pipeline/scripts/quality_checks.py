"""
quality_checks.py
Runs data quality validation on the transformed charges dataframe.
Raises an alert (exception) if any check fails, so Airflow can flag
the task and surface it as a pipeline failure -- this is the
"data quality framework / SLA alerting" piece for the resume story.
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_quality")

VALID_CURRENCIES = {"USD", "EUR", "GBP", "CAD", "AUD"}
VALID_STATUSES = {"succeeded", "pending", "failed"}


class DataQualityError(Exception):
    pass


def run_quality_checks(df: pd.DataFrame) -> dict:
    """Run a battery of checks and return a report. Raises on hard failures."""
    report = {"row_count": len(df), "checks": []}

    # 1. Freshness check: at least one row
    if len(df) == 0:
        raise DataQualityError("No rows found -- pipeline extracted zero records.")
    report["checks"].append({"check": "row_count > 0", "passed": True})

    # 2. Null check on critical fields
    critical_fields = ["charge_id", "amount", "currency", "status"]
    null_counts = df[critical_fields].isnull().sum()
    nulls_found = null_counts[null_counts > 0]
    check_passed = nulls_found.empty
    report["checks"].append({
        "check": "no_nulls_in_critical_fields",
        "passed": check_passed,
        "detail": nulls_found.to_dict() if not check_passed else None,
    })
    if not check_passed:
        raise DataQualityError(f"Null values found in critical fields: {nulls_found.to_dict()}")

    # 3. Range check: amount should be non-negative
    negative_amounts = (df["amount"] < 0).sum()
    check_passed = negative_amounts == 0
    report["checks"].append({
        "check": "no_negative_amounts",
        "passed": check_passed,
        "detail": f"{negative_amounts} negative amounts found" if not check_passed else None,
    })
    if not check_passed:
        raise DataQualityError(f"Found {negative_amounts} negative charge amounts.")

    # 4. Referential/enum check: currency values are recognized
    unexpected_currencies = set(df["currency"].unique()) - VALID_CURRENCIES - {"UNKNOWN"}
    check_passed = len(unexpected_currencies) == 0
    report["checks"].append({
        "check": "known_currencies_only",
        "passed": check_passed,
        "detail": list(unexpected_currencies) if not check_passed else None,
    })
    # soft warning, not a hard failure
    if not check_passed:
        logger.warning(f"Unexpected currencies encountered: {unexpected_currencies}")

    # 5. Duplicate check
    dupes = df["charge_id"].duplicated().sum()
    check_passed = dupes == 0
    report["checks"].append({
        "check": "no_duplicate_charge_ids",
        "passed": check_passed,
        "detail": f"{dupes} duplicates" if not check_passed else None,
    })
    if not check_passed:
        raise DataQualityError(f"Found {dupes} duplicate charge_id values.")

    logger.info(f"Data quality report: {report}")
    return report


if __name__ == "__main__":
    import sys
    from transform import transform_charges
    df = transform_charges(sys.argv[1])
    run_quality_checks(df)
    print("All data quality checks passed.")
