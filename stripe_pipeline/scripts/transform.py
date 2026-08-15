"""
transform.py
Cleans and reshapes raw Stripe charge JSON into a flat table
ready for loading into DuckDB.
"""

import json
import pandas as pd


def transform_charges(raw_path: str) -> pd.DataFrame:
    """Flatten raw Stripe charge records into a clean dataframe."""
    with open(raw_path, "r") as f:
        raw = json.load(f)

    rows = []
    for charge in raw:
        rows.append({
            "charge_id": charge.get("id"),
            "amount": charge.get("amount", 0) / 100,  # cents -> dollars
            "currency": (charge.get("currency") or "").upper(),
            "status": charge.get("status"),
            "customer_id": charge.get("customer"),
            "paid": charge.get("paid"),
            "refunded": charge.get("refunded"),
            "created_ts": charge.get("created"),
            "created_at": pd.to_datetime(charge.get("created"), unit="s", errors="coerce"),
            "payment_method_type": (
                charge.get("payment_method_details", {}).get("type")
                if charge.get("payment_method_details") else None
            ),
        })

    df = pd.DataFrame(rows)

    # basic cleaning
    df = df.drop_duplicates(subset="charge_id")
    df["currency"] = df["currency"].fillna("UNKNOWN")

    return df


if __name__ == "__main__":
    import sys
    df = transform_charges(sys.argv[1])
    print(df.head())
    print(f"\nTransformed {len(df)} rows")
