"""
extract.py
Pulls charges from Stripe's TEST MODE API and writes raw JSON to disk.
Requires: pip install stripe
Requires env var: STRIPE_SECRET_KEY (your TEST mode secret key, starts with sk_test_)
"""

import os
import json
from datetime import datetime
import stripe

stripe.api_key = os.environ["STRIPE_SECRET_KEY"]

RAW_DATA_DIR = "data/raw"


def extract_charges(limit: int = 100) -> str:
    """Pull charges from Stripe test mode and save as raw JSON."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    charges = stripe.Charge.list(limit=limit)
    records = [charge.to_dict() for charge in charges.auto_paging_iter()]

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    out_path = f"{RAW_DATA_DIR}/charges_{timestamp}.json"

    with open(out_path, "w") as f:
        json.dump(records, f, default=str, indent=2)

    print(f"Extracted {len(records)} charges to {out_path}")
    return out_path


if __name__ == "__main__":
    extract_charges()
