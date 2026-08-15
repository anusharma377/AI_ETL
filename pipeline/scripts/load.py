"""
load.py
Loads the cleaned, validated charges dataframe into DuckDB
(a lightweight, file-based SQL warehouse -- no server needed).
"""

import duckdb
import pandas as pd

DB_PATH = "data/warehouse.duckdb"


def load_charges(df: pd.DataFrame, table_name: str = "stripe_charges") -> None:
    """Load dataframe into DuckDB, creating or replacing the table."""
    con = duckdb.connect(DB_PATH)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT * FROM df WHERE 1=0
    """)
    con.execute(f"DELETE FROM {table_name}")
    con.register("df_view", df)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df_view")

    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Loaded {row_count} rows into {DB_PATH} -> table '{table_name}'")

    con.close()


if __name__ == "__main__":
    import sys
    from transform import transform_charges
    df = transform_charges(sys.argv[1])
    load_charges(df)
