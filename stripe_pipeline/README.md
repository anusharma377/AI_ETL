# Stripe Charges ETL Pipeline with Automated Data Quality Monitoring

An end-to-end data pipeline that extracts payment data from Stripe's test-mode
API, validates it against a set of data quality rules, and loads it into a
DuckDB warehouse -- orchestrated with Apache Airflow.

Built to practice production-grade data engineering patterns: pipeline
orchestration, automated data quality checks, and SLA-style alerting on
pipeline failures. AI coding assistants (Claude / GitHub Copilot) were used
throughout to scaffold the DAG, write the validation logic, and generate this
documentation -- see "AI-Assisted Development" below.

## Architecture

```
Stripe API (test mode)
      |
      v
  [extract.py]  -->  data/raw/charges_*.json
      |
      v
 [transform.py] -->  data/staging_charges.parquet
      |
      v
[quality_checks.py]  -->  pass/fail + report (raises on hard failure)
      |
      v
   [load.py]    -->  data/warehouse.duckdb (table: stripe_charges)
```

All four steps are wired together as an Airflow DAG
(`dags/stripe_etl_dag.py`) running on a daily schedule.

## Data Quality Checks

- Row count > 0 (freshness)
- No nulls in critical fields (charge_id, amount, currency, status)
- No negative charge amounts
- Currency values fall within expected set
- No duplicate charge_ids

A failed check raises `DataQualityError`, which fails the Airflow task and
surfaces in the Airflow UI as an alert -- this is a small-scale version of
SLA-based pipeline alerting.

## Setup (GitHub Codespaces -- no local install required)

1. Push this folder to a new GitHub repo.
2. Open the repo, click **Code -> Codespaces -> Create codespace on main**.
3. In the Codespace terminal:
   ```bash
   pip install -r requirements.txt
   export AIRFLOW_HOME=$(pwd)/airflow_home
   export STRIPE_SECRET_KEY=sk_test_your_key_here
   airflow standalone
   ```
4. Airflow will print a generated admin password and start the webserver
   (default port 8080). Codespaces will prompt you to open the forwarded
   port in your browser -- that's your Airflow UI.
5. Copy `dags/stripe_etl_dag.py` into `$AIRFLOW_HOME/dags/` (or symlink it),
   then trigger the `stripe_charges_etl` DAG from the UI.

## Getting Stripe Test Data

1. Create a free Stripe account at stripe.com (no business info needed for
   test mode).
2. In the Dashboard, make sure you're in **Test mode** (toggle top right).
3. Go to **Developers -> API keys** and copy your test **Secret key**
   (starts with `sk_test_`).
4. Stripe's test dashboard has a **"View test data" -> sample data** option
   that generates realistic fake charges, customers, and subscriptions with
   one click -- use that to populate data before running the pipeline.

## AI-Assisted Development

This project was built using Claude and GitHub Copilot to:
- Scaffold the Airflow DAG structure and task dependencies
- Generate the data quality validation logic and edge-case tests
- Write and refine this documentation

Estimated time saved: pipeline + validation logic + docs that would normally
take ~1-2 days was built in a single focused session.

## Possible Extensions

- Add Slack/email alerting on `DataQualityError`
- Add a `payouts` and `refunds` extraction alongside `charges`
- Swap DuckDB for Snowflake/BigQuery for a closer-to-production setup
- Add a small BI layer (Power BI / Streamlit) on top of the DuckDB tables
