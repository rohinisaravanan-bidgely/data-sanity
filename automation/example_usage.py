"""
example_usage.py
----------------
Demonstrates every major feature of databricks_runner.

Run with:
    python example_usage.py

Make sure you have a .env file in this directory (copy .env.example and fill it in).
"""

import pandas as pd
from databricks_runner import DatabricksClient

# ── 1. Create the client ──────────────────────────────────────────────────────
#
# Reads credentials from the nearest .env file automatically.
# You can also pass an explicit path:
#   db = DatabricksClient(env_path="/path/to/my.env")
#
db = DatabricksClient(verbose=True)


# ── 2. Test the connection ────────────────────────────────────────────────────
db.test_connection()


# ── 3. Run a raw SQL query ────────────────────────────────────────────────────
df: pd.DataFrame = db.run("SELECT current_timestamp() AS now, current_user() AS user")
print(df)


# ── 4. Schema discovery ───────────────────────────────────────────────────────
catalogs = db.list_catalogs()
print("Catalogs:\n", catalogs)

schemas = db.list_schemas()                # uses default catalog from .env
print("Schemas:\n", schemas)

tables = db.list_tables()                  # uses default catalog + schema
print("Tables:\n", tables)

# Describe a specific table (edit the name to one that exists in your warehouse)
# cols = db.describe("orders")
# print("Columns:\n", cols)


# ── 5. Parameterised query ────────────────────────────────────────────────────
# df = db.run(
#     "SELECT * FROM orders WHERE status = %s AND created_date >= %s",
#     parameters=["shipped", "2024-01-01"],
# )


# ── 6. Natural-language → SQL (requires ANTHROPIC_API_KEY in .env) ───────────
#
# ask() translates the question to SQL using Claude, prints the generated SQL,
# runs it, and returns the result as a DataFrame.
#
# df = db.ask("How many rows are in the orders table?")
# print(df)
#
# df = db.ask(
#     "Show me the top 10 customers by total order value in the last 30 days",
#     table_hints=["orders", "customers"],   # pull schemas for these tables
# )
# print(df)
#
# df = db.ask(
#     "What percentage of orders were returned each month this year?",
#     extra_context="The 'returns' table has a foreign key 'order_id' to orders.",
# )


# ── 7. Translate only (no execution) ─────────────────────────────────────────
#
# sql = db.translate("Find all products with stock below reorder level")
# print("SQL:", sql)
#
# — or use dry_run to see the SQL without executing it:
# db.ask("Top 5 regions by sales", dry_run=True)


# ── 8. Use as a context manager ───────────────────────────────────────────────
with DatabricksClient() as db2:
    df = db2.run("SELECT 42 AS answer")
    print(df)
# Connection is automatically closed here


# ── 9. Register a schema manually (avoids a live DESCRIBE call) ───────────────
# db.register_table_schema("orders", """
# CREATE TABLE main.sales.orders (
#   order_id   BIGINT,
#   customer_id BIGINT,
#   status     STRING,
#   total      DOUBLE,
#   created_at TIMESTAMP
# );
# """)
# df = db.ask("How many orders were placed today?")
