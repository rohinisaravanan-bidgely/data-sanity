"""
run_query.py
------------
CLI query runner for Databricks. Three modes:

  # Run SQL directly
  python run_query.py --sql "SELECT current_timestamp() AS now"

  # Run a .sql file
  python run_query.py --file my_query.sql

  # Interactive REPL (type queries, Ctrl+D to exit)
  python run_query.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
from databricks_runner import DatabricksClient


# ── Display helpers ────────────────────────────────────────────────────────────

def print_result(df: pd.DataFrame, elapsed: float) -> None:
    """Pretty-print a DataFrame result."""
    if df.empty:
        print("(no rows returned)")
    else:
        try:
            print(df.to_string(index=False))
        except Exception:
            print(df)
    print(f"\n{len(df)} row(s)  |  {elapsed:.2f}s")


def run_sql(db: DatabricksClient, sql: str) -> None:
    """Execute a single SQL string and print the result."""
    sql = sql.strip()
    if not sql:
        return
    print(f"\n-- Running:\n{sql}\n")
    start = time.perf_counter()
    try:
        df = db.run(sql, verbose=False)
        elapsed = time.perf_counter() - start
        print_result(df, elapsed)
    except Exception as exc:
        print(f"ERROR: {exc}")


# ── Modes ──────────────────────────────────────────────────────────────────────

def mode_single(db: DatabricksClient, sql: str) -> None:
    run_sql(db, sql)


def mode_file(db: DatabricksClient, path: str) -> None:
    p = Path(path)
    if not p.exists():
        print(f"File not found: {path}")
        sys.exit(1)
    sql = p.read_text()
    run_sql(db, sql)


def mode_repl(db: DatabricksClient) -> None:
    """
    Interactive REPL.
    - End a statement with a blank line or semicolon to run it.
    - Type 'exit' or press Ctrl+D to quit.
    """
    print("Databricks SQL REPL  (blank line or ';' to run  |  'exit' or Ctrl+D to quit)")
    print(f"Connected to: {db._config.host}")
    print(f"Schema:       {db._config.catalog}.{db._config.schema}\n")

    buffer: list[str] = []

    while True:
        prompt = "  ... " if buffer else "sql> "
        try:
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break

        stripped = line.strip()

        if stripped.lower() in ("exit", "quit", r"\q"):
            print("Bye.")
            break

        # Blank line or line ending with ; → execute whatever is buffered
        if stripped == "" or stripped.endswith(";"):
            if stripped.endswith(";"):
                buffer.append(stripped.rstrip(";"))
            sql = "\n".join(buffer).strip()
            buffer.clear()
            if sql:
                run_sql(db, sql)
        else:
            buffer.append(line)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run SQL queries against Databricks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--sql", "-s", metavar="QUERY", help="SQL string to run")
    group.add_argument("--file", "-f", metavar="PATH",  help="Path to a .sql file")
    parser.add_argument(
        "--schema", metavar="SCHEMA",
        help="Override default schema (default: pre_data_sanity)",
    )
    parser.add_argument(
        "--catalog", metavar="CATALOG",
        help="Override default catalog (default: hive_metastore)",
    )
    args = parser.parse_args()

    # Build client — credentials come from .env in the same directory
    env_path = Path(__file__).parent / ".env"
    db = DatabricksClient(env_path=str(env_path), verbose=False)

    # Apply schema/catalog overrides if provided
    if args.schema:
        db._runner.default_schema = args.schema
    if args.catalog:
        db._runner.default_catalog = args.catalog

    # Test connection first
    print("Connecting to Databricks...", end=" ", flush=True)
    try:
        db.test_connection()
        print("OK")
    except Exception as exc:
        print(f"FAILED\n{exc}")
        sys.exit(1)

    if args.sql:
        mode_single(db, args.sql)
    elif args.file:
        mode_file(db, args.file)
    else:
        mode_repl(db)

    db.disconnect()


if __name__ == "__main__":
    main()
