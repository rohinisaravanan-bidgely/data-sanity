"""
query_runner.py
---------------
Execute SQL queries against a Databricks SQL warehouse and return
results as pandas DataFrames.
"""

from __future__ import annotations

import time
from typing import Any

import pandas as pd

from .connector import DatabricksConnector
from .config import DatabricksConfig


class QueryRunner:
    """
    Runs SQL queries on a Databricks SQL warehouse.

    Parameters
    ----------
    connector : DatabricksConnector
        An open (or lazy) connector to the warehouse.
    default_catalog : str
        Catalog to USE before executing queries (can be overridden per-call).
    default_schema : str
        Schema to USE before executing queries (can be overridden per-call).
    """

    def __init__(
        self,
        connector: DatabricksConnector,
        default_catalog: str = "hive_metastore",
        default_schema: str = "default",
    ) -> None:
        self._connector = connector
        self.default_catalog = default_catalog
        self.default_schema = default_schema

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        sql: str,
        *,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list[Any] | None = None,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        Execute *sql* and return results as a DataFrame.

        Parameters
        ----------
        sql        : SQL string to execute (may include %s placeholders).
        catalog    : Override the default catalog for this call.
        schema     : Override the default schema for this call.
        parameters : Optional list of values for parameterised queries.
        verbose    : If True, print row count and elapsed time.

        Returns
        -------
        pd.DataFrame  (empty DataFrame for non-SELECT statements)
        """
        sql = sql.strip()
        conn = self._connector.get_connection()
        start = time.perf_counter()

        with conn.cursor() as cur:
            # Set catalog / schema context (skip if empty)
            _catalog = catalog if catalog is not None else self.default_catalog
            _schema = schema if schema is not None else self.default_schema
            if _catalog:
                cur.execute(f"USE CATALOG {_catalog}")
            if _schema:
                cur.execute(f"USE SCHEMA {_schema}")

            # Run the actual query
            if parameters:
                cur.execute(sql, parameters)
            else:
                cur.execute(sql)

            # Fetch results
            df = self._fetch_as_dataframe(cur)

        elapsed = time.perf_counter() - start
        if verbose:
            print(f"✓ Query completed in {elapsed:.2f}s — {len(df)} row(s) returned.")

        return df

    def run_many(
        self,
        queries: list[str],
        **kwargs,
    ) -> list[pd.DataFrame]:
        """Run a list of SQL statements and return a list of DataFrames."""
        return [self.run(q, **kwargs) for q in queries]

    # ------------------------------------------------------------------
    # Schema inspection helpers
    # ------------------------------------------------------------------

    def list_catalogs(self) -> pd.DataFrame:
        """Return available catalogs."""
        return self.run("SHOW CATALOGS", verbose=False)

    def list_schemas(self, catalog: str | None = None) -> pd.DataFrame:
        """Return schemas in the given (or default) catalog."""
        cat = catalog or self.default_catalog
        return self.run(f"SHOW SCHEMAS IN {cat}", verbose=False)

    def list_tables(
        self,
        schema: str | None = None,
        catalog: str | None = None,
    ) -> pd.DataFrame:
        """Return tables in the given (or default) schema."""
        cat = catalog or self.default_catalog
        sch = schema or self.default_schema
        return self.run(f"SHOW TABLES IN {cat}.{sch}", verbose=False)

    def describe_table(
        self,
        table: str,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> pd.DataFrame:
        """
        Return column definitions for *table*.

        *table* can be:
          - bare name:               "orders"
          - schema-qualified:        "sales.orders"
          - fully qualified:         "main.sales.orders"
        """
        cat = catalog or self.default_catalog
        sch = schema or self.default_schema
        # If table already has dots, use as-is; otherwise qualify it.
        if "." not in table:
            fqn = f"{cat}.{sch}.{table}"
        elif table.count(".") == 1:
            fqn = f"{cat}.{table}"
        else:
            fqn = table
        return self.run(f"DESCRIBE TABLE {fqn}", verbose=False)

    def get_schema_ddl(
        self,
        table: str,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> str:
        """
        Return a concise CREATE TABLE-style string for *table*,
        suitable for injecting into NL-to-SQL prompts.
        """
        df = self.describe_table(table, catalog=catalog, schema=schema)
        # DESCRIBE returns col_name, data_type, comment columns
        lines = [f"  {row['col_name']} {row['data_type']}" for _, row in df.iterrows()
                 if row.get("col_name") and not str(row["col_name"]).startswith("#")]
        cat = catalog or self.default_catalog
        sch = schema or self.default_schema
        fqn = table if "." in table else f"{cat}.{sch}.{table}"
        return f"CREATE TABLE {fqn} (\n" + ",\n".join(lines) + "\n);"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_as_dataframe(cursor) -> pd.DataFrame:
        """Convert cursor results to a DataFrame."""
        rows = cursor.fetchall()
        if not rows:
            # Still try to get column names if available
            cols = [desc[0] for desc in cursor.description] if cursor.description else []
            return pd.DataFrame(columns=cols)

        cols = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=cols)
