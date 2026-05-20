"""
client.py
---------
DatabricksClient — the single entry point for all functionality.

    from databricks_runner import DatabricksClient

    db = DatabricksClient()          # reads .env automatically
    df = db.run("SELECT * FROM orders LIMIT 10")
    df = db.ask("Show me the top 5 customers by revenue this month")
"""

from __future__ import annotations

import pandas as pd

from .config import DatabricksConfig, load_config
from .connector import DatabricksConnector
from .query_runner import QueryRunner
from .nl_to_sql import NLToSQL


class DatabricksClient:
    """
    High-level client for Databricks SQL warehouses.

    Parameters
    ----------
    config    : DatabricksConfig instance. If None, config is loaded from
                the nearest .env file / environment variables.
    env_path  : Optional explicit path to a .env file.
    nl_model  : Claude model used for NL-to-SQL translation.
    verbose   : Whether to print query timing / row counts by default.

    Quick start
    -----------
    >>> from databricks_runner import DatabricksClient
    >>> db = DatabricksClient()
    >>> df = db.run("SELECT current_timestamp() AS now")
    >>> df = db.ask("How many orders were placed yesterday?")
    """

    def __init__(
        self,
        config: DatabricksConfig | None = None,
        env_path: str | None = None,
        nl_model: str = NLToSQL.DEFAULT_MODEL,
        verbose: bool = True,
    ) -> None:
        self._config = config or load_config(env_path)
        self._connector = DatabricksConnector(self._config)
        self._runner = QueryRunner(
            self._connector,
            default_catalog=self._config.catalog,
            default_schema=self._config.schema,
        )
        self._nl: NLToSQL | None = None
        self._nl_model = nl_model
        self.verbose = verbose

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def connect(self) -> "DatabricksClient":
        """Explicitly open the connection (also called lazily on first query)."""
        self._connector.connect()
        return self

    def disconnect(self) -> None:
        """Close the connection."""
        self._connector.disconnect()

    def test_connection(self) -> bool:
        """Ping the warehouse; returns True on success."""
        ok = self._connector.test_connection()
        if self.verbose:
            status = "✓ Connection successful" if ok else "✗ Connection failed"
            print(status)
        return ok

    # ------------------------------------------------------------------
    # SQL execution
    # ------------------------------------------------------------------

    def run(
        self,
        sql: str,
        *,
        catalog: str | None = None,
        schema: str | None = None,
        parameters: list | None = None,
        verbose: bool | None = None,
    ) -> pd.DataFrame:
        """
        Execute a SQL string and return results as a DataFrame.

        Parameters
        ----------
        sql        : SQL query to run.
        catalog    : Override the default catalog for this call.
        schema     : Override the default schema for this call.
        parameters : Optional list of bind parameters (%s placeholders).
        verbose    : Override the instance-level verbose flag.

        Returns
        -------
        pd.DataFrame
        """
        return self._runner.run(
            sql,
            catalog=catalog,
            schema=schema,
            parameters=parameters,
            verbose=self.verbose if verbose is None else verbose,
        )

    # ------------------------------------------------------------------
    # Natural-language interface
    # ------------------------------------------------------------------

    def ask(
        self,
        question: str,
        *,
        table_hints: list[str] | None = None,
        extra_context: str = "",
        catalog: str | None = None,
        schema: str | None = None,
        dry_run: bool = False,
        verbose: bool | None = None,
    ) -> pd.DataFrame:
        """
        Convert a plain-English question to SQL and execute it.

        Parameters
        ----------
        question      : Natural-language question or task.
        table_hints   : Table names whose schemas to fetch and send to Claude.
                        Auto-detected from the question when omitted.
        extra_context : Extra business rules or hints for the model.
        catalog       : Catalog to query against.
        schema        : Schema to query against.
        dry_run       : If True, generate and print the SQL but do NOT run it.
        verbose       : Override the instance-level verbose flag.

        Returns
        -------
        pd.DataFrame  (empty if dry_run=True)
        """
        nl = self._get_nl_engine()
        sql = nl.translate(
            question,
            table_hints=table_hints,
            extra_context=extra_context,
            verbose=True,  # always show generated SQL
        )

        if dry_run:
            print("(dry_run=True — query not executed)")
            return pd.DataFrame()

        return self._runner.run(
            sql,
            catalog=catalog,
            schema=schema,
            verbose=self.verbose if verbose is None else verbose,
        )

    def translate(
        self,
        question: str,
        table_hints: list[str] | None = None,
        extra_context: str = "",
    ) -> str:
        """
        Translate a question to SQL without running it.
        Useful for review/debugging before execution.
        """
        return self._get_nl_engine().translate(
            question,
            table_hints=table_hints,
            extra_context=extra_context,
            verbose=True,
        )

    # ------------------------------------------------------------------
    # Schema discovery
    # ------------------------------------------------------------------

    def list_catalogs(self) -> pd.DataFrame:
        """List all available catalogs."""
        return self._runner.list_catalogs()

    def list_schemas(self, catalog: str | None = None) -> pd.DataFrame:
        """List schemas in a catalog."""
        return self._runner.list_schemas(catalog)

    def list_tables(
        self, schema: str | None = None, catalog: str | None = None
    ) -> pd.DataFrame:
        """List tables in a schema."""
        return self._runner.list_tables(schema, catalog)

    def describe(
        self,
        table: str,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> pd.DataFrame:
        """Describe columns of a table."""
        return self._runner.describe_table(table, catalog, schema)

    def register_table_schema(self, table: str, ddl: str) -> None:
        """
        Pre-register a table's DDL so it is always included in NL-to-SQL
        prompts when that table name is mentioned. Useful for tables that
        are expensive to DESCRIBE live.
        """
        self._get_nl_engine().register_schema(table, ddl)

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "DatabricksClient":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.disconnect()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_nl_engine(self) -> NLToSQL:
        """Lazily initialise the NL-to-SQL engine."""
        if self._nl is None:
            self._nl = NLToSQL(
                api_key=self._config.anthropic_api_key,
                model=self._nl_model,
                query_runner=self._runner,
            )
        return self._nl
