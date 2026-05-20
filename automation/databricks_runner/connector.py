"""
connector.py
------------
Manages the Databricks SQL connector connection lifecycle.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from databricks import sql as dbsql

from .config import DatabricksConfig

if TYPE_CHECKING:
    from databricks.sql.client import Connection


class DatabricksConnector:
    """
    Thin wrapper around databricks-sql-connector that handles
    connection creation, reuse, and teardown.

    Usage
    -----
    connector = DatabricksConnector(config)
    with connector.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
    """

    def __init__(self, config: DatabricksConfig) -> None:
        self._config = config
        self._connection: Connection | None = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> Connection:
        """Open (or reuse) a connection to the SQL warehouse."""
        if self._connection is None:
            self._connection = dbsql.connect(
                server_hostname=self._config.host,
                http_path=self._config.http_path,
                access_token=self._config.token,
                # Forward default catalog / schema so queries don't need
                # fully qualified names unless they want to override.
                user_agent_entry="databricks-runner",
            )
        return self._connection

    def disconnect(self) -> None:
        """Close the connection if one is open."""
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            finally:
                self._connection = None

    def get_connection(self) -> Connection:
        """Return a live connection, creating one if necessary."""
        return self.connect()

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "DatabricksConnector":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.disconnect()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def test_connection(self) -> bool:
        """
        Verify that credentials and warehouse are reachable.
        Returns True on success, raises on failure.
        """
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ping")
            result = cur.fetchone()
        return result is not None and result[0] == 1
