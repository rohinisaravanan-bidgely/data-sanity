"""
databricks_runner
-----------------
A lightweight Python library for running SQL queries on Databricks SQL warehouses,
with built-in natural-language-to-SQL translation powered by Claude.

Quick start::

    from databricks_runner import DatabricksClient

    db = DatabricksClient()   # reads credentials from .env
    df = db.run("SELECT * FROM orders LIMIT 5")
    df = db.ask("Show me total revenue by region for last quarter")
"""

from .client import DatabricksClient
from .config import DatabricksConfig, load_config
from .connector import DatabricksConnector
from .query_runner import QueryRunner
from .nl_to_sql import NLToSQL

__all__ = [
    "DatabricksClient",
    "DatabricksConfig",
    "load_config",
    "DatabricksConnector",
    "QueryRunner",
    "NLToSQL",
]

__version__ = "1.0.0"
