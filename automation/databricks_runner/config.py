"""
config.py
---------
Loads Databricks + Anthropic credentials from a .env file (or environment variables).
"""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _find_env_file() -> Path | None:
    """Walk up from cwd to find the nearest .env file."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        candidate = parent / ".env"
        if candidate.exists():
            return candidate
    return None


@dataclass
class DatabricksConfig:
    host: str          # e.g. "adb-1234567890.azuredatabricks.net"
    token: str         # personal access token
    warehouse_id: str  # SQL warehouse HTTP path segment
    catalog: str = "hive_metastore"
    schema: str = "default"
    anthropic_api_key: str = ""

    # Derived: full HTTP path used by the connector
    @property
    def http_path(self) -> str:
        return f"/sql/1.0/warehouses/{self.warehouse_id}"


def load_config(env_path: str | Path | None = None) -> DatabricksConfig:
    """
    Load config from a .env file or environment variables.

    Priority (highest → lowest):
      1. Explicit env_path argument
      2. Auto-detected .env file (walks up from cwd)
      3. OS environment variables already set

    Required variables:
      DATABRICKS_HOST         workspace hostname (no https://)
      DATABRICKS_TOKEN        personal access token
      DATABRICKS_WAREHOUSE_ID SQL warehouse ID (not the full path)

    Optional variables:
      DATABRICKS_CATALOG      default catalog  (default: hive_metastore)
      DATABRICKS_SCHEMA       default schema   (default: default)
      ANTHROPIC_API_KEY       needed for NL-to-SQL feature
    """
    if env_path:
        load_dotenv(dotenv_path=Path(env_path), override=True)
    else:
        detected = _find_env_file()
        if detected:
            load_dotenv(dotenv_path=detected, override=True)

    host = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "").strip()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()

    missing = []
    if not host:
        missing.append("DATABRICKS_HOST")
    if not token:
        missing.append("DATABRICKS_TOKEN")
    if not warehouse_id:
        missing.append("DATABRICKS_WAREHOUSE_ID")

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Please set them in your .env file or as OS environment variables.\n"
            "See .env.example for reference."
        )

    return DatabricksConfig(
        host=host,
        token=token,
        warehouse_id=warehouse_id,
        catalog=os.getenv("DATABRICKS_CATALOG", "hive_metastore"),
        schema=os.getenv("DATABRICKS_SCHEMA", "default"),
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
    )
