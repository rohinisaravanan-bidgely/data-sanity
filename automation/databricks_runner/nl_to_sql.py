"""
nl_to_sql.py
------------
Converts natural-language questions into Databricks SQL using Claude.

Requires ANTHROPIC_API_KEY in your .env file.
"""

from __future__ import annotations

import re
import textwrap
from typing import TYPE_CHECKING

import anthropic

if TYPE_CHECKING:
    from .query_runner import QueryRunner


# ---------------------------------------------------------------------------
# System prompt template
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = textwrap.dedent("""
    You are an expert Databricks SQL assistant.
    Your job is to convert a user's plain-English question into a single,
    correct Databricks SQL (Spark SQL) query.

    Rules:
    - Output ONLY the raw SQL statement — no markdown fences, no explanation,
      no commentary, no trailing semicolon.
    - Use ANSI SQL / Spark SQL syntax compatible with Databricks SQL warehouses.
    - Never use unsupported syntax (e.g. TOP N — use LIMIT N instead).
    - If the question is ambiguous, make a sensible assumption and add it as
      a SQL comment on the first line (e.g. -- Assumed: orders in the last 30 days).
    - If the question cannot be answered with a single SQL query, return the
      string: UNSUPPORTED
""").strip()


# ---------------------------------------------------------------------------
# NLToSQL class
# ---------------------------------------------------------------------------

class NLToSQL:
    """
    Translates natural-language requests to Databricks SQL queries
    using the Anthropic Claude API.

    Parameters
    ----------
    api_key      : Anthropic API key (falls back to ANTHROPIC_API_KEY env var).
    model        : Claude model to use (defaults to claude-3-5-haiku for speed).
    query_runner : Optional QueryRunner used to auto-fetch table schemas when
                   table names are mentioned in the question.
    """

    DEFAULT_MODEL = "claude-3-5-haiku-20241022"

    def __init__(
        self,
        api_key: str = "",
        model: str = DEFAULT_MODEL,
        query_runner: "QueryRunner | None" = None,
    ) -> None:
        if not api_key:
            import os
            api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise ValueError(
                "Anthropic API key is required for NL-to-SQL. "
                "Set ANTHROPIC_API_KEY in your .env file."
            )
        self._client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self._runner = query_runner
        self._schema_cache: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def translate(
        self,
        question: str,
        *,
        table_hints: list[str] | None = None,
        extra_context: str = "",
        verbose: bool = True,
    ) -> str:
        """
        Translate *question* into a Databricks SQL query string.

        Parameters
        ----------
        question      : Plain-English question or task description.
        table_hints   : Optional list of table names to fetch schemas for
                        and include as context. If omitted and a QueryRunner
                        is attached, relevant tables are auto-detected.
        extra_context : Free-form additional context (e.g. business rules).
        verbose       : If True, print the generated SQL before returning.

        Returns
        -------
        str  — raw SQL query ready to execute.

        Raises
        ------
        ValueError  — if Claude returns "UNSUPPORTED".
        """
        schema_context = self._build_schema_context(question, table_hints)
        user_message = self._build_user_message(question, schema_context, extra_context)

        response = self._client.messages.create(
            model=self.model,
            max_tokens=2048,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        sql = response.content[0].text.strip()

        # Strip accidental markdown code fences
        sql = re.sub(r"^```[a-z]*\n?", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\n?```$", "", sql)
        sql = sql.strip()

        if sql.upper() == "UNSUPPORTED":
            raise ValueError(
                f"Claude cannot translate this request into a single SQL query:\n{question}"
            )

        if verbose:
            print("── Generated SQL ─────────────────────────────────────────")
            print(sql)
            print("──────────────────────────────────────────────────────────")

        return sql

    def register_schema(self, table_name: str, ddl: str) -> None:
        """
        Manually register a table's DDL string so it is always included
        as context when that table name appears in a question.
        """
        self._schema_cache[table_name.lower()] = ddl

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_schema_context(
        self,
        question: str,
        table_hints: list[str] | None,
    ) -> str:
        """
        Build a block of CREATE TABLE ... statements to inject into the prompt.
        """
        schemas: list[str] = []

        # 1. Use explicitly provided hints
        tables_to_fetch: list[str] = list(table_hints or [])

        # 2. If no hints given, check the cache for any table whose name
        #    appears verbatim in the question (simple heuristic).
        if not tables_to_fetch:
            q_lower = question.lower()
            tables_to_fetch = [t for t in self._schema_cache if t in q_lower]

        for table in tables_to_fetch:
            key = table.lower()
            if key in self._schema_cache:
                schemas.append(self._schema_cache[key])
            elif self._runner is not None:
                # Fetch live from Databricks and cache it
                try:
                    ddl = self._runner.get_schema_ddl(table)
                    self._schema_cache[key] = ddl
                    schemas.append(ddl)
                except Exception as exc:
                    schemas.append(f"-- Could not fetch schema for {table}: {exc}")

        return "\n\n".join(schemas)

    @staticmethod
    def _build_user_message(
        question: str,
        schema_context: str,
        extra_context: str,
    ) -> str:
        parts: list[str] = []

        if schema_context:
            parts.append("Available table schemas:\n\n" + schema_context)

        if extra_context:
            parts.append("Additional context:\n" + extra_context)

        parts.append("Question / Task:\n" + question)

        return "\n\n".join(parts)
