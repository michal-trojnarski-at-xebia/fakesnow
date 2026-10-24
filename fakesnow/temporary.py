from __future__ import annotations

from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

# Live temporary tables and views in the session, as (name, oid, kind). DuckDB lists its system views as temporary too.
_LIVE_SQL = """
    SELECT table_name, table_oid, 'TABLE' FROM duckdb_tables() WHERE temporary
    UNION ALL
    SELECT view_name, view_oid, 'VIEW' FROM duckdb_views() WHERE temporary AND database_name = 'temp'
"""


class TemporaryObjects:
    """Remember the Snowflake schema each session-local temporary table and view was created in.

    DuckDB keeps temporary objects in one session namespace (temp.main) and does not record the schema they were
    created in. OIDs survive renames and are never reused, and registrations for dropped objects are pruned when
    another temporary object is created, so session history stays bounded.
    """

    def __init__(self, duck_conn: DuckDBPyConnection):
        self._duck_conn = duck_conn
        # Temporary object OID -> (catalog, schema) it was created in.
        self._owners: dict[int, tuple[str | None, str | None]] = {}
        # Temporary table OID -> files COPY has loaded into it, kept apart from any permanent table's history.
        self._loaded_files: dict[int, set[str]] = {}

    def record(self, expression: Expr, catalog: str | None, schema: str | None) -> None:
        """Register a temporary table or view just created by expression in catalog.schema."""
        if not (
            isinstance(expression, exp.Create)
            and expression.args.get("kind") in ("TABLE", "VIEW")
            and expression.find(exp.TemporaryProperty)
            and (table := expression.find(exp.Table))
        ):
            return
        # Temporary names are unique within the session, so the name identifies the new object's OID.
        live = {name: oid for name, oid, _ in self._duck_conn.execute(_LIVE_SQL).fetchall()}
        # Forget dropped objects here rather than tracking DROP, so the registry is bounded by live objects.
        live_oids = set(live.values())
        self._owners = {oid: owner for oid, owner in self._owners.items() if oid in live_oids}
        self._loaded_files = {oid: files for oid, files in self._loaded_files.items() if oid in live_oids}
        # CREATE TEMP TABLE IF NOT EXISTS succeeds without creating anything when the name is already taken, so the
        # OID may already be registered. Keep the schema it was created in.
        self._owners.setdefault(live[table.name], (catalog, schema))

    def owner(self, name: str) -> tuple[str | None, str | None] | None:
        """The (catalog, schema) the live temporary table or view called name was created in, or None."""
        found = self._find(name)
        return found[0] if found else None

    def kind(self, name: str, catalog: str, schema: str) -> str | None:
        """The kind, TABLE or VIEW, of the live temporary object called name created in catalog.schema, or None."""
        found = self._find(name)
        return found[1] if found and found[0] == (catalog, schema) else None

    def loaded_files(self, name: str, catalog: str, schema: str) -> set[str] | None:
        """The files COPY has loaded into the live temporary table called name created in catalog.schema, or None.

        The set is the table's load history: COPY adds to it, and it goes when the table is dropped.
        """
        found = self._find_oid(name)
        if not found or found[1] != (catalog, schema):
            return None
        return self._loaded_files.setdefault(found[0], set())

    def _find(self, name: str) -> tuple[tuple[str | None, str | None], str] | None:
        found = self._find_oid(name)
        return (found[1], found[2]) if found else None

    def _find_oid(self, name: str) -> tuple[int, tuple[str | None, str | None], str] | None:
        if not self._owners:
            return None
        sql = f"SELECT oid, kind FROM ({_LIVE_SQL}) AS live(name, oid, kind) WHERE name = ?"
        live = self._duck_conn.execute(sql, [name]).fetchone()
        if not live or (owner := self._owners.get(live[0])) is None:
            return None
        return live[0], owner, live[1]
