from __future__ import annotations

from sqlglot import Expr, exp

from fakesnow.temporary import TemporaryObjects


def temporary_objects(
    expression: Expr,
    temporaries: TemporaryObjects,
    current_database: str | None,
    current_schema: str | None,
) -> Expr:
    """Place temporary tables and views in DuckDB's temp.main, and resolve qualified references to them.

    DuckDB only creates temporary objects in the temp catalog, so CREATE TEMPORARY TABLE|VIEW loses its qualifiers.
    The schema it was created in is kept in temporary_owner for TemporaryObjects.record. A qualified reference to a
    temporary object resolves to it only from that schema, as in Snowflake.
    """
    if (
        isinstance(expression, exp.Create)
        and expression.args.get("kind") in ("TABLE", "VIEW")
        and expression.find(exp.TemporaryProperty)
        and (table := expression.find(exp.Table))
    ):
        expression.args["temporary_owner"] = (table.catalog or current_database, table.db or current_schema)
        table.set("catalog", None)
        table.set("db", None)
        return expression

    if (
        isinstance(expression, exp.Table)
        # An unqualified reference already resolves to the temporary object, which DuckDB searches first. Leaving
        # it alone also keeps CTEs that share a temporary object's name.
        and expression.args.get("db")
        and not _is_target(expression)
        and not _is_other_kind(expression)
        and (owner := temporaries.owner(expression.name))
        and owner == (expression.catalog or current_database, expression.db)
    ):
        expression.set("catalog", exp.to_identifier("temp"))
        expression.set("db", exp.to_identifier("main"))

    return expression


def _is_target(table: exp.Table) -> bool:
    """Whether table is the object a CREATE makes or a COPY loads.

    A permanent CREATE must not redirect to temp, and COPY resolves its target's stage by its Snowflake name.
    """
    parent = table.parent.parent if isinstance(table.parent, exp.Schema) else table.parent
    return isinstance(parent, (exp.Create, exp.Copy)) and (parent.this is table or parent.this.this is table)


def _is_other_kind(table: exp.Table) -> bool:
    """Whether table names an object that cannot be a table or view, eg: DROP SEQUENCE."""
    parent = table.parent
    return (
        isinstance(parent, (exp.Drop, exp.Describe))
        and isinstance(kind := parent.args.get("kind"), str)
        and kind.upper() not in ("TABLE", "VIEW")
    )
