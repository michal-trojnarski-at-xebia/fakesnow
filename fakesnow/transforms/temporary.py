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
    temporary object resolves to it only from that schema, as in Snowflake, and any reference from another schema
    is qualified so it does not resolve to the temporary object.
    """
    if (
        isinstance(expression, exp.Create)
        and expression.args.get("kind") in ("TABLE", "VIEW")
        and expression.find(exp.TemporaryProperty)
        and (table := expression.find(exp.Table))
    ):
        owner = (table.catalog or current_database, table.db or current_schema)
        if (existing := temporaries.owner(table.name)) and existing != owner:
            # DuckDB names temporary objects without their schema, so this one would replace or clash with the other
            raise NotImplementedError("Temporary objects with the same name in different schemas")
        expression.args["temporary_owner"] = owner
        table.set("catalog", None)
        table.set("db", None)
        return expression

    if (
        isinstance(expression, exp.Table)
        and not _is_target(expression)
        and not _is_other_kind(expression)
        and not _is_cte(expression)
        and (owner := temporaries.owner(expression.name))
    ):
        catalog = expression.catalog or current_database
        schema = expression.db or current_schema
        if owner == (catalog, schema):
            # An unqualified reference already resolves to the temporary object, which DuckDB searches first.
            if expression.args.get("db"):
                expression.set("catalog", exp.to_identifier("temp"))
                expression.set("db", exp.to_identifier("main"))
        elif catalog and schema:
            # Qualify fully so the reference resolves to a permanent object rather than the temporary one.
            expression.set("catalog", exp.to_identifier(catalog))
            expression.set("db", exp.to_identifier(schema))

    return expression


def _is_target(table: exp.Table) -> bool:
    """Whether table is the object a CREATE makes or a COPY loads.

    A permanent CREATE must not redirect to temp, and COPY resolves its target's stage by its Snowflake name.
    """
    parent = table.parent.parent if isinstance(table.parent, exp.Schema) else table.parent
    return isinstance(parent, (exp.Create, exp.Copy)) and (parent.this is table or parent.this.this is table)


def _is_other_kind(table: exp.Table) -> bool:
    """Whether table names an object that cannot be a table or view, eg: DROP SEQUENCE or USE SCHEMA."""
    parent = table.parent
    return isinstance(parent, (exp.Use, exp.Show)) or (
        isinstance(parent, (exp.Drop, exp.Describe))
        and isinstance(kind := parent.args.get("kind"), str)
        and kind.upper() not in ("TABLE", "VIEW")
    )


def _is_cte(table: exp.Table) -> bool:
    """Whether table may name a CTE rather than a temporary object."""
    return not table.args.get("db") and any(cte.alias == table.name for cte in table.root().find_all(exp.CTE))
