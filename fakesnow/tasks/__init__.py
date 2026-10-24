"""Snowflake tasks: the statements that manage them and the catalog that stores them."""

from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.tasks.catalog import TaskCatalog, _table
from fakesnow.tasks.statements import CreateTask, DropTask, TaskStatement, parse_task_statement
from fakesnow.transforms.transforms import SUCCESS_NOP

if TYPE_CHECKING:
    from fakesnow.conn import FakeSnowflakeConnection


def task_statement(expression: Expr) -> Expr:
    """Replace a task statement, which sqlglot parses as an opaque command, with a placeholder carrying it."""
    if isinstance(expression, exp.Command) and (statement := parse_task_statement(expression.sql("snowflake"))):
        placeholder = SUCCESS_NOP.copy()
        placeholder.args["task_statement"] = statement
        return placeholder
    return expression


def execute(statement: TaskStatement, conn: FakeSnowflakeConnection) -> str:
    """Run a task statement, returning the SELECT that produces its result rows."""
    catalog = TaskCatalog(
        conn._duck_conn,  # noqa: SLF001
        conn.database if conn.database_set else None,
        conn.schema if conn.schema_set else None,
        conn.role,
    )
    if isinstance(statement, CreateTask):
        return catalog.create(statement)
    if isinstance(statement, DropTask):
        return catalog.drop(statement)
    raise NotImplementedError(type(statement).__name__)


def drop_schema(duck_conn: DuckDBPyConnection, database: str, schema: str) -> None:
    """Delete the tasks of a dropped schema."""
    try:
        with duck_conn.cursor() as conn:
            conn.execute(f"delete from {_table(database)} where schema_name = ?", [schema])
    except duckdb.CatalogException:
        # a database that doesn't exist has no tasks
        pass
