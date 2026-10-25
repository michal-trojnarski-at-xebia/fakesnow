"""Snowflake tasks: the statements that manage them and the catalog that stores them."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import duckdb
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.tasks.catalog import TaskCatalog, status_sql
from fakesnow.tasks.functions import register_capture, store_return_value, task_functions
from fakesnow.tasks.history import task_table_functions
from fakesnow.tasks.runner import EXECUTED, run_graph
from fakesnow.tasks.show import describe_task_sql, show_tasks_sql
from fakesnow.tasks.statements import (
    AlterTask,
    CreateTask,
    DescribeTask,
    DropTask,
    EnableDependents,
    ExecuteTask,
    ShowTasks,
    TaskStatement,
    parse_task_name,
    parse_task_statement,
)
from fakesnow.transforms.transforms import SUCCESS_NOP

if TYPE_CHECKING:
    from fakesnow.conn import FakeSnowflakeConnection


_COMMANDS: dict[type, str] = {
    CreateTask: "CREATE TASK",
    AlterTask: "ALTER TASK",
    DropTask: "DROP TASK",
    ExecuteTask: "EXECUTE TASK",
    EnableDependents: "SYSTEM$TASK_DEPENDENTS_ENABLE",
    ShowTasks: "SHOW TASKS",
    DescribeTask: "DESCRIBE TASK",
}
_ENABLE_DEPENDENTS = "SYSTEM$TASK_DEPENDENTS_ENABLE"


__all__ = [
    "drop_schema",
    "execute",
    "register_capture",
    "store_return_value",
    "task_functions",
    "task_statement",
    "task_table_functions",
]


def task_statement(expression: Expr) -> Expr:
    """Replace a task statement, which sqlglot parses as an opaque command, with a placeholder carrying it."""
    statement = None
    if isinstance(expression, exp.Command):
        statement = parse_task_statement(expression.sql("snowflake"))
    elif isinstance(expression, exp.Select):
        statement = _enable_dependents(expression)
    elif isinstance(expression, exp.Anonymous) and expression.name.upper() == _ENABLE_DEPENDENTS:
        # a use other than SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('<name>'), which is replaced before it is reached
        raise NotImplementedError(f"{_ENABLE_DEPENDENTS} is only supported as the sole column of a SELECT")
    if statement:
        placeholder = SUCCESS_NOP.copy()
        placeholder.args["task_statement"] = statement
        return placeholder
    return expression


def _enable_dependents(select: exp.Select) -> EnableDependents | None:
    """The statement if the SELECT is only a SYSTEM$TASK_DEPENDENTS_ENABLE call with a string literal argument."""
    if {k for k, v in select.args.items() if v} != {"expressions"} or len(select.expressions) != 1:
        return None
    call = select.expressions[0]
    if not (isinstance(call, exp.Anonymous) and call.name.upper() == _ENABLE_DEPENDENTS):
        return None
    if len(call.expressions) != 1 or not (isinstance(arg := call.expressions[0], exp.Literal) and arg.is_string):
        return None
    return EnableDependents(parse_task_name(arg.name), arg.name)


def execute(statement: TaskStatement, conn: FakeSnowflakeConnection) -> str:
    """Run a task statement, returning the SELECT that produces its result rows."""
    catalog = TaskCatalog(
        conn._duck_conn,  # noqa: SLF001
        conn.database if conn.database_set else None,
        conn.schema if conn.schema_set else None,
        conn.role,
        _COMMANDS[type(statement)],
    )
    if isinstance(statement, CreateTask):
        return catalog.create(statement)
    if isinstance(statement, AlterTask):
        return catalog.alter(statement)
    if isinstance(statement, DropTask):
        return catalog.drop(statement)
    if isinstance(statement, EnableDependents):
        catalog.enable_dependents(statement.name)
        column = f"{_ENABLE_DEPENDENTS}('{statement.argument}')".replace('"', '""')
        return f'SELECT NULL AS "{column}"'
    if isinstance(statement, ExecuteTask):
        root = catalog.require(statement.name)
        run_graph(conn, catalog, root, statement.config)
        return status_sql(EXECUTED, root.name)
    if isinstance(statement, ShowTasks):
        return show_tasks_sql(statement, conn._duck_conn, catalog.current_database, catalog.current_schema)  # noqa: SLF001
    if isinstance(statement, DescribeTask):
        return describe_task_sql(catalog.require(statement.name))
    raise NotImplementedError(type(statement).__name__)


def drop_schema(duck_conn: DuckDBPyConnection, database: str, schema: str) -> None:
    """Delete the tasks of a dropped schema."""
    # a database that doesn't exist has no tasks
    with contextlib.suppress(duckdb.CatalogException):
        TaskCatalog(duck_conn, database, schema, "", "DROP SCHEMA").delete_schema(database, schema)
