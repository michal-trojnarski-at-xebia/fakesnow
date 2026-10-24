"""The INFORMATION_SCHEMA table functions over task runs: TASK_HISTORY, COMPLETE_TASK_GRAPHS and TASK_DEPENDENTS."""

from __future__ import annotations

from collections.abc import Callable

import snowflake.connector.errors
import sqlglot
from sqlglot import Expr, exp

from fakesnow import checks
from fakesnow.expr import normalise_ident
from fakesnow.tasks.catalog import graph_runs_table, history_table, tasks_table
from fakesnow.tasks.show import rows_sql
from fakesnow.tasks.statements import parse_task_name

# provisional values, to be checked against live Snowflake
DEFAULT_RESULT_LIMIT = 100
GRAPH_VERSION = 1
ERROR_STATES = ("FAILED", "CANCELLED")

_TIME_RANGE_ARGS = {"SCHEDULED_TIME_RANGE_START", "SCHEDULED_TIME_RANGE_END"}
_START = "_fs_range_start"
_END = "_fs_range_end"

_HISTORY_COLUMNS = """
    query_id AS QUERY_ID,
    name AS NAME,
    {database} AS DATABASE_NAME,
    schema_name AS SCHEMA_NAME,
    query_text AS QUERY_TEXT,
    condition_text AS CONDITION_TEXT,
    state AS STATE,
    error_code AS ERROR_CODE,
    error_message AS ERROR_MESSAGE,
    scheduled_time AS SCHEDULED_TIME,
    query_start_time AS QUERY_START_TIME,
    NULL::TIMESTAMPTZ AS NEXT_SCHEDULED_TIME,
    completed_time AS COMPLETED_TIME,
    root_task_id AS ROOT_TASK_ID,
    {graph_version} AS GRAPH_VERSION,
    run_id AS RUN_ID,
    return_value AS RETURN_VALUE,
    scheduled_from AS SCHEDULED_FROM,
    attempt_number AS ATTEMPT_NUMBER,
    config AS CONFIG,
    NULL::VARCHAR AS QUERY_HASH,
    NULL::INTEGER AS QUERY_HASH_VERSION,
    NULL::VARCHAR AS QUERY_PARAMETERIZED_HASH,
    NULL::INTEGER AS QUERY_PARAMETERIZED_HASH_VERSION,
    graph_run_group_id AS GRAPH_RUN_GROUP_ID,
    NULL::VARCHAR AS BACKFILL_INFO,
    NULL::VARCHAR AS SPCS_JOB_ID"""

_GRAPH_COLUMNS = """
    root_task_name AS ROOT_TASK_NAME,
    {database} AS DATABASE_NAME,
    schema_name AS SCHEMA_NAME,
    state AS STATE,
    scheduled_from AS SCHEDULED_FROM,
    first_error_task_name AS FIRST_ERROR_TASK_NAME,
    first_error_code AS FIRST_ERROR_CODE,
    first_error_message AS FIRST_ERROR_MESSAGE,
    scheduled_time AS SCHEDULED_TIME,
    query_start_time AS QUERY_START_TIME,
    NULL::TIMESTAMPTZ AS NEXT_SCHEDULED_TIME,
    completed_time AS COMPLETED_TIME,
    root_task_id AS ROOT_TASK_ID,
    {graph_version} AS GRAPH_VERSION,
    run_id AS RUN_ID,
    attempt_number AS ATTEMPT_NUMBER,
    config AS CONFIG,
    graph_run_group_id AS GRAPH_RUN_GROUP_ID,
    NULL::VARCHAR AS BACKFILL_INFO"""


def _literal(value: object) -> str:
    return exp.Literal.string(str(value)).sql("duckdb")


class _Args:
    """The named arguments of a table function call. Their values must be literals."""

    def __init__(self, function: str, call: exp.Anonymous, allowed: set[str]) -> None:
        self.values: dict[str, object] = {}
        # the time range bounds, which can be any scalar expression
        self.expressions: dict[str, Expr] = {}
        for arg in call.expressions:
            if not isinstance(arg, exp.Kwarg):
                raise NotImplementedError(f"{function} only supports named arguments")
            name = arg.this.name.upper()
            if name not in allowed:
                raise NotImplementedError(f"{function} doesn't support the argument {name}")
            value = arg.expression
            if name in _TIME_RANGE_ARGS:
                if not isinstance(value, exp.Null):
                    self.expressions[name] = value
                continue
            if not isinstance(value, (exp.Literal, exp.Boolean, exp.Null)):
                raise NotImplementedError(f"{function} only supports literal values for the argument {name}")
            if isinstance(value, exp.Boolean):
                self.values[name] = value.this
            elif isinstance(value, exp.Null):
                self.values[name] = None
            else:
                self.values[name] = int(value.this) if not value.is_string and value.is_int else value.this

    def get(self, name: str) -> str | None:
        value = self.values.get(name)
        return None if value is None else str(value)

    def flag(self, name: str, default: bool = False) -> bool:
        """A boolean argument, which can also be given as the string 'true' or 'false'."""
        value = self.values.get(name)
        if value is None:
            return default
        if isinstance(value, str) and value.lower() in ("true", "false"):
            return value.lower() == "true"
        if isinstance(value, bool):
            return value
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"Invalid value [{value}] for argument {name}: expected a boolean.", errno=2023, sqlstate="22023"
        )

    def limit(self) -> int:
        value = self.values.get("RESULT_LIMIT")
        return DEFAULT_RESULT_LIMIT if value is None else int(str(value))


def _filters(args: _Args, database: str, columns: dict[str, str]) -> list[str]:
    """The conditions for the arguments shared by the history functions, given the source column of each."""
    filters = []
    for arg, column in columns.items():
        if (value := args.get(arg)) is not None:
            filters.append(f"upper({column}) = upper({_literal(value)})")
    if (value := args.get("DATABASE_NAME")) is not None and value.upper() != database.upper():
        filters.append("false")
    if args.flag("ERROR_ONLY"):
        filters.append("state in ({})".format(", ".join(_literal(s) for s in ERROR_STATES)))
    # the expressions of the time range are spliced in by _select
    if "SCHEDULED_TIME_RANGE_START" in args.expressions:
        filters.append(f"scheduled_time >= {_START}::TIMESTAMPTZ")
    if "SCHEDULED_TIME_RANGE_END" in args.expressions:
        filters.append(f"scheduled_time <= {_END}::TIMESTAMPTZ")
    if (value := args.get("ROOT_TASK_ID")) is not None:
        filters.append(f"root_task_id = {_literal(value)}")
    return filters


def _select(columns: str, table: str, filters: list[str], args: _Args) -> exp.Expr:
    where = f" where {' and '.join(filters)}" if filters else ""
    # the newest rows are kept by the limit, and returned newest first
    select = sqlglot.parse_one(
        f"select {columns} from {table}{where} order by scheduled_time desc, completed_time desc limit {args.limit()}",
        read="duckdb",
    )
    # splice in the Snowflake expressions, so the transforms that follow translate them like any other
    for placeholder, name in ((_START, "SCHEDULED_TIME_RANGE_START"), (_END, "SCHEDULED_TIME_RANGE_END")):
        if name in args.expressions:
            for column in list(select.find_all(exp.Column)):
                if column.name == placeholder:
                    column.replace(exp.Paren(this=args.expressions[name].copy()))
    return select


def _task_history(call: exp.Anonymous, database: str, current_schema: str | None) -> exp.Expr:
    args = _Args(
        "TASK_HISTORY",
        call,
        {
            "SCHEDULED_TIME_RANGE_START",
            "SCHEDULED_TIME_RANGE_END",
            "RESULT_LIMIT",
            "TASK_NAME",
            "DATABASE_NAME",
            "SCHEMA_NAME",
            "ERROR_ONLY",
            "ROOT_TASK_ID",
        },
    )
    filters = _filters(args, database, {"TASK_NAME": "name", "SCHEMA_NAME": "schema_name"})
    columns = _HISTORY_COLUMNS.format(database=_literal(database), graph_version=GRAPH_VERSION)
    return _select(columns, history_table(database), filters, args)


def _complete_task_graphs(call: exp.Anonymous, database: str, current_schema: str | None) -> exp.Expr:
    args = _Args("COMPLETE_TASK_GRAPHS", call, {"RESULT_LIMIT", "ROOT_TASK_NAME", "ERROR_ONLY"})
    filters = _filters(args, database, {"ROOT_TASK_NAME": "root_task_name"})
    columns = _GRAPH_COLUMNS.format(database=_literal(database), graph_version=GRAPH_VERSION)
    return _select(columns, graph_runs_table(database), filters, args)


def _task_dependents(call: exp.Anonymous, database: str, current_schema: str | None) -> exp.Expr:
    args = _Args("TASK_DEPENDENTS", call, {"TASK_NAME", "RECURSIVE"})
    task_name = args.get("TASK_NAME")
    if task_name is None:
        raise NotImplementedError("TASK_DEPENDENTS needs the TASK_NAME argument")
    name = parse_task_name(task_name)
    schema = name.schema or current_schema
    recursive = args.flag("RECURSIVE", default=True)
    start = f"name = {_literal(name.name)}" + (f" and schema_name = {_literal(schema)}" if schema else "")
    # the finalizer of a task is one of its dependents
    child = "(list_contains(t.predecessors, d.name) or t.finalize = d.name)"
    depth = "" if recursive else " and d.depth < 1"
    tasks = tasks_table(name.database or database)
    return sqlglot.parse_one(
        f"""
    with recursive deps(schema_name, name, depth) as (
        select schema_name, name, 0 from {tasks} where {start}
        union all
        select t.schema_name, t.name, d.depth + 1 from {tasks} t
        join deps d on t.schema_name = d.schema_name and {child}{depth}
    )
    select r.* from ({rows_sql(name.database or database)}) r
    where (r.schema_name, r.name) in (select schema_name, name from deps)
    order by r.schema_name, r.name""",
        read="duckdb",
    )


_FUNCTIONS: dict[str, Callable[[exp.Anonymous, str, str | None], exp.Expr]] = {
    "TASK_HISTORY": _task_history,
    "COMPLETE_TASK_GRAPHS": _complete_task_graphs,
    "TASK_DEPENDENTS": _task_dependents,
}


def task_table_functions(expression: Expr, current_database: str | None, current_schema: str | None = None) -> Expr:
    """Replace TABLE([<db>.]INFORMATION_SCHEMA.<function>(...)) for a task function by the subquery that produces
    its rows, keeping any alias. The database defaults to the current database."""
    if not (isinstance(expression, exp.TableFromRows) and isinstance(expression.this, exp.Dot)):
        return expression
    dot = expression.this
    call = dot.expression
    qualifier = dot.this
    if not (isinstance(call, exp.Anonymous) and isinstance(call.this, str) and call.this.upper() in _FUNCTIONS):
        return expression
    database: str | None = None
    if isinstance(qualifier, exp.Dot) and isinstance(qualifier.this, exp.Identifier):
        database = _ident(qualifier.this)
        qualifier = qualifier.expression
    if not (isinstance(qualifier, exp.Identifier) and _ident(qualifier) == "INFORMATION_SCHEMA"):
        return expression
    function = call.this.upper()
    database = database or current_database
    if not database:
        raise checks.no_current_database_error(function)
    select = _FUNCTIONS[function](call, database, current_schema)
    if function == "TASK_DEPENDENTS":
        # keep the lower case column names of SHOW TASKS, which the upper casing of identifiers would change
        for alias in select.find_all(exp.Alias):
            alias.args["alias"].set("quoted", True)
    return exp.Subquery(this=select, alias=expression.args.get("alias"))


def _ident(ident: exp.Identifier) -> str:
    return normalise_ident(ident.sql("snowflake"))
