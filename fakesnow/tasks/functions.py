"""What a task body can know about the graph run it is part of."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlglot import Expr, exp

from fakesnow.tasks.catalog import Task
from fakesnow.tasks.statements import parse_task_name

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass
class TaskRunContext:
    """The run of one task in a graph run, set on the task's session while its body runs."""

    task: Task
    root: Task
    graph_run_group_id: str
    run_id: int
    config: str | None
    predecessor_return_values: dict[str, str | None]
    return_value: str | None = None


_PREFIX = "SYSTEM$"
SET_RETURN_VALUE = "SYSTEM$SET_RETURN_VALUE"
_FUNCTIONS = {
    "CURRENT_USER_TASK_NAME",
    "TASK_RUNTIME_INFO",
    "GET_TASK_GRAPH_CONFIG",
    "GET_PREDECESSOR_RETURN_VALUE",
    "SET_RETURN_VALUE",
}
_NULL = exp.Null()

# provisional formats, to be checked against live Snowflake (see "Live checks" in the plan)
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"  # rendered in UTC


def task_functions(expression: Expr, context: TaskRunContext | None) -> Expr:
    """Replace the SYSTEM$ functions that read, or set, the state of the task run the statement is part of. Outside
    a task run they return NULL."""
    if not (isinstance(expression, exp.Anonymous) and isinstance(expression.this, str)):
        return expression
    name = expression.this.upper()
    if not name.startswith(_PREFIX) or name[len(_PREFIX) :] not in _FUNCTIONS:
        return expression
    args = expression.expressions
    if name == SET_RETURN_VALUE:
        expression.root().set("set_return_value", True)
        # outside a task run there is nothing to set
        return args[0].copy() if context and args else _NULL.copy()
    value = _read(name[len(_PREFIX) :], args, context) if context else None
    return _NULL.copy() if value is None else exp.Literal.string(value)


def store_return_value(context: TaskRunContext | None, result: pa.Table) -> None:
    """Set the return value of the task run, as the first column of the first row of the SET_RETURN_VALUE result."""
    if context is None or not result.num_rows:
        return
    value = result.column(0)[0].as_py()
    context.return_value = None if value is None else str(value)


def _read(function: str, args: list[Expr], context: TaskRunContext) -> str | None:
    arg = args[0].name if args else None
    if function == "CURRENT_USER_TASK_NAME":
        # the bare name, where TASK_RUNTIME_INFO gives the fqname
        return context.task.name
    if function == "TASK_RUNTIME_INFO":
        return _runtime_info(arg, context)
    if function == "GET_TASK_GRAPH_CONFIG":
        return _config(arg, context.config)
    # GET_PREDECESSOR_RETURN_VALUE, with no argument the only predecessor's
    values = context.predecessor_return_values
    if arg is None:
        return next(iter(values.values())) if len(values) == 1 else None  # ambiguous with several
    return values.get(parse_task_name(arg).name)


def _runtime_info(field: str | None, context: TaskRunContext) -> str | None:
    match (field or "").upper():
        case "CURRENT_TASK_NAME":
            return context.task.fqname
        case "CURRENT_ROOT_TASK_NAME":
            return context.root.fqname
        case "CURRENT_ROOT_TASK_UUID":
            return context.root.id
        case "CURRENT_TASK_GRAPH_RUN_GROUP_ID":
            return context.graph_run_group_id
        case "CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP":
            # the run id is the root's scheduled time in epoch milliseconds
            return datetime.fromtimestamp(context.run_id / 1000, UTC).strftime(_TIMESTAMP_FORMAT)
        case _:
            return None


def _config(key: str | None, config: str | None) -> str | None:
    if key is None or config is None:
        return config
    value = json.loads(config).get(key)
    return value if value is None or isinstance(value, str) else json.dumps(value)
