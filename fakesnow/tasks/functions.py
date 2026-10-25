"""What a task body can know about the graph run it is part of."""

from __future__ import annotations

import json
import re
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
_PATH_STEP = re.compile(r"""\[(\d+)\]|\.?"((?:[^"]|"")*)"|\.?([^.\["]+)|\['((?:[^']|'')*)'\]""")
# the column a top-level SELECT gets for the value given to SET_RETURN_VALUE, removed from its result
_RETURN_VALUE_COLUMN = "_fs_return_value"

# provisional formats, to be checked against live Snowflake (see "Live checks" in the plan)
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"  # rendered in UTC


def task_functions(expression: Expr, context: TaskRunContext | None) -> Expr:
    """Replace the SYSTEM$ functions that read, or set, the state of the task run the statement is part of. Outside
    a task run they return NULL."""
    if context and isinstance(expression, exp.Select) and expression.parent is None:
        call = next((c for c in expression.find_all(exp.Anonymous) if _name(c) == SET_RETURN_VALUE), None)
        if call and call.expressions:
            # a column of its own, so the value is captured wherever the call is in the statement
            value = exp.alias_(call.expressions[0].copy(), _RETURN_VALUE_COLUMN, quoted=True)
            expression.select(value, copy=False)
        return expression
    if not isinstance(expression, exp.Anonymous):
        return expression
    name = _name(expression)
    if not name.startswith(_PREFIX) or name[len(_PREFIX) :] not in _FUNCTIONS:
        return expression
    args = expression.expressions
    if name == SET_RETURN_VALUE:
        expression.root().set("set_return_value", True)
        # outside a task run there is nothing to set
        # a replacement isn't descended into by the transform, so replace the task functions nested in the value
        return args[0].copy().transform(lambda e: task_functions(e, context)) if context and args else _NULL.copy()
    value = _read(name[len(_PREFIX) :], args, context) if context else None
    return _NULL.copy() if value is None else exp.Literal.string(value)


def store_return_value(context: TaskRunContext | None, result: pa.Table) -> pa.Table:
    """Set the return value of the task run from the first row of the SET_RETURN_VALUE result, returning the result
    without the column added for it. Without that column, eg: in an INSERT, the first column is used."""
    if context is None:
        return result
    index = result.schema.get_field_index(_RETURN_VALUE_COLUMN)
    if result.num_rows:
        value = result.column(max(index, 0))[0].as_py()
        context.return_value = None if value is None else str(value)
    return result.remove_column(index) if index >= 0 else result


def _name(call: exp.Anonymous) -> str:
    return call.this.upper() if isinstance(call.this, str) else ""


def _read(function: str, args: list[Expr], context: TaskRunContext) -> str | None:
    arg = args[0].name if args else None
    if function == "CURRENT_USER_TASK_NAME":
        return context.task.fqname
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


def _config(path: str | None, config: str | None) -> str | None:
    if path is None or config is None:
        return config
    value = json.loads(config)
    pos = 0
    # a GET_PATH path: keys, which can be quoted, separated by periods, and [index] or ['key'] subscripts
    while pos < len(path):
        step = _PATH_STEP.match(path, pos)
        if step is None:
            return None
        pos = step.end()
        index, quoted, key, subscript = step.groups()
        if index is not None:
            value = value[int(index)] if isinstance(value, list) and int(index) < len(value) else None
        else:
            if key is not None:
                name = key
            elif quoted is not None:
                name = quoted.replace('""', '"')
            else:
                name = subscript.replace("''", "'")
            value = value.get(name) if isinstance(value, dict) else None
        if value is None:
            return None
    return value if isinstance(value, str) else json.dumps(value)
