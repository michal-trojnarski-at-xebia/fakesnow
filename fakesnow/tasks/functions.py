"""What a task body can know about the graph run it is part of."""

from __future__ import annotations

import json
import re
import uuid
import weakref
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import snowflake.connector.errors
from duckdb.func import SPECIAL
from sqlglot import Expr, exp

from fakesnow.tasks.catalog import Task

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


@dataclass
class TaskRunContext:
    """The run of one task in a graph run, set on the task's session while its body runs."""

    task: Task
    root: Task
    graph_run_group_id: str
    run_id: int
    config: str | None
    predecessor_return_values: dict[str, str | None]
    # every ancestor's, as a named lookup falls back to a matching ancestor
    ancestor_return_values: dict[str, str | None] = field(default_factory=dict)
    return_value: str | None = None
    # the values given to SET_RETURN_VALUE by the statement running, and the key its calls find them by
    set_values: list[str | None] = field(default_factory=list)
    key: str = field(default_factory=lambda: uuid.uuid4().hex)


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
# the function a SET_RETURN_VALUE call is replaced by, which records the value it's given for the task run
_CAPTURE = "_fs_capture_return_value"
# the task runs whose SET_RETURN_VALUE calls are being recorded, by key
_RUNS: weakref.WeakValueDictionary[str, TaskRunContext] = weakref.WeakValueDictionary()

# provisional formats, to be checked against live Snowflake (see "Live checks" in the plan)
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"  # rendered in UTC
_AMBIGUOUS_PREDECESSOR = (
    "SYSTEM$GET_PREDECESSOR_RETURN_VALUE needs a task name when the task has several enabled predecessors"
)
_UNMATCHED_PREDECESSOR = "SYSTEM$GET_PREDECESSOR_RETURN_VALUE: '{name}' is not an enabled predecessor of the task"
_PREDECESSOR_ERRNO = 100357


def task_functions(expression: Expr, context: TaskRunContext | None) -> Expr:
    """Replace the SYSTEM$ functions that read, or set, the state of the task run the statement is part of. Outside
    a task run they return NULL."""
    if not isinstance(expression, exp.Anonymous):
        return expression
    name = _name(expression)
    if not name.startswith(_PREFIX) or name[len(_PREFIX) :] not in _FUNCTIONS:
        return expression
    args = expression.expressions
    if name == SET_RETURN_VALUE:
        expression.root().set("set_return_value", True)
        if not context or not args:
            # outside a task run there is nothing to set
            return _NULL.copy()
        # a replacement isn't descended into by the transform, so replace the task functions nested in the value
        value = args[0].copy().transform(lambda e: task_functions(e, context))
        _RUNS[context.key] = context
        # the string value, recorded as it's evaluated wherever the call is in the statement, eg: in the SELECT of an
        # INSERT, and returned
        return exp.Anonymous(
            this=_CAPTURE, expressions=[exp.Literal.string(context.key), exp.cast(value, exp.DataType.Type.VARCHAR)]
        )
    value = _read(name[len(_PREFIX) :], args, context) if context else None
    return _NULL.copy() if value is None else exp.Literal.string(value)


def register_capture(duck_conn: DuckDBPyConnection) -> None:
    """Register the function SET_RETURN_VALUE calls are replaced by, in a new instance."""
    duck_conn.create_function(
        _CAPTURE, _capture, ["VARCHAR", "VARCHAR"], "VARCHAR", null_handling=SPECIAL, side_effects=True
    )


def _capture(key: str, value: str | None) -> str | None:
    if context := _RUNS.get(key):
        context.set_values.append(value)
    return value


def store_return_value(context: TaskRunContext | None) -> None:
    """Set the return value of the task run from the first value the statement gave SET_RETURN_VALUE."""
    if context and context.set_values:
        context.return_value = context.set_values[0]
        context.set_values.clear()


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
    if arg is None and len(values) > 1:
        # the name is required with several, so the call fails
        raise _predecessor_error(_AMBIGUOUS_PREDECESSOR)
    if arg is None:
        return next(iter(values.values()), None)
    # matched as given, as task names are case-sensitive here, falling back to a matching ancestor
    if arg in values:
        return values[arg]
    if arg in context.ancestor_return_values:
        return context.ancestor_return_values[arg]
    raise _predecessor_error(_UNMATCHED_PREDECESSOR.format(name=arg))


def _predecessor_error(message: str) -> snowflake.connector.errors.ProgrammingError:
    return snowflake.connector.errors.ProgrammingError(msg=message, errno=_PREDECESSOR_ERRNO, sqlstate="P0000")


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
