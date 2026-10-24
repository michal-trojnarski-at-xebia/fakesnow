"""The task catalog, stored per database in ${catalog}._fs_information_schema._fs_tasks."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

import snowflake.connector.errors
from duckdb import DuckDBPyConnection

from fakesnow import checks
from fakesnow.tasks.statements import AlterTask, CreateTask, DropTask, TaskName
from fakesnow.transforms.temporary import check_schema_exists

# provisional messages and values, to be checked against live Snowflake
CREATED = "Task {name} successfully created."
EXISTS = "{name} already exists, statement succeeded."
DROPPED = "{name} successfully dropped."
ALTERED = "Statement executed successfully."
DROP_MISSING = "Drop statement executed successfully ({name} already dropped)."
ALREADY_EXISTS = "SQL compilation error:\nObject '{fqname}' already exists."
DOES_NOT_EXIST = "SQL compilation error:\nTask '{fqname}' does not exist or not authorized."
INVALID_PROPERTY = "SQL compilation error:\ninvalid property '{prop}' for 'TASK'"

STARTED_ROOT = "Unable to update graph with root task {root} since that root task is not suspended."
OTHER_SCHEMA_PREDECESSOR = "SQL compilation error:\nTask {fqname} cannot depend on task {pred} in a different schema."
SCHEDULED_CHILD = "SQL compilation error:\nTask {fqname} with predecessors or a finalized root cannot have a schedule."
INVALID_FINALIZER = "SQL compilation error:\nInvalid finalizer task {fqname}: {reason}."
FINALIZER_HAS_PREDECESSORS = "a finalizer cannot have predecessors"
FINALIZER_HAS_CHILDREN = "a finalizer cannot have child tasks"
FINALIZER_EXISTS = "root task {root} already has a finalizer"
FINALIZER_NOT_ROOT = "{root} is not a root task"
FINALIZER_SELF = "a task cannot finalize itself"
MIXED_GRAPHS = "SQL compilation error:\nTask {fqname} cannot have predecessors in different task graphs."
CYCLE = "SQL compilation error:\nTask graph cycle detected involving task {fqname}."
LIMIT_EXCEEDED = "SQL compilation error:\nTask {fqname} exceeds the maximum of {limit} {what}."

MAX_PREDECESSORS = 100
MAX_CHILDREN = 100
MAX_GRAPH_TASKS = 1000

SUSPENDED = "suspended"
STARTED = "started"
CHILD_BECAME_ROOT = "CHILD_BECAME_ROOT"
FINALIZER_BECAME_ROOT = "FINALIZER_BECAME_ROOT"
USER_SUSPENDED = "USER_SUSPENDED"

# task properties with a column of their own, by property name
COLUMN_PROPERTIES = {
    "WAREHOUSE": "warehouse",
    "SCHEDULE": "schedule",
    "CONFIG": "config",
    "OVERLAP_POLICY": "overlap_policy",
    "ALLOW_OVERLAPPING_EXECUTION": "allow_overlapping_execution",
    "ERROR_INTEGRATION": "error_integration",
    "SUCCESS_INTEGRATION": "success_integration",
    "COMMENT": "comment",
    "FINALIZE": "finalize",
}
# task properties kept in the properties JSON column
OTHER_PROPERTIES = frozenset(
    {
        "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE",
        "USER_TASK_TIMEOUT_MS",
        "SUSPEND_TASK_AFTER_NUM_FAILURES",
        "TASK_AUTO_RETRY_ATTEMPTS",
        "TARGET_COMPLETION_INTERVAL",
        "USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS",
        "SERVERLESS_TASK_MIN_STATEMENT_SIZE",
        "SERVERLESS_TASK_MAX_STATEMENT_SIZE",
        "LOG_LEVEL",
        "TRACE_LEVEL",
    }
)
SESSION_PARAMETERS = frozenset(
    {
        "QUERY_TAG",
        "TIMEZONE",
        "AUTOCOMMIT",
        "QUOTED_IDENTIFIERS_IGNORE_CASE",
        "DATE_INPUT_FORMAT",
        "DATE_OUTPUT_FORMAT",
        "TIMESTAMP_INPUT_FORMAT",
        "TIMESTAMP_OUTPUT_FORMAT",
        "TIMESTAMP_TYPE_MAPPING",
        "STATEMENT_TIMEOUT_IN_SECONDS",
        "LOCK_TIMEOUT",
        "WEEK_START",
        "BINARY_INPUT_FORMAT",
        "JSON_INDENT",
        "ROWS_PER_RESULTSET",
    }
)

_COLUMNS = (
    "created_on, name, id, schema_name, owner, comment, warehouse, schedule, predecessors, state, definition, "
    "condition, finalize, config, error_integration, success_integration, overlap_policy, "
    "allow_overlapping_execution, properties, session_parameters, last_committed_on, last_suspended_on, "
    "last_suspended_reason"
)


def _error(msg: str, errno: int, sqlstate: str) -> snowflake.connector.errors.ProgrammingError:
    return snowflake.connector.errors.ProgrammingError(msg=msg, errno=errno, sqlstate=sqlstate)


@dataclass
class Task:
    database: str
    schema_name: str
    name: str
    created_on: datetime
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    owner: str = "PUBLIC"
    comment: str | None = None
    warehouse: str | None = None
    schedule: str | None = None
    predecessors: list[str] = field(default_factory=list)
    state: str = SUSPENDED
    definition: str | None = None
    condition: str | None = None
    finalize: str | None = None
    config: str | None = None
    error_integration: str | None = None
    success_integration: str | None = None
    overlap_policy: str | None = None
    allow_overlapping_execution: bool | None = None
    properties: dict[str, str] = field(default_factory=dict)
    session_parameters: dict[str, str] = field(default_factory=dict)
    last_committed_on: datetime | None = None
    last_suspended_on: datetime | None = None
    last_suspended_reason: str | None = None

    @property
    def fqname(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.name}"


class TaskCatalog:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        current_database: str | None,
        current_schema: str | None,
        role: str,
        command: str,
    ) -> None:
        self.duck_conn = duck_conn
        self.current_database = current_database
        self.current_schema = current_schema
        self.role = role
        self.command = command  # the statement being run, eg: CREATE TASK, for errors

    def resolve(self, name: TaskName) -> tuple[str, str, str]:
        """Qualify a task name with the session's database and schema."""
        database = name.database or self.current_database
        if not database:
            raise checks.no_current_database_error(self.command)
        schema = name.schema or self.current_schema
        if not schema:
            raise checks.no_current_schema_error(self.command)
        return database, schema, name.name

    def get(self, database: str, schema: str, name: str) -> Task | None:
        # a separate cursor, so reads see what save committed without joining the caller's transaction
        with self.duck_conn.cursor() as conn:
            row = conn.execute(
                f"select {_COLUMNS} from {tasks_table(database)} where schema_name = ? and name = ?", [schema, name]
            ).fetchone()
        return _task(database, row) if row else None

    def require(self, name: TaskName) -> Task:
        database, schema, task_name = self.resolve(name)
        task = self.get(database, schema, task_name)
        if task is None:
            raise _error(DOES_NOT_EXIST.format(fqname=f"{database}.{schema}.{task_name}"), 2003, "02000")
        return task

    def root_of(self, task: Task) -> Task:
        """The root of the graph the task belongs to: the task itself if it has no predecessors or finalized root."""
        tasks = {t.name: t for t in self._schema_tasks(task.database, task.schema_name)}
        current = task
        seen = {task.name}
        while True:
            parent_name = current.finalize or (current.predecessors[0] if current.predecessors else None)
            parent = tasks.get(parent_name) if parent_name else None
            if parent is None or parent.name in seen:
                return current
            seen.add(parent.name)
            current = parent

    def children(self, task: Task) -> list[Task]:
        return [t for t in self._schema_tasks(task.database, task.schema_name) if task.name in t.predecessors]

    def finalizer_of(self, root: Task) -> Task | None:
        for t in self._schema_tasks(root.database, root.schema_name):
            if t.finalize == root.name and t.name != root.name:
                return t
        return None

    def graph(self, root: Task) -> list[Task]:
        """The root, its descendants in an order where predecessors come first, then its finalizer."""
        tasks = self._schema_tasks(root.database, root.schema_name)
        members = {root.name}
        pending = [t for t in tasks if t.predecessors]
        grew = True
        while grew:
            grew = False
            for t in pending:
                if t.name not in members and members.intersection(t.predecessors):
                    members.add(t.name)
                    grew = True
        remaining = [t for t in tasks if t.name in members and t.name != root.name]
        ordered = [root]
        placed = {root.name}
        while remaining:
            ready = [t for t in remaining if all(p in placed or p not in members for p in t.predecessors)]
            if not ready:  # a cycle, which the rules prevent
                ready = remaining
            ordered.extend(ready)
            placed.update(t.name for t in ready)
            remaining = [t for t in remaining if t not in ready]
        finalizer = self.finalizer_of(root)
        if finalizer:
            ordered.append(finalizer)
        return ordered

    def check_root_suspended(self, task: Task) -> None:
        """Raise if the root of the task's graph, which may be the task itself, is started."""
        root = self.root_of(task)
        if root.state == STARTED:
            raise _error(STARTED_ROOT.format(root=root.fqname), 91089, "02000")

    def create(self, stmt: CreateTask) -> str:
        database, schema, name = self.resolve(stmt.name)
        with self.duck_conn.cursor() as conn:
            check_schema_exists(conn, database, schema)
        task = self._new(database, schema, name, stmt)
        existing = self.get(database, schema, name)
        if existing and not (stmt.replace or stmt.or_alter):
            if stmt.if_not_exists:
                return _status(EXISTS, name)
            raise _error(ALREADY_EXISTS.format(fqname=existing.fqname), 2002, "42710")
        if existing and not stmt.replace:
            # altering keeps the identity, state and history, and resets what the statement leaves out
            task = replace(
                task,
                id=existing.id,
                created_on=existing.created_on,
                state=existing.state,
                last_committed_on=existing.last_committed_on,
                last_suspended_on=existing.last_suspended_on,
                last_suspended_reason=existing.last_suspended_reason,
            )
        after = (
            [self.resolve(a) for a in stmt.after]
            if not stmt.clone_of
            else [(database, schema, p) for p in task.predecessors]
        )
        self._store(task, existing, after)
        return _status(CREATED, name)

    def alter(self, stmt: AlterTask) -> str:
        database, schema, name = self.resolve(stmt.name)
        task = self.get(database, schema, name)
        if task is None:
            if stmt.if_exists:
                return _status(ALTERED, name)
            raise _error(DOES_NOT_EXIST.format(fqname=f"{database}.{schema}.{name}"), 2003, "02000")
        if stmt.action in ("RESUME", "SUSPEND"):
            # a root can always be resumed or suspended, a child only while its root is suspended
            if self.root_of(task).name != task.name:
                self.check_root_suspended(task)
            self.save(_resumed(task) if stmt.action == "RESUME" else _suspended(task, USER_SUSPENDED))
            return _status(ALTERED, name)

        new = replace(task, predecessors=list(task.predecessors), properties=dict(task.properties))
        new.session_parameters = dict(task.session_parameters)
        after = [(database, schema, p) for p in task.predecessors]
        if stmt.action == "ADD_AFTER":
            # keep the resolved names, so a predecessor in another schema is rejected as it is by CREATE
            for pred in (self.resolve(p) for p in stmt.tasks):
                if pred not in after:
                    after.append(pred)
                    new.predecessors.append(pred[2])
        elif stmt.action == "REMOVE_AFTER":
            removed = {r[2] for r in map(self.resolve, stmt.tasks) if r[:2] == (database, schema)}
            new.predecessors = [p for p in new.predecessors if p not in removed]
            if task.predecessors and not new.predecessors:
                new = _suspended(new, CHILD_BECAME_ROOT)
        elif stmt.action == "SET":
            for prop, value in stmt.properties.items():
                _set_property(new, prop, value)
        elif stmt.action == "UNSET":
            for prop in stmt.unset:
                _unset_property(new, prop)
            if task.finalize and not new.finalize:
                new = _suspended(new, FINALIZER_BECAME_ROOT)
        elif stmt.action == "MODIFY_AS":
            new.definition = stmt.text
        elif stmt.action == "MODIFY_WHEN":
            new.condition = stmt.text
        elif stmt.action == "REMOVE_WHEN":
            new.condition = None
        if stmt.action != "ADD_AFTER":
            after = [(database, schema, p) for p in new.predecessors]
        self._store(new, task, after)
        return _status(ALTERED, name)

    def enable_dependents(self, name: TaskName) -> None:
        """Resume the task and all its dependents."""
        task = self.require(name)
        if self.root_of(task).name != task.name:
            self.check_root_suspended(task)
        for member in self.graph(task):
            self.save(_resumed(member))

    def _store(self, task: Task, existing: Task | None, after: list[tuple[str, str, str]]) -> None:
        """Check the graph rules and save a new or changed task. A task already in a started graph can't change,
        nor can one join a started graph."""
        if existing:
            self.check_root_suspended(existing)
        self._check_graph(task, after)
        self.save(task)

    def _check_graph(self, task: Task, after: list[tuple[str, str, str]]) -> None:
        """Check the rules for a task about to be saved, in the order Snowflake reports them."""
        preds: list[Task] = []
        for pred_db, pred_schema, pred_name in after:
            pred = self.get(pred_db, pred_schema, pred_name)
            if pred is None:
                raise _error(DOES_NOT_EXIST.format(fqname=f"{pred_db}.{pred_schema}.{pred_name}"), 2003, "02000")
            if (pred.database, pred.schema_name) != (task.database, task.schema_name):
                raise _error(OTHER_SCHEMA_PREDECESSOR.format(fqname=task.fqname, pred=pred.fqname), 2003, "02000")
            if pred.finalize:
                raise _error(INVALID_FINALIZER.format(fqname=pred.fqname, reason=FINALIZER_HAS_CHILDREN), 2003, "02000")
            preds.append(pred)
        if task.finalize == task.name:
            raise _error(INVALID_FINALIZER.format(fqname=task.fqname, reason=FINALIZER_SELF), 2003, "02000")
        finalized = self.get(task.database, task.schema_name, task.finalize) if task.finalize else None
        if task.finalize and finalized is None:
            raise _error(
                DOES_NOT_EXIST.format(fqname=f"{task.database}.{task.schema_name}.{task.finalize}"), 2003, "02000"
            )
        for member in [*preds, finalized] if finalized else preds or [task]:
            self.check_root_suspended(member)
        if len({self.root_of(pred).fqname for pred in preds}) > 1:
            raise _error(MIXED_GRAPHS.format(fqname=task.fqname), 2003, "02000")
        if (task.predecessors or task.finalize) and task.schedule:
            raise _error(SCHEDULED_CHILD.format(fqname=task.fqname), 2003, "02000")
        if finalized:
            reason = None
            existing_finalizer = self.finalizer_of(finalized)
            if finalized.predecessors or finalized.finalize:
                reason = FINALIZER_NOT_ROOT.format(root=finalized.fqname)
            elif task.predecessors:
                reason = FINALIZER_HAS_PREDECESSORS
            elif self.children(task):
                reason = FINALIZER_HAS_CHILDREN
            elif existing_finalizer and existing_finalizer.name != task.name:
                reason = FINALIZER_EXISTS.format(root=finalized.fqname)
            if reason:
                raise _error(INVALID_FINALIZER.format(fqname=task.fqname, reason=reason), 2003, "02000")
        if self._has_cycle(task):
            raise _error(CYCLE.format(fqname=task.fqname), 2003, "02000")
        self._check_limits(task)

    def _has_cycle(self, task: Task) -> bool:
        tasks = {t.name: t for t in self._schema_tasks(task.database, task.schema_name)}
        stack = list(task.predecessors)
        seen: set[str] = set()
        while stack:
            name = stack.pop()
            if name == task.name:
                return True
            if name not in seen and name in tasks:
                seen.add(name)
                stack.extend(tasks[name].predecessors)
        return False

    def _check_limits(self, task: Task) -> None:
        limits = [(len(task.predecessors), MAX_PREDECESSORS, "predecessors", task)]
        for pred_name in task.predecessors:
            pred = self.get(task.database, task.schema_name, pred_name)
            assert pred
            siblings = {c.name for c in self.children(pred)} | {task.name}
            limits.append((len(siblings), MAX_CHILDREN, "child tasks", pred))
        graph = self.graph(self.root_of(task))
        limits.append((len({t.name for t in graph} | {task.name}), MAX_GRAPH_TASKS, "tasks in a graph", task))
        for count, limit, what, subject in limits:
            if count > limit:
                raise _error(LIMIT_EXCEEDED.format(fqname=subject.fqname, limit=limit, what=what), 2003, "02000")

    def drop(self, stmt: DropTask) -> str:
        database, schema, name = self.resolve(stmt.name)
        task = self.get(database, schema, name)
        if task is None:
            if stmt.if_exists:
                return _status(DROP_MISSING, name)
            raise _error(DOES_NOT_EXIST.format(fqname=f"{database}.{schema}.{name}"), 2003, "02000")
        self.check_root_suspended(task)
        # dependents of the dropped task become standalone
        for child in self.children(task):
            child.predecessors.remove(name)
            self.save(child if child.predecessors else _suspended(child, CHILD_BECAME_ROOT))
        finalizer = self.finalizer_of(task)
        if finalizer:
            self.save(_suspended(replace(finalizer, finalize=None), FINALIZER_BECAME_ROOT))
        self.delete(database, schema, name)
        return _status(DROPPED, name)

    def delete(self, database: str, schema: str, name: str) -> None:
        with self.duck_conn.cursor() as conn:
            conn.execute(f"delete from {tasks_table(database)} where schema_name = ? and name = ?", [schema, name])

    def delete_schema(self, database: str, schema: str) -> None:
        """Delete every task in a schema."""
        with self.duck_conn.cursor() as conn:
            conn.execute(f"delete from {tasks_table(database)} where schema_name = ?", [schema])

    def save(self, task: Task) -> None:
        """Insert or replace the task. Writes go through a separate cursor, so they neither commit nor join the
        caller's transaction."""
        values: list[Any] = [
            task.created_on,
            task.name,
            task.id,
            task.schema_name,
            task.owner,
            task.comment,
            task.warehouse,
            task.schedule,
            task.predecessors,
            task.state,
            task.definition,
            task.condition,
            task.finalize,
            task.config,
            task.error_integration,
            task.success_integration,
            task.overlap_policy,
            task.allow_overlapping_execution,
            json.dumps(task.properties),
            json.dumps(task.session_parameters),
            task.last_committed_on,
            task.last_suspended_on,
            task.last_suspended_reason,
        ]
        with self.duck_conn.cursor() as conn:
            conn.execute(
                f"insert or replace into {tasks_table(task.database)} ({_COLUMNS}) "
                f"values ({', '.join('?' * len(values))})",
                values,
            )

    def _schema_tasks(self, database: str, schema: str) -> list[Task]:
        with self.duck_conn.cursor() as conn:
            rows = conn.execute(
                f"select {_COLUMNS} from {tasks_table(database)} where schema_name = ? order by created_on, name",
                [schema],
            ).fetchall()
        return [_task(database, row) for row in rows]

    def _new(self, database: str, schema: str, name: str, stmt: CreateTask) -> Task:
        if stmt.clone_of:
            source = self.require(stmt.clone_of)
            # everything but the identity, state and run history is copied
            return replace(
                source,
                database=database,
                schema_name=schema,
                name=name,
                id=str(uuid.uuid4()),
                created_on=_now(),
                owner=self.role,
                state=SUSPENDED,
                last_committed_on=None,
                last_suspended_on=None,
                last_suspended_reason=None,
            )

        task = Task(
            database=database,
            schema_name=schema,
            name=name,
            created_on=_now(),
            owner=self.role,
            predecessors=[after.name for after in stmt.after],
            condition=stmt.condition,
            definition=stmt.definition,
        )
        for prop, value in stmt.properties.items():
            _set_property(task, prop, value)
        return task


def _set_property(task: Task, prop: str, value: str) -> None:
    if prop in COLUMN_PROPERTIES:
        column = COLUMN_PROPERTIES[prop]
        if column == "allow_overlapping_execution":
            task.allow_overlapping_execution = value.upper() == "TRUE"
        elif column == "finalize":
            task.finalize = value.rsplit(".", 1)[-1]
        else:
            setattr(task, column, value)
    elif prop in OTHER_PROPERTIES:
        task.properties[prop] = value
    elif prop in SESSION_PARAMETERS and not (prop == "AUTOCOMMIT" and value.upper() == "FALSE"):
        task.session_parameters[prop] = value
    else:
        raise _error(INVALID_PROPERTY.format(prop=prop), 1008, "22023")


def _unset_property(task: Task, prop: str) -> None:
    if prop in COLUMN_PROPERTIES:
        setattr(task, COLUMN_PROPERTIES[prop], None)
    elif prop in OTHER_PROPERTIES:
        task.properties.pop(prop, None)
    elif prop in SESSION_PARAMETERS:
        task.session_parameters.pop(prop, None)
    else:
        raise _error(INVALID_PROPERTY.format(prop=prop), 1008, "22023")


def _resumed(task: Task) -> Task:
    return replace(task, state=STARTED, last_committed_on=task.last_committed_on or _now())


def _suspended(task: Task, reason: str) -> Task:
    return replace(task, state=SUSPENDED, last_suspended_on=_now(), last_suspended_reason=reason)


def _now() -> datetime:
    return datetime.now().astimezone()


def tasks_table(database: str) -> str:
    return '"{}"._fs_information_schema._fs_tasks'.format(database.replace('"', '""'))


def _status(template: str, name: str) -> str:
    return "SELECT '{}' AS status".format(template.format(name=name).replace("'", "''"))


def _task(database: str, row: tuple) -> Task:
    (created_on, name, id_, schema, owner, comment, warehouse, schedule, predecessors, state, definition) = row[:11]
    (condition, finalize, config, error_integration, success_integration, overlap_policy) = row[11:17]
    (allow_overlap, properties, session_parameters, committed, suspended, reason) = row[17:23]
    return Task(
        database=database,
        schema_name=schema,
        name=name,
        created_on=created_on,
        id=id_,
        owner=owner,
        comment=comment,
        warehouse=warehouse,
        schedule=schedule,
        predecessors=list(predecessors or []),
        state=state,
        definition=definition,
        condition=condition,
        finalize=finalize,
        config=config,
        error_integration=error_integration,
        success_integration=success_integration,
        overlap_policy=overlap_policy,
        allow_overlapping_execution=allow_overlap,
        properties=json.loads(properties) if properties else {},
        session_parameters=json.loads(session_parameters) if session_parameters else {},
        last_committed_on=committed,
        last_suspended_on=suspended,
        last_suspended_reason=reason,
    )
