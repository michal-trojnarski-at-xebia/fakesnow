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
from fakesnow.tasks.statements import CreateTask, DropTask, TaskName
from fakesnow.transforms.temporary import check_schema_exists

# provisional messages and values, to be checked against live Snowflake
CREATED = "Task {name} successfully created."
EXISTS = "{name} already exists, statement succeeded."
DROPPED = "{name} successfully dropped."
DROP_MISSING = "Drop statement executed successfully ({name} already dropped)."
ALREADY_EXISTS = "SQL compilation error:\nObject '{fqname}' already exists."
DOES_NOT_EXIST = "SQL compilation error:\nTask '{fqname}' does not exist or not authorized."
INVALID_PROPERTY = "SQL compilation error:\ninvalid property '{prop}' for 'TASK'"

SUSPENDED = "suspended"

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
                f"select {_COLUMNS} from {_table(database)} where schema_name = ? and name = ?", [schema, name]
            ).fetchone()
        return _task(database, row) if row else None

    def require(self, name: TaskName) -> Task:
        database, schema, task_name = self.resolve(name)
        task = self.get(database, schema, task_name)
        if task is None:
            raise _error(DOES_NOT_EXIST.format(fqname=f"{database}.{schema}.{task_name}"), 2003, "02000")
        return task

    def create(self, stmt: CreateTask) -> str:
        database, schema, name = self.resolve(stmt.name)
        with self.duck_conn.cursor() as conn:
            check_schema_exists(conn, database, schema)
        task = self._new(database, schema, name, stmt)
        existing = self.get(database, schema, name)
        if existing is None or stmt.replace:
            self.save(task)
        elif stmt.or_alter:
            # altering keeps the identity, state and history, and resets what the statement leaves out
            self.save(
                replace(
                    task,
                    id=existing.id,
                    created_on=existing.created_on,
                    state=existing.state,
                    last_committed_on=existing.last_committed_on,
                    last_suspended_on=existing.last_suspended_on,
                    last_suspended_reason=existing.last_suspended_reason,
                )
            )
        elif stmt.if_not_exists:
            return _status(EXISTS, name)
        else:
            raise _error(ALREADY_EXISTS.format(fqname=existing.fqname), 2002, "42710")
        return _status(CREATED, name)

    def drop(self, stmt: DropTask) -> str:
        database, schema, name = self.resolve(stmt.name)
        if self.get(database, schema, name) is None:
            if stmt.if_exists:
                return _status(DROP_MISSING, name)
            raise _error(DOES_NOT_EXIST.format(fqname=f"{database}.{schema}.{name}"), 2003, "02000")
        self.delete(database, schema, name)
        return _status(DROPPED, name)

    def delete(self, database: str, schema: str, name: str) -> None:
        with self.duck_conn.cursor() as conn:
            conn.execute(f"delete from {_table(database)} where schema_name = ? and name = ?", [schema, name])

    def delete_schema(self, database: str, schema: str) -> None:
        """Delete every task in a schema."""
        with self.duck_conn.cursor() as conn:
            conn.execute(f"delete from {_table(database)} where schema_name = ?", [schema])

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
                f"insert or replace into {_table(task.database)} ({_COLUMNS}) values ({', '.join('?' * len(values))})",
                values,
            )

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
        return task


def _now() -> datetime:
    return datetime.now().astimezone()


def _table(database: str) -> str:
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
