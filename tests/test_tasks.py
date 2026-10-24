import tempfile

import pytest
import snowflake.connector
import snowflake.connector.cursor

import fakesnow
from tests.utils import indent


def _select(cur: snowflake.connector.cursor.SnowflakeCursor, columns: str, name: str | None = None) -> list[tuple]:
    where = f" where name = '{name.upper()}'" if name else ""
    cur.execute(f"select {columns} from db1._fs_information_schema._fs_tasks{where}")
    return cur.fetchall()


def _task_ids(cur: snowflake.connector.cursor.SnowflakeCursor) -> list[tuple]:
    cur.execute("select id from db1._fs_information_schema._fs_tasks")
    return cur.fetchall()


def test_create_task_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    assert cur.fetchall() == [("Task T1 successfully created.",)]


def test_create_task_if_not_exists_existing(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task if not exists t1 as select 1")
    assert cur.fetchall() == [("T1 already exists, statement succeeded.",)]


def test_create_task_existing_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="already exists"):
        cur.execute("create task t1 as select 1")


def test_create_or_replace_task_gets_new_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    (before,) = _task_ids(cur)
    cur.execute("create or replace task t1 as select 2")
    (after,) = _task_ids(cur)
    assert before != after


def test_create_or_alter_task_keeps_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    (before,) = _task_ids(cur)
    cur.execute("create or alter task t1 as select 2")
    assert _task_ids(cur) == [before]


def test_create_or_alter_task_replaces_definition(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create or alter task t1 as select 2")
    assert _select(cur, "definition") == [("select 2",)]


def test_create_or_alter_task_resets_omitted_properties(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 comment = 'c' as select 1")
    cur.execute("create or alter task t1 as select 1")
    assert _select(cur, "comment") == [(None,)]


def test_create_or_alter_task_keeps_state(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("update db1._fs_information_schema._fs_tasks set state = 'started'")
    cur.execute("create or alter task t1 as select 2")
    assert _select(cur, "state") == [("started",)]


def test_create_task_unknown_property_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="invalid property 'NOPE' for 'TASK'"):
        cur.execute("create task t1 nope = 1 as select 1")


def test_create_task_autocommit_false_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="invalid property 'AUTOCOMMIT' for 'TASK'"):
        cur.execute("create task t1 autocommit = false as select 1")


def test_create_task_is_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    assert _select(cur, "state") == [("suspended",)]


def test_create_task_owner_is_session_role(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", role="SYSADMIN") as conn:
        cur = conn.cursor()
        cur.execute("create task t1 as select 1")
        assert _select(cur, "owner") == [("SYSADMIN",)]


def test_create_task_stores_column_properties(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 warehouse = wh schedule = '5 minute' as select 1")
    assert _select(cur, "warehouse, schedule") == [("WH", "5 minute")]


def test_create_task_stores_session_parameters(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 timezone = 'UTC' as select 1")
    assert indent(_select(cur, "session_parameters::varchar")) == [('{\n  "TIMEZONE": "UTC"\n}',)]


def test_create_task_clone_copies_properties(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 comment = 'c' schedule = '1 minute' as select 1")
    cur.execute("create task t2 clone t1")
    assert _select(cur, "comment, schedule, definition", "t2") == [("c", "1 minute", "select 1")]


def test_create_task_clone_gets_new_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 clone t1")
    assert len({id_ for (id_,) in _task_ids(cur)}) == 2


def test_create_task_clone_is_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("update db1._fs_information_schema._fs_tasks set state = 'started'")
    cur.execute("create task t2 clone t1")
    assert _select(cur, "state", "t2") == [("suspended",)]


def test_create_task_clone_clears_last_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute(
        "update db1._fs_information_schema._fs_tasks set last_committed_on = now(), last_suspended_on = now(), "
        "last_suspended_reason = 'x'"
    )
    cur.execute("create task t2 clone t1")
    assert _select(cur, "last_committed_on, last_suspended_on, last_suspended_reason", "t2") == [(None, None, None)]


def test_create_task_clone_missing_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist"):
        cur.execute("create task t2 clone nope")


def test_drop_task_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("drop task t1")
    assert cur.fetchall() == [("T1 successfully dropped.",)]


def test_drop_task_removes_it(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("drop task t1")
    assert _task_ids(cur) == []


def test_drop_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist or not authorized\.",
    ):
        cur.execute("drop task nope")


def test_drop_task_if_exists_missing(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("drop task if exists nope")
    assert cur.rowcount == 1


def test_create_task_quoted_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Task" as select 1')
    assert cur.fetchall() == [("Task My Task successfully created.",)]


def test_create_task_no_current_schema_raises():
    with fakesnow.patch(), snowflake.connector.connect(database="db1") as conn:
        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not have a current schema") as e:
            conn.cursor().execute("create task t1 as select 1")
        assert e.value.errno == 90106


def test_create_task_missing_schema_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Schema 'DB1\.NOPE' does not exist or not authorized\.",
    ) as e:
        cur.execute("create task nope.t1 as select 1")
    assert (e.value.errno, e.value.sqlstate) == (2003, "02000")


def test_create_task_missing_database_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Schema 'NOPE\.SCHEMA1' does not exist or not authorized\.",
    ):
        cur.execute("create task nope.schema1.t1 as select 1")


def test_create_task_commits_open_transaction(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("create table t (i int)")
        cur.execute("insert into t values (1)")
        cur.execute("create task t1 as select 1")
        conn.rollback()
        cur.execute("select i from t")
        assert cur.fetchall() == [(1,)]


def test_create_task_no_current_database_raises():
    with fakesnow.patch(), snowflake.connector.connect() as conn:
        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not have a current database") as e:
            conn.cursor().execute("create task t1 as select 1")
        assert e.value.errno == 90105


def test_tasks_persist_with_db_path():
    with tempfile.TemporaryDirectory(prefix="fakesnow-test") as db_path:
        with fakesnow.patch(db_path=db_path), snowflake.connector.connect(database="db1", schema="schema1") as conn:
            conn.cursor().execute("create task t1 as select 1")
        with (
            fakesnow.patch(db_path=db_path),
            snowflake.connector.connect(database="db1", schema="schema1") as conn,
            pytest.raises(snowflake.connector.errors.ProgrammingError, match="already exists"),
        ):
            conn.cursor().execute("create task t1 as select 1")


def test_drop_schema_drops_its_tasks(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema s2")
    cur.execute("create task s2.t1 as select 1")
    cur.execute("drop schema s2")
    cur.execute("create schema s2")
    cur.execute("create task s2.t1 as select 1")
    assert cur.fetchall() == [("Task T1 successfully created.",)]
