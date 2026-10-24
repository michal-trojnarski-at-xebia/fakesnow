import tempfile
from datetime import datetime

import pytest
import snowflake.connector
import snowflake.connector.cursor

import fakesnow
from fakesnow.conn import FakeSnowflakeConnection
from fakesnow.instance import FakeSnow
from tests.utils import dindent, indent


def _select(cur: snowflake.connector.cursor.SnowflakeCursor, columns: str, name: str | None = None) -> list[tuple]:
    where = f" where name = '{name.upper()}'" if name else ""
    cur.execute(f"select {columns} from db1._fs_information_schema._fs_tasks{where}")
    return cur.fetchall()


def _show(cur: snowflake.connector.cursor.SnowflakeCursor, columns: str, name: str | None = None) -> list[tuple]:
    """The columns of SHOW TASKS, for the named task or all tasks. A quoted name is matched as written, otherwise
    it is upper-cased."""
    cur.execute("show tasks")
    names = [d.name for d in cur.description]
    wanted = [c.strip() for c in columns.split(",")]
    return [
        tuple(row[names.index(c)] for c in wanted)
        for row in cur.fetchall()
        if name is None or row[names.index("name")] == (name[1:-1] if name.startswith('"') else name.upper())
    ]


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
    assert _show(cur, "definition") == [("select 2",)]


def test_create_or_alter_task_resets_omitted_properties(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 comment = 'c' as select 1")
    cur.execute("create or alter task t1 as select 1")
    assert _show(cur, "comment") == [(None,)]


def test_create_or_alter_task_keeps_last_suspended_reason(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("alter task child remove after root")
    cur.execute("create or alter task child as select 2")
    assert _show(cur, "last_suspended_reason", "child") == [("CHILD_BECAME_ROOT",)]


def test_create_task_unknown_property_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="invalid property 'NOPE' for 'TASK'"):
        cur.execute("create task t1 nope = 1 as select 1")


def test_create_task_autocommit_false_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="invalid property 'AUTOCOMMIT' for 'TASK'"):
        cur.execute("create task t1 autocommit = false as select 1")


def test_create_task_is_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    assert _show(cur, "state") == [("suspended",)]


def test_create_task_owner_is_session_role(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", role="SYSADMIN") as conn:
        cur = conn.cursor()
        cur.execute("create task t1 as select 1")
        assert _show(cur, "owner") == [("SYSADMIN",)]


def test_create_task_stores_column_properties(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 warehouse = wh schedule = '5 minute' as select 1")
    assert _show(cur, "warehouse, schedule") == [("WH", "5 minute")]


def test_create_task_stores_session_parameters(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 timezone = 'UTC' as select 1")
    assert indent(_select(cur, "session_parameters::varchar")) == [('{\n  "TIMEZONE": "UTC"\n}',)]


def test_create_task_clone_copies_properties(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 comment = 'c' schedule = '1 minute' as select 1")
    cur.execute("create task t2 clone t1")
    assert _show(cur, "comment, schedule, definition", "t2") == [("c", "1 minute", "select 1")]


def test_create_task_clone_gets_new_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 clone t1")
    assert len({id_ for (id_,) in _task_ids(cur)}) == 2


def test_create_task_clone_is_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("create task t2 clone t1")
    assert _show(cur, "state", "t2") == [("suspended",)]


def test_create_task_clone_clears_last_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("alter task t1 suspend")
    cur.execute("create task t2 clone t1")
    assert _show(cur, "last_committed_on, last_suspended_on, last_suspended_reason", "t2") == [(None, None, None)]


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


def _start(cur: snowflake.connector.cursor.SnowflakeCursor, name: str) -> None:
    cur.execute(f"alter task {name} resume")


def _create_children(
    cur: snowflake.connector.cursor.SnowflakeCursor, count: int, after: str, prefix: str = "c"
) -> None:
    for i in range(count):
        cur.execute(f"create task {prefix}{i} after {after} as select 1")


def test_create_child_of_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    _start(cur, "root")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Unable to update graph with root task DB1\.SCHEMA1\.ROOT since that root task is not suspended\.",
    ) as e:
        cur.execute("create task child after root as select 1")
    assert (e.value.errno, e.value.sqlstate) == (91089, "02000")


def test_create_child_with_missing_predecessor_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError, match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist or not authorized"
    ):
        cur.execute("create task child after nope as select 1")


def test_create_child_with_schedule_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.CHILD with predecessors or a finalized root cannot have a schedule\.",
    ):
        cur.execute("create task child schedule = '1 minute' after root as select 1")


def test_create_child_in_other_schema_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema s2")
    cur.execute("create task s2.root as select 1")
    cur.execute("use schema schema1")  # create schema switches to the new schema
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.CHILD cannot depend on task DB1\.S2\.ROOT in a different schema\.",
    ):
        cur.execute("create task child after s2.root as select 1")


def test_create_second_finalizer_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin1 finalize = root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid finalizer task DB1\.SCHEMA1\.FIN2: root task DB1\.SCHEMA1\.ROOT already has a finalizer\.",
    ):
        cur.execute("create task fin2 finalize = root as select 1")


def test_create_finalizer_with_predecessor_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task other as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid finalizer task DB1\.SCHEMA1\.FIN: a finalizer cannot have predecessors\.",
    ):
        cur.execute("create task fin finalize = root after other as select 1")


def test_create_finalizer_with_children_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin as select 1")
    cur.execute("create task child after fin as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid finalizer task DB1\.SCHEMA1\.FIN: a finalizer cannot have child tasks\.",
    ):
        cur.execute("create or replace task fin finalize = root as select 1")


def test_create_task_cycle_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task graph cycle detected involving task DB1\.SCHEMA1\.ROOT\.",
    ):
        cur.execute("create or replace task root after child as select 1")


def test_create_task_too_many_predecessors_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    # three middle tasks keep every task within the child limit, in a single graph
    for m in range(3):
        cur.execute(f"create task m{m} after root as select 1")
        _create_children(cur, 34, after=f"m{m}", prefix=f"c{m}_")
    leaves = [f"c{m}_{i}" for m in range(3) for i in range(34)]
    after = ", ".join(leaves[:101])
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.JOIN exceeds the maximum of 100 predecessors\.",
    ):
        cur.execute(f"create task join after {after} as select 1")


def test_create_task_too_many_children_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    _create_children(cur, 100, after="root")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.ROOT exceeds the maximum of 100 child tasks\.",
    ):
        cur.execute("create task one_more after root as select 1")


def test_drop_only_predecessor_makes_child_standalone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("drop task root")
    assert _show(cur, "state, last_suspended_reason", "child") == [("suspended", "CHILD_BECAME_ROOT")]


def test_drop_only_predecessor_clears_child_predecessors(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("drop task root")
    assert _show(cur, "predecessors", "child") == [("[]",)]


def test_drop_root_makes_finalizer_standalone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("drop task root")
    assert indent(_show(cur, "state, task_relations, last_suspended_reason", "fin")) == [
        ("suspended", '{\n  "Predecessors": []\n}', "FINALIZER_BECAME_ROOT")
    ]


def test_drop_task_in_started_graph_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("drop task child")


@pytest.mark.parametrize("create", ["create or replace task t1 as select 2", "create or alter task t1 as select 2"])
def test_create_over_started_standalone_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor, create: str):
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    _start(cur, "t1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute(create)


def test_create_child_after_finalizer_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid finalizer task DB1\.SCHEMA1\.FIN: a finalizer cannot have child tasks\.",
    ):
        cur.execute("create task child after fin as select 1")


def test_create_child_after_task_in_started_second_graph_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root1 as select 1")
    cur.execute("create task root2 schedule = '1 minute' as select 1")
    _start(cur, "root2")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("create task child after root1, root2 as select 1")


def test_create_child_with_predecessors_in_different_graphs_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root1 as select 1")
    cur.execute("create task root2 as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.CHILD cannot have predecessors in different task graphs\.",
    ):
        cur.execute("create task child after root1, root2 as select 1")


def test_create_child_with_predecessors_in_one_graph(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after root as select 1")
    cur.execute("create task join after a, b as select 1")
    assert indent(_show(cur, "predecessors", "join")) == [('[\n  "DB1.SCHEMA1.A",\n  "DB1.SCHEMA1.B"\n]',)]


def test_create_child_after_other_schema_task_ignores_same_named_started_local_task(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task x schedule = '1 minute' as select 1")
    _start(cur, "x")
    cur.execute("create schema s2")
    cur.execute("create task s2.x as select 1")
    cur.execute("use schema schema1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="in a different schema"):
        cur.execute("create task child after s2.x as select 1")


def test_create_finalizer_of_non_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid finalizer task DB1\.SCHEMA1\.FIN: DB1\.SCHEMA1\.CHILD is not a root task\.",
    ):
        cur.execute("create task fin finalize = child as select 1")


def test_create_task_finalizing_itself_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid finalizer task DB1\.SCHEMA1\.T1: a task cannot finalize itself\.",
    ):
        cur.execute("create or replace task t1 finalize = t1 as select 1")


def test_create_or_replace_task_over_started_child_with_new_predecessor_raises(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create task other as select 1")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"graph with root task DB1\.SCHEMA1\.ROOT"):
        cur.execute("create or replace task child after other as select 1")


def test_create_or_replace_task_over_started_standalone_with_new_finalize_raises(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task root as select 1")
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    _start(cur, "t1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"graph with root task DB1\.SCHEMA1\.T1"):
        cur.execute("create or replace task t1 finalize = root as select 1")


def test_create_or_alter_task_over_started_standalone_with_new_predecessor_raises(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task root as select 1")
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    _start(cur, "t1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"graph with root task DB1\.SCHEMA1\.T1"):
        cur.execute("create or alter task t1 after root as select 1")


def test_alter_task_resume_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 resume")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_alter_task_resume_sets_state(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 resume")
    assert _show(cur, "state") == [("started",)]


def test_alter_task_first_resume_sets_last_committed_on(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 resume")
    assert _show(cur, "last_committed_on") != [(None,)]


def test_alter_task_second_resume_keeps_last_committed_on(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 resume")
    (before,) = _show(cur, "last_committed_on")
    cur.execute("alter task t1 suspend")
    cur.execute("alter task t1 resume")
    assert _show(cur, "last_committed_on") == [before]


def test_alter_task_suspend_sets_state(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("alter task t1 suspend")
    assert _show(cur, "state") == [("suspended",)]


def test_alter_task_suspend_sets_reason(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("alter task t1 suspend")
    assert _show(cur, "last_suspended_reason") == [("USER_SUSPENDED",)]


def test_alter_task_suspend_sets_last_suspended_on(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("alter task t1 suspend")
    assert _show(cur, "last_suspended_on") != [(None,)]


def test_alter_task_set_comment(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 set comment = 'c'")
    assert _show(cur, "comment") == [("c",)]


def test_alter_task_set_keeps_other_properties(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 warehouse = wh as select 1")
    cur.execute("alter task t1 set comment = 'c'")
    assert _show(cur, "warehouse") == [("WH",)]


def test_alter_task_set_unknown_property_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="invalid property 'NOPE' for 'TASK'"):
        cur.execute("alter task t1 set nope = 1")


def test_alter_task_set_session_parameter(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 set timezone = 'UTC'")
    assert indent(_select(cur, "session_parameters::varchar")) == [('{\n  "TIMEZONE": "UTC"\n}',)]


def test_alter_task_unset_comment(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 comment = 'c' as select 1")
    cur.execute("alter task t1 unset comment")
    assert _show(cur, "comment") == [(None,)]


def test_alter_task_unset_session_parameter(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 timezone = 'UTC' as select 1")
    cur.execute("alter task t1 unset timezone")
    assert indent(_select(cur, "session_parameters::varchar")) == [("{}",)]


def test_alter_task_set_on_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    _start(cur, "t1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task t1 set comment = 'c'")


def test_alter_child_resume_with_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"graph with root task DB1\.SCHEMA1\.ROOT since that root task is not suspended",
    ):
        cur.execute("alter task child resume")


def test_alter_child_suspend_with_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("alter task child resume")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task child suspend")


def test_alter_started_root_suspend_succeeds(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    cur.execute("alter task root suspend")
    assert _show(cur, "state", "root") == [("suspended",)]


def test_alter_task_add_after(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child as select 1")
    cur.execute("alter task child add after root")
    assert indent(_show(cur, "predecessors", "child")) == [('[\n  "DB1.SCHEMA1.ROOT"\n]',)]


def test_alter_task_add_after_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child as select 1")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task child add after root")


def test_alter_task_add_after_other_schema_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task child as select 1")
    cur.execute("create schema s2")
    cur.execute("create task s2.p as select 1")
    cur.execute("use schema schema1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.CHILD cannot depend on task DB1\.S2\.P in a different schema\.",
    ):
        cur.execute("alter task child add after s2.p")


def test_alter_task_add_after_cycle_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="Task graph cycle detected"):
        cur.execute("alter task root add after child")


def test_alter_task_add_after_to_scheduled_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="cannot have a schedule"):
        cur.execute("alter task t1 add after root")


def test_alter_task_add_after_different_graph_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root1 as select 1")
    cur.execute("create task root2 as select 1")
    cur.execute("create task child after root1 as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="predecessors in different task graphs"):
        cur.execute("alter task child add after root2")


def test_alter_task_remove_after(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after root as select 1")
    cur.execute("create task child after a, b as select 1")
    cur.execute("alter task child remove after a")
    assert indent(_show(cur, "predecessors", "child")) == [('[\n  "DB1.SCHEMA1.B"\n]',)]


def test_alter_task_remove_last_predecessor_makes_child_standalone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("alter task child remove after root")
    assert _show(cur, "predecessors, state, last_suspended_reason", "child") == [
        ("[]", "suspended", "CHILD_BECAME_ROOT")
    ]


def test_alter_task_remove_after_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task child remove after root")


def test_alter_task_set_finalize(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin as select 1")
    cur.execute("alter task fin set finalize = root")
    assert indent(_show(cur, "task_relations", "fin")) == [
        ('{\n  "Predecessors": [],\n  "FinalizedRootTask": "DB1.SCHEMA1.ROOT"\n}',)
    ]


def test_alter_task_set_finalize_of_non_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create task fin as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="is not a root task"):
        cur.execute("alter task fin set finalize = child")


def test_alter_task_set_schedule_on_child_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="cannot have a schedule"):
        cur.execute("alter task child set schedule = '1 minute'")


def test_alter_task_unset_finalize_makes_finalizer_standalone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("alter task fin unset finalize")
    assert indent(_show(cur, "task_relations, state, last_suspended_reason", "fin")) == [
        ('{\n  "Predecessors": []\n}', "suspended", "FINALIZER_BECAME_ROOT")
    ]


def test_alter_task_modify_as(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 modify as select 2")
    assert _show(cur, "definition") == [("select 2",)]


def test_alter_task_modify_when(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 modify when 1 = 1")
    assert _show(cur, "condition") == [("1 = 1",)]


def test_alter_task_remove_when(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 when 1 = 1 as select 1")
    cur.execute("alter task t1 remove when")
    assert _show(cur, "condition") == [(None,)]


def test_alter_task_modify_as_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    _start(cur, "t1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task t1 modify as select 2")


def test_alter_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist or not authorized\.",
    ):
        cur.execute("alter task nope resume")


def test_alter_missing_task_if_exists(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("alter task if exists nope resume")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_task_dependents_enable_column_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    assert [d.name for d in cur.description] == ["SYSTEM$TASK_DEPENDENTS_ENABLE('root')"]


def test_task_dependents_enable_returns_null(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    assert cur.fetchall() == [(None,)]


def test_task_dependents_enable_resumes_graph(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create task grandchild after child as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    assert sorted(_show(cur, "name, state")) == [("CHILD", "started"), ("GRANDCHILD", "started"), ("ROOT", "started")]


def test_task_dependents_enable_leaves_other_graphs_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task other as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    assert _show(cur, "state", "other") == [("suspended",)]


def test_task_dependents_enable_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist or not authorized"):
        cur.execute("select system$task_dependents_enable('nope')")


def test_task_dependents_enable_in_larger_select_not_implemented(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="only supported as the sole column"):
        cur.execute("select system$task_dependents_enable('root'), 1")


SHOW_TASKS_COLUMNS = [
    "created_on",
    "name",
    "id",
    "database_name",
    "schema_name",
    "owner",
    "comment",
    "warehouse",
    "schedule",
    "scheduling_mode",
    "target_completion_interval",
    "predecessors",
    "state",
    "definition",
    "condition",
    "allow_overlapping_execution",
    "error_integration",
    "success_integration",
    "last_committed_on",
    "last_suspended_on",
    "owner_role_type",
    "config",
    "last_suspended_reason",
    "task_relations",
    "execute_as_user",
    "overlap_policy",
]


def _show_names(cur: snowflake.connector.cursor.SnowflakeCursor, show: str) -> list[str]:
    cur.execute(show)
    names = [d.name for d in cur.description]
    return [row[names.index("name")] for row in cur.fetchall()]


def test_show_tasks_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("show tasks")
    assert [d.name for d in cur.description] == SHOW_TASKS_COLUMNS


def test_show_tasks_new_task_row(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create task t1 schedule = '5 minute' as select 1")
    dcur.execute("show tasks")
    (row,) = dindent(dcur.fetchall())
    assert row.pop("created_on")
    assert row.pop("id")
    assert row == {
        "name": "T1",
        "database_name": "DB1",
        "schema_name": "SCHEMA1",
        "owner": "PUBLIC",
        "comment": None,
        "warehouse": None,
        "schedule": "5 minute",
        "scheduling_mode": None,
        "target_completion_interval": None,
        "predecessors": "[]",
        "state": "suspended",
        "definition": "select 1",
        "condition": None,
        "allow_overlapping_execution": "false",
        "error_integration": None,
        "success_integration": None,
        "last_committed_on": None,
        "last_suspended_on": None,
        "owner_role_type": "ROLE",
        "config": None,
        "last_suspended_reason": None,
        "task_relations": '{\n  "Predecessors": []\n}',
        "execute_as_user": None,
        "overlap_policy": None,
    }


def test_show_tasks_predecessors_are_qualified(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after root as select 1")
    cur.execute("create task join after a, b as select 1")
    assert indent(_show(cur, "predecessors", "join")) == [('[\n  "DB1.SCHEMA1.A",\n  "DB1.SCHEMA1.B"\n]',)]


def test_show_tasks_child_has_null_root_only_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root config = '{\"k\": 1}' as select 1")
    cur.execute("create task child after root as select 1")
    assert _show(cur, "allow_overlapping_execution, config, overlap_policy", "child") == [(None, None, None)]


def test_show_tasks_task_relations_for_root_with_finalizer(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    assert indent(_show(cur, "task_relations", "root")) == [
        ('{\n  "Predecessors": [],\n  "FinalizerTask": "DB1.SCHEMA1.FIN"\n}',)
    ]


def test_show_tasks_task_relations_for_finalizer(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    assert indent(_show(cur, "task_relations", "fin")) == [
        ('{\n  "Predecessors": [],\n  "FinalizedRootTask": "DB1.SCHEMA1.ROOT"\n}',)
    ]


def test_show_tasks_task_relations_for_child(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    assert indent(_show(cur, "task_relations", "child")) == [
        ('{\n  "Predecessors": [\n    "DB1.SCHEMA1.ROOT"\n  ]\n}',)
    ]


def test_show_tasks_target_completion_interval(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '5 minute' target_completion_interval = '10 minutes' as select 1")
    assert _show(cur, "target_completion_interval") == [("10 minutes",)]


def test_show_terse_tasks_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("show terse tasks")
    assert [d.name for d in cur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
        "schedule",
    ]


def test_show_terse_tasks_row(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create task t1 schedule = '5 minute' as select 1")
    dcur.execute("show terse tasks")
    (row,) = dcur.fetchall()
    assert row.pop("created_on")
    assert row == {"name": "T1", "kind": None, "database_name": "DB1", "schema_name": "SCHEMA1", "schedule": "5 minute"}


def test_show_tasks_orders_by_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task b as select 1")
    cur.execute("create task a as select 1")
    assert _show_names(cur, "show tasks") == ["A", "B"]


def test_show_tasks_like(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task load_a as select 1")
    cur.execute("create task other as select 1")
    assert _show_names(cur, "show tasks like 'LOAD%'") == ["LOAD_A"]


def test_show_tasks_like_is_case_insensitive(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task load_a as select 1")
    assert _show_names(cur, "show tasks like 'load%'") == ["LOAD_A"]


def test_show_tasks_starts_with(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task load_a as select 1")
    cur.execute("create task other as select 1")
    assert _show_names(cur, "show tasks starts with 'LOAD'") == ["LOAD_A"]


def test_show_tasks_starts_with_is_case_sensitive(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task load_a as select 1")
    assert _show_names(cur, "show tasks starts with 'load'") == []


def test_show_tasks_root_only(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    assert _show_names(cur, "show tasks root only") == ["ROOT"]


def test_show_tasks_limit(cur: snowflake.connector.cursor.SnowflakeCursor):
    for name in ("a", "b", "c"):
        cur.execute(f"create task {name} as select 1")
    assert _show_names(cur, "show tasks limit 2") == ["A", "B"]


def test_show_tasks_limit_from(cur: snowflake.connector.cursor.SnowflakeCursor):
    for name in ("a", "b", "c"):
        cur.execute(f"create task {name} as select 1")
    assert _show_names(cur, "show tasks limit 1 from 'B'") == ["B"]


def test_show_tasks_defaults_to_current_database(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create database db2")
    cur.execute("create schema db2.schema1")
    cur.execute("use schema db1.schema1")
    cur.execute("create task db2.schema1.other as select 1")
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks") == ["T1"]


def test_show_tasks_in_database(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create database db2")
    cur.execute("create schema db2.schema1")
    cur.execute("use schema db1.schema1")
    cur.execute("create task db2.schema1.other as select 1")
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks in database db2") == ["OTHER"]


def test_show_tasks_in_schema_scopes_same_named_tasks(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema schema2")
    cur.execute("use schema schema1")
    cur.execute("create task schema1.t1 as select 1")
    cur.execute("create task schema2.t1 comment = 'two' as select 1")
    cur.execute("show tasks in schema schema2")
    names = [d.name for d in cur.description]
    assert [row[names.index("comment")] for row in cur.fetchall()] == ["two"]


def test_show_tasks_in_schema_without_name_uses_current_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema schema2")
    cur.execute("use schema schema1")
    cur.execute("create task schema1.t1 as select 1")
    cur.execute("create task schema2.t2 as select 1")
    assert _show_names(cur, "show tasks in schema") == ["T1"]


def test_show_tasks_in_schema_with_database(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create database db2")
    cur.execute("create schema db2.schema1")
    cur.execute("use schema db1.schema1")
    cur.execute("create task db2.schema1.other as select 1")
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks in schema db2.schema1") == ["OTHER"]


def test_show_tasks_in_account(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create database db2")
    cur.execute("create schema db2.schema1")
    cur.execute("use schema db1.schema1")
    cur.execute("create task db2.schema1.other as select 1")
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks in account") == ["OTHER", "T1"]


def test_show_tasks_without_current_database_uses_account(_fakesnow: None):
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        cur.execute("create database db1")
        cur.execute("create schema db1.schema1")
        cur.execute("create task db1.schema1.t1 as select 1")
        assert _show_names(cur, "show tasks") == ["T1"]


def test_describe_task_row(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create task t1 schedule = '5 minute' comment = 'c' as select 1")
    dcur.execute("show tasks")
    (shown,) = dcur.fetchall()
    dcur.execute("describe task t1")
    assert dcur.fetchall() == [shown]


def test_describe_task_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("desc task t1")
    assert [d.name for d in cur.description] == SHOW_TASKS_COLUMNS


def test_describe_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist"):
        cur.execute("describe task nope")


def test_show_tasks_quoted_mixed_case_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Task" as select 1')
    assert _show(cur, "name") == [("My Task",)]


def test_describe_task_quoted_mixed_case_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Task" as select 1')
    cur.execute('describe task "My Task"')
    names = [d.name for d in cur.description]
    assert [row[names.index("name")] for row in cur.fetchall()] == ["My Task"]


def test_show_tasks_in_account_skips_database_without_tasks_table(conn: snowflake.connector.SnowflakeConnection):
    conn._duck_conn.execute("attach ':memory:' as plain")  # type: ignore[attr-defined]  # noqa: SLF001
    cur = conn.cursor()
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks in account") == ["T1"]


def _history(cur: snowflake.connector.cursor.SnowflakeCursor, columns: str, name: str | None = None) -> list[tuple]:
    where = f" where name = '{name.upper()}'" if name else ""
    history = "table(information_schema.task_history(result_limit => 1000))"
    cur.execute(f"select {columns} from {history}{where} order by scheduled_time")
    return cur.fetchall()


def _graph_runs(cur: snowflake.connector.cursor.SnowflakeCursor, columns: str) -> list[tuple]:
    graphs = "table(information_schema.complete_task_graphs(result_limit => 1000))"
    cur.execute(f"select {columns} from {graphs} order by scheduled_time")
    return cur.fetchall()


def _log(cur: snowflake.connector.cursor.SnowflakeCursor) -> list[str]:
    cur.execute("select name from log order by seq")
    return [name for (name,) in cur.fetchall()]


def _logging_task(cur: snowflake.connector.cursor.SnowflakeCursor, name: str, clause: str = "") -> None:
    """Create a task that appends its name to the log table."""
    cur.execute(f"create task {name} {clause} as insert into log (name) values ('{name}')")


@pytest.fixture
def log(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table log (seq int autoincrement, name varchar)")


def test_execute_task_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    assert cur.fetchall() == [("Task T1 is scheduled to run immediately.",)]


def test_execute_task_runs_body(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (i int)")
    cur.execute("create task t1 as insert into t values (1)")
    cur.execute("execute task t1")
    cur.execute("select * from t")
    assert cur.fetchall() == [(1,)]


def test_execute_task_runs_graph_in_order(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "child", "after root")
    _logging_task(cur, "grandchild", "after child")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    assert _log(cur) == ["root", "child", "grandchild"]


def test_execute_task_failed_body_does_not_raise(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    assert cur.fetchall() == [("Task T1 is scheduled to run immediately.",)]


def test_execute_task_failed_task_skips_dependents(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    cur.execute("create task root as select * from missing")
    _logging_task(cur, "child", "after root")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    assert _log(cur) == []


def test_execute_task_finalizer_runs_after_failure(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    cur.execute("create task root as select * from missing")
    _logging_task(cur, "child", "after root")
    _logging_task(cur, "fin", "finalize = root")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    assert _log(cur) == ["fin"]


def test_execute_task_when_false_skips_task_and_dependents(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "child", "after root when 1 = 2")
    _logging_task(cur, "grandchild", "after child")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    assert _log(cur) == ["root"]


def test_execute_task_root_when_false_skips_finalizer(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root", "when 1 = 2")
    _logging_task(cur, "fin", "finalize = root")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    assert _log(cur) == []


def test_execute_task_suspended_child_is_skipped(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "child", "after root")
    cur.execute("execute task root")
    assert _log(cur) == ["root"]


def test_execute_task_suspended_child_counts_as_succeeded(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "child", "after root")
    _logging_task(cur, "grandchild", "after child")
    cur.execute("alter task grandchild resume")
    cur.execute("execute task root")
    assert _log(cur) == ["root", "grandchild"]


def test_execute_task_suspended_child_has_no_history_row(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("execute task root")
    assert _history(cur, "name") == [("ROOT",)]


def test_execute_child_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Cannot execute task DB1\.SCHEMA1\.CHILD: EXECUTE TASK can only run a standalone or root task\.",
    ) as e:
        cur.execute("execute task child")
    assert (e.value.errno, e.value.sqlstate) == (2003, "02000")


def test_execute_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist"):
        cur.execute("execute task nope")


def test_execute_task_runs_in_own_session(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (s varchar)")
    cur.execute("create task t1 as insert into db1.schema1.t select current_schema()")
    cur.execute("create schema s2")
    cur.execute("execute task schema1.t1")
    cur.execute("select * from schema1.t")
    assert cur.fetchall() == [("SCHEMA1",)]


def test_execute_task_runs_as_owner_role(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", role="SYSADMIN") as conn:
        cur = conn.cursor()
        cur.execute("create table t (r varchar)")
        cur.execute("create task t1 as insert into t select current_role()")
    with snowflake.connector.connect(database="db1", schema="schema1") as conn:
        cur = conn.cursor()
        cur.execute("execute task t1")
        cur.execute("select * from t")
        assert cur.fetchall() == [("SYSADMIN",)]


def test_execute_task_commits_callers_transaction(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("create table t (i int)")
        cur.execute("create task t1 as select 1")
        cur.execute("insert into t values (1)")
        cur.execute("execute task t1")
        conn.rollback()
        cur.execute("select i from t")
        assert cur.fetchall() == [(1,)]


def test_execute_task_sees_callers_uncommitted_writes(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("create table t (i int)")
        cur.execute("create table copied (i int)")
        cur.execute("create task t1 as insert into copied select * from t")
        cur.execute("insert into t values (1)")
        cur.execute("execute task t1")
        cur.execute("select i from copied")
        assert cur.fetchall() == [(1,)]


def test_execute_task_quoted_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (i int)")
    cur.execute('create task "My Task" as insert into t values (1)')
    cur.execute('execute task "My Task"')
    cur.execute("select * from t")
    assert cur.fetchall() == [(1,)]


def test_execute_task_with_plain_connection_raises():
    fs = FakeSnow()
    try:
        conn = FakeSnowflakeConnection(fs.duck_conn.cursor(), fs.results_cache, database="db1", schema="schema1")
        cur = conn.cursor()
        cur.execute("create task t1 as select 1")
        # the cursor reports the NotImplementedError as a ProgrammingError
        with pytest.raises(
            snowflake.connector.errors.ProgrammingError, match=r"needs a connection from fakesnow\.patch"
        ):
            cur.execute("execute task t1")
    finally:
        fs.close()


def test_execute_task_sets_last_committed_on(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    ((last_committed_on,),) = _show(cur, "last_committed_on")
    assert last_committed_on is not None


def test_execute_task_history_succeeded_row(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    assert _history(cur, "name, schema_name, query_text, state, error_code, scheduled_from, attempt_number") == [
        ("T1", "SCHEMA1", "select 1", "SUCCEEDED", None, "EXECUTE_TASK", 1)
    ]


def test_execute_task_history_failed_row_has_error(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    ((state, error_code, error_message),) = _history(cur, "state, error_code, error_message")
    assert (state, error_code) == ("FAILED", 2003)
    assert "MISSING" in error_message


def test_execute_task_history_when_false_row_skipped(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 when 1 = 2 as select 1")
    cur.execute("execute task t1")
    assert _history(cur, "state, condition_text, query_id") == [("SKIPPED", "1 = 2", None)]


def test_execute_task_history_query_id_is_body_query(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 42 as answer")
    cur.execute("execute task t1")
    ((query_id,),) = _history(cur, "query_id")
    cur.execute(f"select * from table(result_scan('{query_id}'))")
    assert cur.fetchall() == [(42,)]


def test_execute_task_history_run_id_is_scheduled_time_in_ms(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    ((run_id, scheduled_time),) = _history(cur, "run_id, scheduled_time")
    assert run_id == int(scheduled_time.timestamp() * 1000)


def _run_graph_twice(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    cur.execute("execute task root")


def test_execute_task_graph_run_group_id_per_run(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_graph_twice(cur)
    cur.execute("select count(distinct graph_run_group_id) from table(information_schema.task_history())")
    assert cur.fetchall() == [(2,)]


def test_execute_task_run_id_per_run(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_graph_twice(cur)
    cur.execute("select count(distinct run_id) from table(information_schema.task_history())")
    assert cur.fetchall() == [(2,)]


def test_execute_task_history_root_task_id_shared(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_graph_twice(cur)
    cur.execute("select count(distinct root_task_id) from table(information_schema.task_history())")
    assert cur.fetchall() == [(1,)]


def test_execute_task_using_config_replaces_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""create task t1 config = '{"a": 1}' as select 1""")
    cur.execute("""execute task t1 using config = '{"b": 2}'""")
    assert _history(cur, "config") == [('{"b": 2}',)]


def test_execute_task_history_records_root_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""create task t1 config = '{"a": 1}' as select 1""")
    cur.execute("execute task t1")
    assert _history(cur, "config") == [('{"a": 1}',)]


def test_execute_task_graph_run_succeeded(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("execute task root")
    assert _graph_runs(cur, "root_task_name, schema_name, state, scheduled_from, attempt_number") == [
        ("ROOT", "SCHEMA1", "SUCCEEDED", "EXECUTE_TASK", 1)
    ]


def test_execute_task_graph_run_failed_records_first_error(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select * from missing")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    assert _graph_runs(cur, "state, first_error_task_name, first_error_code") == [("FAILED", "CHILD", 2003)]


def test_execute_task_graph_run_skipped_when_root_skipped(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root when 1 = 2 as select 1")
    cur.execute("execute task root")
    assert _graph_runs(cur, "state") == [("SKIPPED",)]


def _run_and_drop_schema(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create schema s2")
    cur.execute("create task s2.t1 as select 1")
    cur.execute("execute task s2.t1")
    cur.execute("drop schema s2")


def test_drop_schema_drops_its_task_history(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_and_drop_schema(cur)
    assert _history(cur, "name") == []


def test_drop_schema_drops_its_task_graph_runs(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_and_drop_schema(cur)
    assert _graph_runs(cur, "root_task_name") == []


def test_execute_task_runs_siblings_by_name(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "b", "after root")
    _logging_task(cur, "a", "after root")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    assert _log(cur) == ["root", "a", "b"]


def test_execute_task_suspended_finalizer_is_skipped(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "fin", "finalize = root")
    cur.execute("execute task root")
    assert _log(cur) == ["root"]


def test_execute_task_keeps_changes_made_by_its_body(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as alter task t1 set comment = 'changed'")
    cur.execute("execute task t1")
    assert _show(cur, "comment") == [("changed",)]


def test_execute_task_sees_writes_after_explicit_begin(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (i int)")
    cur.execute("create table copied (i int)")
    cur.execute("create task t1 as insert into copied select * from t")
    cur.execute("begin")
    cur.execute("insert into t values (1)")
    cur.execute("execute task t1")
    cur.execute("select i from copied")
    assert cur.fetchall() == [(1,)]


def test_execute_task_commits_explicit_transaction(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (i int)")
    cur.execute("create task t1 as select 1")
    cur.execute("begin")
    cur.execute("insert into t values (1)")
    cur.execute("execute task t1")
    cur.execute("rollback")
    cur.execute("select i from t")
    assert cur.fetchall() == [(1,)]


def test_execute_task_history_survives_callers_rollback(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("create table t (i int)")
        cur.execute("create task t1 as select 1")
        cur.execute("insert into t values (1)")
        cur.execute("execute task t1")
        cur.execute("insert into t values (2)")
        conn.rollback()
        assert _history(cur, "name") == [("T1",)]


def test_execute_task_using_dollar_quoted_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("""execute task t1 using config = $${"a": 1}$$""")
    assert _history(cur, "config") == [('{"a": 1}',)]


def test_execute_task_quoted_mixed_case_graph(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    cur.execute("""create task "My Root" as insert into log (name) values ('My Root')""")
    cur.execute("""create task "my child" after "My Root" as insert into log (name) values ('my child')""")
    cur.execute("""select system$task_dependents_enable('"My Root"')""")
    cur.execute('execute task "My Root"')
    assert _log(cur) == ["My Root", "my child"]


def test_execute_task_status_quoted_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Task" as select 1')
    cur.execute('execute task "My Task"')
    assert cur.fetchall() == [("Task My Task is scheduled to run immediately.",)]


def test_execute_quoted_child_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Root" as select 1')
    cur.execute('create task "my child" after "My Root" as select 1')
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Cannot execute task DB1\.SCHEMA1\.my child: EXECUTE TASK can only run a standalone or root task\.",
    ):
        cur.execute('execute task "my child"')


def _create_root_returning_value(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select system$set_return_value('from root')")


def test_set_return_value_recorded_in_history(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select system$set_return_value('done')")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("done",)]


def test_set_return_value_stringifies_result(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select system$set_return_value(40 + 2)")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("42",)]


def test_predecessor_return_value_in_child(cur: snowflake.connector.cursor.SnowflakeCursor):
    _create_root_returning_value(cur)
    cur.execute("create task child after root as insert into seen select system$get_predecessor_return_value('root')")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("from root",)]


def test_predecessor_return_value_without_argument(cur: snowflake.connector.cursor.SnowflakeCursor):
    _create_root_returning_value(cur)
    cur.execute("create task child after root as insert into seen select system$get_predecessor_return_value()")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("from root",)]


def test_predecessor_return_value_quoted_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("""create task "My Root" as select system$set_return_value('quoted')""")
    cur.execute(
        """create task child after "My Root" as
        insert into seen select system$get_predecessor_return_value('"My Root"')"""
    )
    cur.execute("""select system$task_dependents_enable('"My Root"')""")
    cur.execute('execute task "My Root"')
    cur.execute("select v from seen")
    assert cur.fetchall() == [("quoted",)]


def test_predecessor_return_value_in_when(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select system$set_return_value('go')")
    cur.execute(
        """create task child after root
        when system$get_predecessor_return_value('root') = 'go'
        as insert into seen values ('ran')"""
    )
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("ran",)]


def test_get_task_graph_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("""create task t1 config = '{"a": 1}' as insert into seen select system$get_task_graph_config()""")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == [('{"a": 1}',)]


def test_get_task_graph_config_key(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("""create task t1 config = '{"a": "x"}' as insert into seen select system$get_task_graph_config('a')""")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("x",)]


def test_get_task_graph_config_using_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("""create task t1 config = '{"a": 1}' as insert into seen select system$get_task_graph_config()""")
    cur.execute("""execute task t1 using config = '{"b": 2}'""")
    cur.execute("select v from seen")
    assert cur.fetchall() == [('{"b": 2}',)]


def test_current_user_task_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task t1 as insert into seen select system$current_user_task_name()")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("T1",)]


def test_task_runtime_info_current_root_task_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select 1")
    cur.execute(
        "create task child after root as insert into seen select system$task_runtime_info('CURRENT_ROOT_TASK_NAME')"
    )
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("DB1.SCHEMA1.ROOT",)]


def test_task_runtime_info_graph_run_group_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task t1 as insert into seen select system$task_runtime_info('CURRENT_TASK_GRAPH_RUN_GROUP_ID')")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == _history(cur, "graph_run_group_id")


def test_task_functions_match_case_insensitively(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task t1 as insert into seen select SYSTEM$Current_User_Task_Name()")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("T1",)]


@pytest.mark.parametrize(
    "call",
    [
        "system$current_user_task_name()",
        "system$task_runtime_info('CURRENT_TASK_NAME')",
        "system$get_task_graph_config()",
        "system$get_predecessor_return_value('t')",
    ],
)
def test_task_function_outside_task_returns_null(cur: snowflake.connector.cursor.SnowflakeCursor, call: str):
    cur.execute(f"select {call}")
    assert cur.fetchall() == [(None,)]


def _runtime_info(cur: snowflake.connector.cursor.SnowflakeCursor, field: str) -> str:
    """The value a task sees for a TASK_RUNTIME_INFO field."""
    cur.execute("create or replace table seen (v varchar)")
    cur.execute(f"create or replace task t1 as insert into seen select system$task_runtime_info('{field}')")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    ((value,),) = cur.fetchall()
    return value


def test_task_runtime_info_current_task_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert _runtime_info(cur, "CURRENT_TASK_NAME") == "DB1.SCHEMA1.T1"


def test_task_runtime_info_current_root_task_uuid(cur: snowflake.connector.cursor.SnowflakeCursor):
    value = _runtime_info(cur, "CURRENT_ROOT_TASK_UUID")
    assert _show(cur, "id", "t1") == [(value,)]


def test_task_runtime_info_original_scheduled_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    value = _runtime_info(cur, "CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP")
    ((scheduled,),) = _history(cur, "scheduled_time")
    # in UTC, to the millisecond
    assert value.endswith("+0000")
    assert 0 <= (scheduled - datetime.fromisoformat(value)).total_seconds() < 0.001


def test_get_predecessor_return_value_without_argument_and_several_predecessors(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select system$set_return_value('a')")
    cur.execute("create task b after root as select system$set_return_value('b')")
    cur.execute("create task c after a, b as insert into seen select system$get_predecessor_return_value()")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [(None,)]


def test_set_return_value_outside_task_returns_null(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select system$set_return_value('x')")
    assert cur.fetchall() == [(None,)]


def _table_function(cur: snowflake.connector.cursor.SnowflakeCursor, sql: str) -> list[dict]:
    cur.execute(sql)
    names = [d.name for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def test_task_history_succeeded_row(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    (row,) = _table_function(cur, "select * from table(information_schema.task_history())")
    assert {k: row[k] for k in ("NAME", "DATABASE_NAME", "SCHEMA_NAME", "QUERY_TEXT", "STATE", "GRAPH_VERSION")} == {
        "NAME": "T1",
        "DATABASE_NAME": "DB1",
        "SCHEMA_NAME": "SCHEMA1",
        "QUERY_TEXT": "select 1",
        "STATE": "SUCCEEDED",
        "GRAPH_VERSION": 1,
    }


def test_task_history_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select * from table(information_schema.task_history())")
    assert [d.name for d in cur.description] == [
        "QUERY_ID",
        "NAME",
        "DATABASE_NAME",
        "SCHEMA_NAME",
        "QUERY_TEXT",
        "CONDITION_TEXT",
        "STATE",
        "ERROR_CODE",
        "ERROR_MESSAGE",
        "SCHEDULED_TIME",
        "QUERY_START_TIME",
        "NEXT_SCHEDULED_TIME",
        "COMPLETED_TIME",
        "ROOT_TASK_ID",
        "GRAPH_VERSION",
        "RUN_ID",
        "RETURN_VALUE",
        "SCHEDULED_FROM",
        "ATTEMPT_NUMBER",
        "CONFIG",
        "QUERY_HASH",
        "QUERY_HASH_VERSION",
        "QUERY_PARAMETERIZED_HASH",
        "QUERY_PARAMETERIZED_HASH_VERSION",
        "GRAPH_RUN_GROUP_ID",
        "BACKFILL_INFO",
        "SPCS_JOB_ID",
    ]


def test_task_history_failed_row_has_error(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    (row,) = _table_function(cur, "select * from table(information_schema.task_history())")
    assert (row["STATE"], row["ERROR_CODE"]) == ("FAILED", 2003)
    assert "MISSING" in row["ERROR_MESSAGE"]


def test_task_history_database_qualified(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    cur.execute("select name from table(db1.information_schema.task_history())")
    assert cur.fetchall() == [("T1",)]


def test_task_history_alias(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    cur.execute("select h.name from table(information_schema.task_history()) h")
    assert cur.fetchall() == [("T1",)]


def test_task_history_task_name_filter(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 as select 1")
    cur.execute("execute task t1")
    cur.execute("execute task t2")
    cur.execute("select name from table(information_schema.task_history(task_name => 't2'))")
    assert cur.fetchall() == [("T2",)]


def test_task_history_error_only(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task ok as select 1")
    cur.execute("create task bad as select * from missing")
    cur.execute("execute task ok")
    cur.execute("execute task bad")
    cur.execute("select name from table(information_schema.task_history(error_only => true))")
    assert cur.fetchall() == [("BAD",)]


def test_task_history_result_limit(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 as select 1")
    cur.execute("execute task t1")
    cur.execute("execute task t2")
    cur.execute("select name from table(information_schema.task_history(result_limit => 1))")
    assert cur.fetchall() == [("T2",)]


def test_task_history_newest_first(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 as select 1")
    cur.execute("execute task t1")
    cur.execute("execute task t2")
    cur.execute("select name from table(information_schema.task_history())")
    assert cur.fetchall() == [("T2",), ("T1",)]


def test_task_history_scheduled_time_range(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    cur.execute("select name from table(information_schema.task_history(scheduled_time_range_start => '2999-01-01'))")
    assert cur.fetchall() == []


def test_task_history_root_task_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 as select 1")
    cur.execute("execute task t1")
    cur.execute("execute task t2")
    ((root_id,),) = _show(cur, "id", "t1")
    cur.execute(f"select name from table(information_schema.task_history(root_task_id => '{root_id}'))")
    assert cur.fetchall() == [("T1",)]


def test_task_history_non_literal_argument_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="literal"):
        cur.execute("select * from table(information_schema.task_history(task_name => lower('T1')))")


def test_task_history_query_id_result_scan(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 42 as answer")
    cur.execute("execute task t1")
    cur.execute("select query_id from table(information_schema.task_history())")
    ((query_id,),) = cur.fetchall()
    cur.execute(f"select * from table(result_scan('{query_id}'))")
    assert cur.fetchall() == [(42,)]


def test_task_history_survives_caller_rollback(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("begin")
    cur.execute("execute task t1")
    cur.execute("rollback")
    cur.execute("select name from table(information_schema.task_history())")
    assert cur.fetchall() == [("T1",)]


def test_complete_task_graphs_failed_graph(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select * from missing")
    cur.execute("select system$task_dependents_enable('root')")
    cur.execute("execute task root")
    (row,) = _table_function(cur, "select * from table(information_schema.complete_task_graphs())")
    assert {k: row[k] for k in ("ROOT_TASK_NAME", "DATABASE_NAME", "STATE", "FIRST_ERROR_TASK_NAME")} == {
        "ROOT_TASK_NAME": "ROOT",
        "DATABASE_NAME": "DB1",
        "STATE": "FAILED",
        "FIRST_ERROR_TASK_NAME": "CHILD",
    }


def test_complete_task_graphs_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select * from table(information_schema.complete_task_graphs())")
    assert [d.name for d in cur.description] == [
        "ROOT_TASK_NAME",
        "DATABASE_NAME",
        "SCHEMA_NAME",
        "STATE",
        "SCHEDULED_FROM",
        "FIRST_ERROR_TASK_NAME",
        "FIRST_ERROR_CODE",
        "FIRST_ERROR_MESSAGE",
        "SCHEDULED_TIME",
        "QUERY_START_TIME",
        "NEXT_SCHEDULED_TIME",
        "COMPLETED_TIME",
        "ROOT_TASK_ID",
        "GRAPH_VERSION",
        "RUN_ID",
        "ATTEMPT_NUMBER",
        "CONFIG",
        "GRAPH_RUN_GROUP_ID",
        "BACKFILL_INFO",
    ]


def test_complete_task_graphs_root_task_name_filter(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task r1 as select 1")
    cur.execute("create task r2 as select 1")
    cur.execute("execute task r1")
    cur.execute("execute task r2")
    cur.execute("select root_task_name from table(information_schema.complete_task_graphs(root_task_name => 'r1'))")
    assert cur.fetchall() == [("R1",)]


def test_complete_task_graphs_error_only(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task ok as select 1")
    cur.execute("create task bad as select * from missing")
    cur.execute("execute task ok")
    cur.execute("execute task bad")
    cur.execute("select root_task_name from table(information_schema.complete_task_graphs(error_only => true))")
    assert cur.fetchall() == [("BAD",)]


def _dependents_graph(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create task grandchild after child as select 1")
    cur.execute("create task other as select 1")


def test_task_dependents_recursive(cur: snowflake.connector.cursor.SnowflakeCursor):
    _dependents_graph(cur)
    cur.execute("select name from table(information_schema.task_dependents(task_name => 'root')) order by name")
    assert cur.fetchall() == [("CHILD",), ("GRANDCHILD",), ("ROOT",)]


def test_task_dependents_not_recursive(cur: snowflake.connector.cursor.SnowflakeCursor):
    _dependents_graph(cur)
    cur.execute(
        "select name from table(information_schema.task_dependents(task_name => 'root', recursive => false)) "
        "order by name"
    )
    assert cur.fetchall() == [("CHILD",), ("ROOT",)]


def test_task_dependents_has_show_tasks_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    _dependents_graph(cur)
    cur.execute("show tasks")
    expected = [d.name for d in cur.description]
    cur.execute("select * from table(information_schema.task_dependents(task_name => 'root'))")
    assert [d.name for d in cur.description] == expected


def test_task_dependents_qualified_task_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    _dependents_graph(cur)
    cur.execute("select name from table(information_schema.task_dependents(task_name => 'db1.schema1.child'))")
    assert sorted(cur.fetchall()) == [("CHILD",), ("GRANDCHILD",)]
