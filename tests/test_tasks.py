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


def test_create_or_alter_task_keeps_last_suspended_reason(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("update db1._fs_information_schema._fs_tasks set last_suspended_reason = 'CHILD_BECAME_ROOT'")
    cur.execute("create or alter task t1 as select 2")
    assert _select(cur, "last_suspended_reason") == [("CHILD_BECAME_ROOT",)]


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


def _start(cur: snowflake.connector.cursor.SnowflakeCursor, name: str) -> None:
    cur.execute(f"update db1._fs_information_schema._fs_tasks set state = 'started' where name = '{name.upper()}'")


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
    assert _select(cur, "state, last_suspended_reason", "child") == [("suspended", "CHILD_BECAME_ROOT")]


def test_drop_only_predecessor_clears_child_predecessors(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("drop task root")
    assert _select(cur, "array_length(predecessors)", "child") == [(0,)]


def test_drop_root_makes_finalizer_standalone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("drop task root")
    assert _select(cur, "state, finalize, last_suspended_reason", "fin") == [
        ("suspended", None, "FINALIZER_BECAME_ROOT")
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
    assert _select(cur, "array_length(predecessors)", "join") == [(2,)]


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
